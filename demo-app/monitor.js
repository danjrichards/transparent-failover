const { Kafka, logLevel } = require('@confluentinc/kafka-javascript').KafkaJS;
const cfg = require('./config');

// IMPORTANT, read before touching this file: an earlier version of this module also tried to
// independently verify "which cluster is actually serving traffic" by creating a native Kafka
// admin connection directly to the GLOBAL bootstrap URL and calling admin.clusterId(). That
// turned out to be actively dangerous, not just unreliable: during the real outage window of a
// failover (when the global URL is, by design, briefly unreachable), attempting to connect to it
// with this librdkafka-based client did not fail fast or even just hang an async promise -- it
// appears to block the Node.js event loop itself for long enough that systemd's watchdog/health
// checks (and every other request this process was serving, including unrelated /api/state
// reads) stalled, and the process was eventually killed and restarted by systemd. A JS-level
// `Promise.race` timeout does NOT protect against this, because if the blocking happens at the
// native/thread level the event loop never gets a chance to run the timeout callback either.
//
// The fix: never attempt a native Kafka connection to a target we aren't already confident is
// reachable. "Which side is active" and "has the cutover completed" are now determined entirely
// from the Switchover Management REST API (switchover.js) -- a plain HTTPS call to
// api.confluent.cloud that carries none of this risk. The producer and consumer (server.js) are
// left alone to prove the real transparent-failover behavior on their own long-lived connections;
// this module's only remaining job is the side-by-side offset comparison, and even that only
// ever connects to the side we've already confirmed (via the REST API) is currently active.
function makeHandle(brokers, tag) {
  return { brokers, tag, kafka: null, admin: null, connected: false };
}

const primaryHandle = makeHandle(cfg.primaryBootstrap, 'primary-admin');
const drHandle = makeHandle(cfg.drBootstrap, 'dr-admin');

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  // Belt-and-braces: if `promise` later rejects after the timeout already won the race, that
  // rejection must not become unhandled (Node crashes the process on unhandled rejections).
  promise.catch(() => {});
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

async function ensureConnected(handle) {
  if (handle.connected) return;
  handle.kafka = new Kafka({
    kafkaJS: {
      clientId: `danr-tf-demo-${handle.tag}`,
      brokers: [handle.brokers],
      ssl: true,
      sasl: { mechanism: 'plain', username: cfg.kafkaApiKey, password: cfg.kafkaApiSecret },
      connectionTimeout: 5000,
      requestTimeout: 7000,
      retry: { retries: 0 },
      logLevel: logLevel ? logLevel.NOTHING : undefined,
    },
  });
  handle.admin = handle.kafka.admin();
  await withTimeout(handle.admin.connect(), 6000, `${handle.tag} connect`);
  handle.connected = true;
}

async function withAdmin(handle, fn) {
  await ensureConnected(handle);
  try {
    return await withTimeout(fn(handle.admin), 8000, `${handle.tag} operation`);
  } catch (err) {
    handle.connected = false;
    try {
      await withTimeout(handle.admin.disconnect(), 3000, `${handle.tag} disconnect`);
    } catch {
      // already dead, or disconnect itself hung -- either way, drop the reference below so the
      // next ensureConnected() builds a fresh client rather than reusing a possibly-wedged one.
    }
    handle.admin = null;
    handle.kafka = null;
    throw err;
  }
}

async function fetchClusterSnapshot(handle, label) {
  try {
    return await withAdmin(handle, async (admin) => {
      let groupOffsetsError = null;
      // Sequential, not Promise.all: firing both immediately races this client's broker-pool/
      // coordinator discovery and fetchOffsets intermittently fails with "No brokers in the
      // broker pool" even though fetchTopicOffsets on the same connection succeeds.
      const topicOffsets = await admin.fetchTopicOffsets(cfg.topic);
      const groupOffsets = await admin
        .fetchOffsets({ groupId: cfg.group, topics: [cfg.topic] })
        .catch((e) => {
          groupOffsetsError = e.message;
          return null;
        });

      const committedByPartition = {};
      if (groupOffsets && groupOffsets[0] && groupOffsets[0].partitions) {
        for (const p of groupOffsets[0].partitions) {
          committedByPartition[p.partition] = parseInt(p.offset, 10);
        }
      }

      let totalHigh = 0;
      let totalCommitted = 0;
      let totalLag = 0;
      const partitions = {};
      for (const p of topicOffsets) {
        const high = parseInt(p.offset, 10);
        const committed = committedByPartition[p.partition];
        const hasCommitted = committed !== undefined && committed >= 0;
        const lag = hasCommitted ? Math.max(0, high - committed) : null;
        partitions[p.partition] = { high, committed: hasCommitted ? committed : null, lag };
        totalHigh += high;
        if (hasCommitted) totalCommitted += committed;
        if (lag !== null) totalLag += lag;
      }

      return { ok: true, label, partitions, totalHigh, totalCommitted, totalLag, groupOffsetsError };
    });
  } catch (err) {
    return { ok: false, label, error: err.message };
  }
}

// `activeSide` is 'primary' | 'dr' | null (null = not currently confident which side is safe to
// query directly -- e.g. a cutover is in progress -- so we skip both rather than guess).
async function snapshot(activeSide) {
  const primaryPromise =
    activeSide === 'dr'
      ? Promise.resolve({ ok: false, label: cfg.primaryLabel, standbyRestricted: true })
      : activeSide === 'primary'
        ? fetchClusterSnapshot(primaryHandle, cfg.primaryLabel)
        : Promise.resolve({ ok: false, label: cfg.primaryLabel, error: 'active side not yet known' });

  const drPromise =
    activeSide === 'primary'
      ? Promise.resolve({ ok: false, label: cfg.drLabel, standbyRestricted: true })
      : activeSide === 'dr'
        ? fetchClusterSnapshot(drHandle, cfg.drLabel)
        : Promise.resolve({ ok: false, label: cfg.drLabel, error: 'active side not yet known' });

  const [primary, dr] = await Promise.all([primaryPromise, drPromise]);
  return { primary, dr };
}

module.exports = { snapshot };
