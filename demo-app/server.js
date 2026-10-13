const path = require('path');
const express = require('express');

const cfg = require('./config');
const events = require('./events');
const { kafkaGlobal } = require('./kafkaClients');
const { OrderProducer } = require('./producer');
const { OrderConsumer } = require('./consumer');
const monitor = require('./monitor');
const switchover = require('./switchover');
const metrics = require('./metrics');

const startedAt = Date.now();
const producer = new OrderProducer(kafkaGlobal);
const consumer = new OrderConsumer(kafkaGlobal);

const latest = {
  pair: null,
  pairError: null,
  snapshot: { primary: null, dr: null },
  activeCluster: { side: 'unknown' },
  lastPolledAt: null,
  metrics: { primary: null, dr: null },
  metricsHistory: { primary: [], dr: [] },
  mirrorLag: null,
  mirrorLagError: null,
};

// 30 samples at the 1-minute poll cadence below == 30 minutes of history, matching the Metrics
// API's own 60s server-side sample rate (polling faster would just repeat the same sample).
const MAX_METRICS_HISTORY = 30;
function pushMetricsHistory(side, byType) {
  const arr = latest.metricsHistory[side];
  arr.push({ ts: Date.now(), byType: byType || {} });
  if (arr.length > MAX_METRICS_HISTORY) arr.shift();
}

function clusterIdForSide(side) {
  return side === 'dr' ? cfg.drClusterId : side === 'primary' ? cfg.primaryClusterId : null;
}
function otherSide(side) {
  return side === 'primary' ? 'dr' : side === 'dr' ? 'primary' : null;
}

const failoverOp = {
  inProgress: false,
  action: null, // 'failover' | 'failback'
  targetSide: null, // 'primary' | 'dr'
  startedAt: null,
  completedAt: null,
  durationMs: null,
  error: null,
  // Three extra milestones the control-plane completion above doesn't capture on its own --
  // see watchForCompletion()/watchMilestones() for how each is detected.
  producerRecoveredAt: null,
  consumerRecoveredAt: null,
  syncRestoredAt: null,
};

// Detects "a successful send/receive following a real gap" -- a graceful PLANNED cutover often
// has no gap at all (poll() returns null, which the UI reports as "no interruption observed").
// Reads the live timestamp off producer/consumer state each time rather than trusting error
// flags, because librdkafka buffers/retries sends below the JS promise layer and never surfaces a
// transient outage as a rejection -- see the comment on computeHealth() in index.html.
//
// Returns the recovery timestamp only on the poll() call where it's first detected (not sticky),
// so the same tracker can keep reporting fresh recoveries indefinitely -- this one runs
// continuously for the app's whole lifetime (see pollRecoveryTrackers()), not just while a
// particular failover op's watchForCompletion() loop happens to still be running. The control
// plane can report an op "done" well before the Kafka client's own reconnect/metadata-refresh
// delay actually clears, so a tracker scoped to that loop would miss recoveries that land after
// completion -- exactly the mismatch where the live health badge still says "retrying" after the
// stopwatch has already frozen.
function makeRecoveryTracker(getLastAt, expectedGapMs) {
  let prev = null;
  const threshold = Math.max(expectedGapMs * 8, 3000);
  return function poll() {
    const cur = getLastAt();
    let recoveredAt = null;
    if (cur != null) {
      if (prev != null && cur - prev > threshold) {
        recoveredAt = cur;
      }
      prev = cur;
    }
    return recoveredAt;
  };
}

const expectedGapMs = 1000 / (cfg.producePerSec || 10);
const producerTracker = makeRecoveryTracker(() => producer.state.lastSuccessAt, expectedGapMs);
const consumerTracker = makeRecoveryTracker(() => consumer.state.lastReceivedAt, expectedGapMs);

// Runs for the app's whole lifetime, independent of any one failover op's lifecycle -- see the
// comment on makeRecoveryTracker() above for why. Only latches a detected recovery onto the
// *current* failoverOp if it happened at or after that op's trigger and hasn't already been
// recorded, so a recovery left over from a previous op (or app startup) is never misattributed.
function pollRecoveryTrackers() {
  const producerRecoveredAt = producerTracker();
  if (producerRecoveredAt && failoverOp.startedAt && producerRecoveredAt >= failoverOp.startedAt && !failoverOp.producerRecoveredAt) {
    failoverOp.producerRecoveredAt = producerRecoveredAt;
    events.log('ok', `Producers resumed sending (${((producerRecoveredAt - failoverOp.startedAt) / 1000).toFixed(1)}s after trigger)`);
  }
  const consumerRecoveredAt = consumerTracker();
  if (consumerRecoveredAt && failoverOp.startedAt && consumerRecoveredAt >= failoverOp.startedAt && !failoverOp.consumerRecoveredAt) {
    failoverOp.consumerRecoveredAt = consumerRecoveredAt;
    events.log('ok', `Consumers resumed receiving (${((consumerRecoveredAt - failoverOp.startedAt) / 1000).toFixed(1)}s after trigger)`);
  }
}

// Which side is safe to treat as "active" right now, derived ONLY from the Switchover
// Management REST API (a plain HTTPS call, not a Kafka connection). Returns null while a
// cutover is in flight (phase === 'UPDATING') rather than guessing -- see monitor.js for why a
// wrong guess here used to cause real hangs/crashes (attempting a native Kafka connection to a
// cluster that's still, or again, the fenced standby).
function activeSideFromPair(pair) {
  if (!pair || !pair.spec || !pair.status) return null;
  if (pair.status.phase === 'UPDATING') return null;
  return pair.spec.active_member === 'secondary' ? 'dr' : 'primary';
}

async function pollOnce() {
  try {
    const pair = await switchover.getPair().catch((e) => {
      latest.pairError = e.message;
      return null;
    });
    if (pair) {
      latest.pairError = null;
      latest.pair = pair;
    }
    const activeSide = activeSideFromPair(pair);
    latest.activeCluster = activeSide ? { side: activeSide } : { side: 'unknown' };
    latest.snapshot = await monitor.snapshot(activeSide);
    latest.lastPolledAt = Date.now();
  } catch (err) {
    events.log('error', `Poll cycle failed: ${err.message}`);
  }
}

function startPolling() {
  pollOnce();
  setInterval(pollOnce, 1500);
  pollRecoveryTrackers();
  setInterval(pollRecoveryTrackers, 1000);
}

// The Metrics API samples every 60s server-side, so polling any faster than that would just
// re-fetch the same sample -- this loop polls once a minute, in step with the API's own cadence.
async function pollMetricsOnce() {
  try {
    const [primary, dr] = await Promise.all([
      metrics.requestBytes(cfg.primaryClusterId).catch((e) => ({ error: e.message })),
      metrics.requestBytes(cfg.drClusterId).catch((e) => ({ error: e.message })),
    ]);
    latest.metrics = { primary, dr };
    if (!primary.error) pushMetricsHistory('primary', primary.byType);
    if (!dr.error) pushMetricsHistory('dr', dr.byType);

    // Mirror lag only means something on the standby side (the one holding the mirror copy of
    // the topic) -- querying the active side would just come back empty.
    const standbySide = otherSide(latest.activeCluster && latest.activeCluster.side);
    const standbyClusterId = clusterIdForSide(standbySide);
    if (standbyClusterId) {
      const sample = await metrics.mirrorLag(standbyClusterId, 'dr-link', cfg.topic);
      latest.mirrorLag = { side: standbySide, value: sample ? sample.value : null };
      latest.mirrorLagError = null;
    } else {
      latest.mirrorLag = null;
    }
  } catch (err) {
    latest.mirrorLagError = err.message;
    events.log('error', `Metrics poll failed: ${err.message}`);
  }
}

function startMetricsPolling() {
  pollMetricsOnce();
  setInterval(pollMetricsOnce, 60000);
}

const OP_LABELS = {
  'failover-planned': 'Planned failover',
  'failback-planned': 'Planned fail-back',
  'failover-unplanned': 'Unplanned failover',
  'failback-unplanned': 'Unplanned fail-back',
  restore: 'Restore',
};

// After a failover/failback/restore call succeeds, watch (via the same safe REST API, not a
// Kafka connection) until the switchover pair reports the operation done -- that moment, not the
// trigger call returning, is what the stopwatch should measure. Completion looks different for
// restore (phase-based: READY_TO_RESTORE -> READY_TO_FAILOVER, active_member never changes) than
// for a failover/failback (active_member-based: reaches targetSide and phase leaves UPDATING).
// Standby cluster id to watch for "back in sync" during this op -- for failover/failback it's
// the side *not* being made active; for restore (active_member never changes) it's whichever
// side was already the standby when the op started.
function standbyClusterIdForOp(targetSide) {
  const standbySide = targetSide ? otherSide(targetSide) : otherSide(latest.activeCluster.side);
  return clusterIdForSide(standbySide);
}

async function watchForCompletion(action, targetSide) {
  const isRestore = action === 'restore';
  const deadline = Date.now() + 120000; // safety cap
  // Producer/consumer recovery is tracked by the always-on pollRecoveryTrackers() loop (see
  // above), not scoped to this function -- it needs to keep watching even after this loop below
  // returns, since the control plane can report "done" before the Kafka client has actually
  // finished reconnecting.
  const standbyClusterId = standbyClusterIdForOp(targetSide);
  let tick = 0;
  while (Date.now() < deadline) {
    // Mirror lag is only sampled once a minute server-side, but we poll it every ~5s here (far
    // more often than the once-a-minute background poll) purely to catch the new sample as soon
    // as the Metrics API has it -- the milestone timestamp used is the sample's own embedded
    // timestamp, not this poll's arrival time.
    if (!failoverOp.syncRestoredAt && standbyClusterId && tick % 5 === 0) {
      const sample = await metrics.mirrorLag(standbyClusterId, 'dr-link', cfg.topic).catch(() => null);
      // Guard against a stale sample from before this op even started (e.g. a leftover 0-lag
      // reading from the last time this side was a standby, still inside the Metrics API's
      // lookback window) -- only a sample timestamped at or after the trigger counts as evidence
      // the backup is back in sync *for this operation*.
      const sampleAtMs = sample ? Date.parse(sample.ts) : NaN;
      if (sample && sample.value === 0 && sampleAtMs >= failoverOp.startedAt) {
        failoverOp.syncRestoredAt = sampleAtMs;
        events.log('ok', `Backup cluster back in sync (mirror lag 0) (${((sampleAtMs - failoverOp.startedAt) / 1000).toFixed(1)}s after trigger)`);
      }
    }
    tick++;

    const pair = await switchover.getPair().catch(() => null);
    if (pair) {
      latest.pair = pair;
      const activeSide = activeSideFromPair(pair);
      latest.activeCluster = activeSide ? { side: activeSide } : { side: 'unknown' };
      const phase = pair.status && pair.status.phase;
      const done = isRestore ? phase === 'READY_TO_FAILOVER' : activeSide === targetSide;
      if (done) {
        failoverOp.completedAt = Date.now();
        failoverOp.durationMs = failoverOp.completedAt - failoverOp.startedAt;
        failoverOp.inProgress = false;
        // PLANNED failover/failback and RESTORE all require zero replication lag as a precondition
        // of completing, so if the Metrics API's 60s sampling granularity never happened to catch
        // a live nonzero-then-zero sample (the common case at this demo's traffic volume -- the
        // whole catch-up finishes inside a single sampling window, see COMMANDS.md), completion
        // itself is still honest evidence the backup is in sync. UNPLANNED leaves the link stopped
        // with no such guarantee, so it's left pending there until a Restore is run.
        const isUnplanned = action === 'failover-unplanned' || action === 'failback-unplanned';
        if (!failoverOp.syncRestoredAt && !isUnplanned) {
          failoverOp.syncRestoredAt = failoverOp.completedAt;
        }
        const what = isRestore ? 'standby cluster resynced and restored' : `global bootstrap now serving ${targetSide}`;
        events.log('ok', `${OP_LABELS[action]} complete: ${what} (${(failoverOp.durationMs / 1000).toFixed(1)}s)`);
        return;
      }
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  failoverOp.inProgress = false;
  failoverOp.error = 'Timed out waiting for operation to complete';
  events.log('error', failoverOp.error);
}

// activeMember is null for restore (the API requires it to be omitted entirely, not just empty --
// see switchover.js). targetSide is likewise null for restore (active_member doesn't change).
async function triggerSwitchoverOp(action, { activeMember, failoverType, targetSide }) {
  if (failoverOp.inProgress) {
    throw new Error('A failover operation is already in progress');
  }
  failoverOp.inProgress = true;
  failoverOp.action = action;
  failoverOp.targetSide = targetSide;
  failoverOp.startedAt = Date.now();
  failoverOp.completedAt = null;
  failoverOp.durationMs = null;
  failoverOp.error = null;
  failoverOp.producerRecoveredAt = null;
  failoverOp.consumerRecoveredAt = null;
  failoverOp.syncRestoredAt = null;
  events.log('info', activeMember
    ? `${OP_LABELS[action]} triggered -> active_member=${activeMember}`
    : `${OP_LABELS[action]} triggered`);
  // The customer-facing ask here: show the *exact* REST call this button press makes. Logged
  // as its own event-log entry, distinct from the narrative "triggered" line above.
  const envCrn = `crn://confluent.cloud/organization=${cfg.orgId}/environment=${cfg.envId}`;
  const requestSpec = { environment_crn: envCrn, failover_type: failoverType };
  if (activeMember) requestSpec.active_member = activeMember;
  events.log(
    'cmd',
    `POST https://api.confluent.cloud/switchover/v1/switchover-pairs/${cfg.switchoverPairId}:failover\n${JSON.stringify({ spec: requestSpec })}`
  );
  try {
    await switchover.failover(activeMember, failoverType);
  } catch (err) {
    failoverOp.inProgress = false;
    failoverOp.error = err.message;
    events.log('error', `Switchover API call failed: ${err.message}`);
    throw err;
  }
  watchForCompletion(action, targetSide); // fire and forget; state read via /api/state
}

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

app.get('/api/state', (req, res) => {
  res.json({
    now: Date.now(),
    startedAt,
    topic: cfg.topic,
    group: cfg.group,
    primaryLabel: cfg.primaryLabel,
    drLabel: cfg.drLabel,
    producePerSec: cfg.producePerSec,
    pair: latest.pair,
    pairError: latest.pairError,
    activeCluster: latest.activeCluster,
    snapshot: latest.snapshot,
    lastPolledAt: latest.lastPolledAt,
    metrics: latest.metrics,
    metricsHistory: latest.metricsHistory,
    mirrorLag: latest.mirrorLag,
    mirrorLagError: latest.mirrorLagError,
    producer: producer.state,
    consumer: consumer.state,
    failoverOp,
    events: events.recent(),
  });
});

app.post('/api/failover', async (req, res) => {
  try {
    await triggerSwitchoverOp('failover-planned', { activeMember: 'secondary', failoverType: 'PLANNED', targetSide: 'dr' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/failback', async (req, res) => {
  try {
    await triggerSwitchoverOp('failback-planned', { activeMember: 'primary', failoverType: 'PLANNED', targetSide: 'primary' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/failover-unplanned', async (req, res) => {
  try {
    await triggerSwitchoverOp('failover-unplanned', { activeMember: 'secondary', failoverType: 'UNPLANNED', targetSide: 'dr' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/failback-unplanned', async (req, res) => {
  try {
    await triggerSwitchoverOp('failback-unplanned', { activeMember: 'primary', failoverType: 'UNPLANNED', targetSide: 'primary' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// RESTORE never changes active_member -- it just re-syncs the standby (whichever side that
// currently is) as a proper mirror target again after an UNPLANNED failover stopped the link.
app.post('/api/restore', async (req, res) => {
  try {
    await triggerSwitchoverOp('restore', { activeMember: null, failoverType: 'RESTORE', targetSide: null });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/events/clear', (req, res) => {
  events.clear();
  res.json({ ok: true });
});

async function main() {
  events.log('info', `Starting demo app: topic=${cfg.topic} group=${cfg.group} rate=${cfg.producePerSec}/s`);
  startPolling();
  startMetricsPolling();
  await producer.start();
  await consumer.start();
  app.listen(cfg.port, () => {
    events.log('info', `Dashboard listening on :${cfg.port}`);
  });
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Fatal startup error:', err);
  process.exit(1);
});
