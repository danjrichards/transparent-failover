// Thin client for the Confluent Cloud Metrics API (https://api.telemetry.confluent.cloud/v2/metrics/cloud).
// Pure HTTPS, authenticated with the same Cloud API key used for the Switchover Management API --
// no native Kafka connection involved, so none of the risks documented in monitor.js apply here.
const cfg = require('./config');

const BASE = 'https://api.telemetry.confluent.cloud/v2/metrics/cloud';

function authHeader() {
  return 'Basic ' + Buffer.from(`${cfg.cloudApiKey}:${cfg.cloudApiSecret}`).toString('base64');
}

function isoMinute(d) {
  return new Date(Math.floor(d.getTime() / 60000) * 60000).toISOString();
}

async function query(body) {
  const res = await fetch(`${BASE}/query`, {
    method: 'POST',
    headers: { Authorization: authHeader(), 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`metrics query failed: ${res.status} ${JSON.stringify(json)}`);
  return json.data || [];
}

function latestByGroup(data, groupKey) {
  const byGroup = {};
  for (const d of data) {
    const key = groupKey ? d[groupKey] : '_';
    if (!byGroup[key] || d.timestamp > byGroup[key].timestamp) byGroup[key] = d;
  }
  return byGroup;
}

// io.confluent.kafka.server/request_bytes, broken down by Kafka protocol request type, for one
// cluster. Returns the most recent complete 1-minute sample per type (it's a per-minute delta
// counter, not a running total) plus a sum across all types.
async function requestBytes(clusterId) {
  const now = new Date();
  const start = new Date(now.getTime() - 10 * 60000);
  const data = await query({
    aggregations: [{ metric: 'io.confluent.kafka.server/request_bytes' }],
    filter: { field: 'resource.kafka.id', op: 'EQ', value: clusterId },
    granularity: 'PT1M',
    intervals: [`${isoMinute(start)}/${isoMinute(now)}`],
    group_by: ['metric.type'],
  });
  const byType = latestByGroup(data, 'metric.type');
  const byTypeValues = {};
  let total = 0;
  for (const [type, point] of Object.entries(byType)) {
    byTypeValues[type] = point.value;
    total += point.value;
  }
  return {
    total,
    produce: byTypeValues.Produce ?? null,
    fetch: byTypeValues.Fetch ?? null,
    byType: byTypeValues,
  };
}

// io.confluent.kafka.server/cluster_link_mirror_topic_offset_lag for one cluster/link/topic.
// Only meaningful on whichever cluster currently holds the *mirror* copy of the topic (i.e. the
// standby side) -- the active/source side isn't mirroring anything and won't report this metric.
async function mirrorLag(clusterId, linkName, topic) {
  const now = new Date();
  const start = new Date(now.getTime() - 10 * 60000);
  const data = await query({
    aggregations: [{ metric: 'io.confluent.kafka.server/cluster_link_mirror_topic_offset_lag' }],
    filter: {
      op: 'AND',
      filters: [
        { field: 'resource.kafka.id', op: 'EQ', value: clusterId },
        { field: 'metric.topic', op: 'EQ', value: topic },
        { field: 'metric.link_name', op: 'EQ', value: linkName },
      ],
    },
    granularity: 'PT1M',
    intervals: [`${isoMinute(start)}/${isoMinute(now)}`],
  });
  if (data.length === 0) return null;
  const latest = data.reduce((a, b) => (b.timestamp > a.timestamp ? b : a));
  // Callers that need true recovery timing (e.g. "when did the standby actually catch up")
  // should use the sample's own embedded timestamp, not when this call returned -- the Metrics
  // API only samples every 60s server-side, so poll-arrival time lags the real event.
  return { value: latest.value, ts: latest.timestamp };
}

module.exports = { requestBytes, mirrorLag };
