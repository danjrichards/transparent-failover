// Thin client for Confluent Cloud's Switchover Management REST API
// (https://api.confluent.cloud/switchover/v1/...). Authenticates with a Cloud API key
// (org/environment-admin scoped) -- a completely different credential from the Kafka
// data-plane API keys used by the producer/consumer/admin clients.
const cfg = require('./config');

const BASE = 'https://api.confluent.cloud/switchover/v1';

function authHeader() {
  const token = Buffer.from(`${cfg.cloudApiKey}:${cfg.cloudApiSecret}`).toString('base64');
  return `Basic ${token}`;
}

async function getPair() {
  const res = await fetch(`${BASE}/switchover-pairs/${cfg.switchoverPairId}?environment=${cfg.envId}`, {
    headers: { Authorization: authHeader() },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`getPair failed: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

// activeMember is required for PLANNED/UNPLANNED, and must be *omitted entirely* (not just null)
// for RESTORE -- the API rejects a RESTORE request that includes it.
async function failover(activeMember, failoverType = 'PLANNED') {
  const environmentCrn = `crn://confluent.cloud/organization=${cfg.orgId}/environment=${cfg.envId}`;
  const spec = { environment_crn: environmentCrn, failover_type: failoverType };
  if (activeMember) spec.active_member = activeMember;
  const res = await fetch(`${BASE}/switchover-pairs/${cfg.switchoverPairId}:failover`, {
    method: 'POST',
    headers: { Authorization: authHeader(), 'Content-Type': 'application/json' },
    body: JSON.stringify({ spec }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`failover failed: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

module.exports = { getPair, failover };
