# transparent-failover

A working example of a Confluent Cloud **transparent failover** setup: two Azure **Dedicated**
Kafka clusters (primary + DR) connected only via **Azure Private Link**, a jump-box VM with
private network access to both clusters, a sample data producer, and Confluent's **DR Automatic
Failover** feature (Early Access) — Cluster Linking, a switchover pair/endpoint, and a global
bootstrap URL that lets clients fail over without ever changing their connection config.

A full, worked example of how this was built — every command, in order — is in
[`COMMANDS.md`](./COMMANDS.md). Secrets and resource IDs for your own build go in `env.private`
(gitignored, never committed).

## Architecture

```
 Confluent Cloud environment
 ┌────────────────────────────────────┐        ┌────────────────────────────────────┐
 │ primary cluster (DEDICATED)        │        │ DR cluster (DEDICATED)             │
 │ westeurope                         │        │ northeurope                        │
 │ + a sample producer/connector      │        │                                    │
 └──────────────────┬─────────────────┘        └──────────────────┬─────────────────┘
                    │.                                            │  Azure Private Link (classic model: one Confluent "network" per
                    │.                                            │  cluster, 3 zonal Private Endpoints each, matching the network's
                    │.                                            │  own dns_domain directly)
 ┌──────────────────▼─────────────────┐        ┌──────────────────▼─────────────────┐
 │ primary-vnet                       │◄──────►│ dr-vnet                            │
 │ westeurope · 10.20.0.0/16          │  VNet  │ northeurope · 10.21.0.0/16         │
 │ privatelink-subnet: 3 zonal PEs    │peering │ privatelink-subnet: 3 zonal PEs    │
 │ vm-subnet 10.20.0.0/24 (unused)    │        │ vm-subnet 10.21.0.0/24 ◄── VM here │
 └────────────────────────────────────┘        └────────────────────────────────────┘
                                                                 │
                                                       jump-box VM (Standard_B1s, Ubuntu 22.04)
                                                       public IP, SSH restricted to a known CIDR
```

Everything lives in a single Azure resource group. The VM can be placed in either region's VNet —
because the VNets are peered and both Private DNS zones are linked to both VNets, it has private
connectivity to **both** clusters regardless of which one it actually sits in.

## Confluent Cloud resources

| Resource | Notes |
|---|---|
| Environment | pre-existing or created fresh |
| Network (primary) | classic Private Link network, westeurope |
| Network (DR) | classic Private Link network, northeurope |
| Cluster (primary) | **Dedicated**, 1 CKU, single-zone, westeurope |
| Cluster (DR) | **Dedicated**, 1 CKU, single-zone, northeurope |
| Connector | `DatagenSource`, `quickstart=USERS`, on the primary cluster, producing sample data |

### A note on cluster type

This setup targets **Dedicated** clusters, which use the "classic" per-cluster/zonal Private Link
model (one Confluent `network` per cluster, individual zonal Private Endpoints, DNS matching the
network's own `dns_domain` directly). **Enterprise** clusters use a different, shared/SNI-routed
Ingress PrivateLink Gateway + Access Point model instead — if you're building this against
Enterprise clusters, expect a different Private Link setup than the one documented here (see
Confluent's docs for the Enterprise-specific flow).

### DNS

For Dedicated clusters, the bootstrap/REST hostname directly matches the Confluent network's own
`dns_domain` — e.g. `<cluster-id>.<dns-domain>.westeurope.azure.confluent.cloud`. Create an Azure
Private DNS zone named exactly after that `dns_domain` (taken verbatim from `confluent network
describe`), with a wildcard `*` A record (all 3 zonal endpoint IPs) plus `*.az1`/`*.az2`/`*.az3`
records (one IP each), linked to both VNets. Same pattern for the DR side.

## Azure resources

| Resource | Details |
|---|---|
| Primary VNet | westeurope, `10.20.0.0/16`, subnets: `vm-subnet` (`10.20.0.0/24`), `privatelink-subnet` (`10.20.1.0/24`) |
| DR VNet | northeurope, `10.21.0.0/16`, subnets: `vm-subnet` (`10.21.0.0/24`), `privatelink-subnet` (`10.21.1.0/24`) |
| VNet peering | bidirectional, `allowVnetAccess=true` |
| Private endpoints | 3 per cluster (one per zone), **Approved** |
| Private DNS zones | one per cluster's `dns_domain`, each with a wildcard `*` record and `*.az1`/`*.az2`/`*.az3` records, linked to **both** VNets |
| NSG (VM) | inbound 22 restricted to your own IP/CIDR only; inbound/outbound 80, 443, 9092 open as needed |
| VM | Standard_B1s, Ubuntu 22.04 LTS, public IP, private IP in whichever VNet you place it |
| SSH key | generate a fresh keypair for the VM; keep the private key out of version control |

If your SSH access to the VM is blocked by policy in your environment, Azure's
`az vm run-command invoke` lets you run shell commands on the VM without SSH (used throughout
`COMMANDS.md` for exactly this reason).

### Note: VM region vs. regional quota

If the region you want for the VM has no vCPU quota headroom in your subscription, you can deploy
it in the *other* region's VNet instead — peering + the DNS zone links above mean it still reaches
both clusters privately either way. Request a quota increase if you specifically need it in a
given region.

## Connecting from the VM

Bootstrap servers / REST endpoints (resolve only from inside the peered VNets):

- Primary: `<primary-cluster-id>.<dns-domain-primary>.westeurope.azure.confluent.cloud:9092` / `:443`
- DR: `<dr-cluster-id>.<dns-domain-dr>.northeurope.azure.confluent.cloud:9092` / `:443`

Verify from the VM:

```
# TLS handshake, both clusters
openssl s_client -connect <primary-bootstrap>:9092 -servername <primary-bootstrap>
openssl s_client -connect <dr-bootstrap>:9092 -servername <dr-bootstrap>

# Kafka protocol, real data
kafkacat -b <primary-bootstrap>:9092 \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN \
  -X sasl.username=<api-key> -X sasl.password=<api-secret> \
  -t <topic> -C -c 3 -e -o -3
```

Credentials go in `env.private` (gitignored).

## DR Automatic Failover tutorial

Following [Confluent's DR Automatic Failover tutorial](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/dr-automatic-failover-tutorial.html)
(an Early Access feature) against the two Dedicated clusters above. This needs a recent Confluent
CLI (`switchover` is a newer command family — upgrade via `brew upgrade confluentinc/tap/cli` if
your `confluent switchover ...` commands aren't recognized).

### Resources

| Resource | Notes |
|---|---|
| Link service account | identity for the cluster link; cluster-agnostic, reusable across rebuilds |
| RBAC (link SA) | `CloudClusterAdmin` on both clusters |
| Cluster link | `BIDIRECTIONAL` mode, `ACTIVE` on both sides |
| Mirror topic | the sample topic, mirrored to DR, `ACTIVE`, 0 lag |
| Switchover pair | `READY_TO_FAILOVER` once set up |
| Switchover endpoint | `READY`, `type=private`, targets both Dedicated clusters' `network` CRNs |
| Global bootstrap URL | the single endpoint clients connect to; DNS CNAME-chases through to your own private zone |
| Test client service account | needs a **global** (not cluster-scoped) API key to use the global bootstrap URL |

### Verified end to end

```
confluent kafka link describe <link-name> --cluster <cluster-id> --environment <env-id>   # ACTIVE
confluent kafka mirror describe <topic> --link <link-name> --cluster <dr-cluster-id> --environment <env-id>
# Mirror Status: ACTIVE, Partition Mirror Lag: 0

confluent switchover pair describe <pair-id> --environment <env-id>      # status.phase: READY_TO_FAILOVER
confluent switchover endpoint describe <endpoint-id> --environment <env-id>  # status.phase: READY

# DNS resolves through the documented CNAME chase, down to your own private zone:
#   <global-bootstrap-url>
#     -> <pair-id>.<endpoint-id>.global.confluent.cloud
#     -> placeholder.<dns-domain>.westeurope.azure.confluent.cloud
#     -> your 3 zonal private-endpoint IPs

kafkacat -b <global-bootstrap-url>:9092 \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN \
  -X sasl.username=<global-api-key> -X sasl.password=<secret> \
  -t <topic> -C -c 3 -e -o -3 -X group.id=<test-group>
```

### Gotchas

- **Dedicated clusters use `network=` CRNs for the switchover endpoint, not `access-point=`** —
  Dedicated doesn't have Enterprise's gateway/access-point concept. Format:
  `crn://confluent.cloud/organization=.../environment=.../network=n-xxxxx`. The endpoint create
  response echoes back the right `hostname` per side to confirm it matched correctly.
- **The global bootstrap URL needs a global API key**, not a cluster-scoped one — create a
  dedicated test-client service account for this if you don't already have one.
- **`confluent iam rbac role-binding list` hides Topic/Group-scoped bindings by default** — add
  `--inclusive` or they look like they silently failed to create (they didn't).
- **`confluent kafka link`/`mirror` commands only work from inside the private network** (they talk
  directly to each cluster's own Kafka REST endpoint) — run them from the VM, not your laptop.
  `confluent switchover pair`/`endpoint` commands, by contrast, hit the public control-plane API and
  work from anywhere with an authenticated CLI session.
- **The Switchover failover REST payload is nested and doesn't take a query param**:
  ```
  POST /switchover/v1/switchover-pairs/{id}:failover
  { "spec": { "active_member": "secondary", "environment_crn": "crn://.../environment=...", "failover_type": "PLANNED" } }
  ```
  A flat `{"active_member": ..., "failover_type": ...}` body or `?environment=` query param (which
  works fine for the `GET` describe call) both get rejected with `400 unknown field "active_member"`.

## Slides

`slides/` — a [Slidev](https://sli.dev) deck covering the problem DR solves, the architecture
built in this project, what actually happens during a failover, and an appendix comparing
Enterprise vs. Dedicated Private Link models.

```
cd slides
pnpm install
pnpm dev      # present at http://localhost:3030 (Space/arrows to navigate, 'o' for overview)
pnpm build    # static export to slides/dist/
```

Note: `vite.config.ts` disables CSS minification for `pnpm build` — without it, this Slidev
version's own bundled CSS crashes the `lightningcss` minifier. `pnpm dev` is unaffected.

## Demo app

`demo-app/` — a small Node.js app (Express +
[`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript),
Confluent's own librdkafka-based client, used via its KafkaJS-compatible API) that gives the DR
Automatic Failover setup above a live, visual demo: a producer and consumer that never change
their configuration across a failover, a single-page dashboard, and buttons that trigger real
failovers. Deploy it as a systemd service on your jump-box VM (the only place with private network
access to the clusters).

### What it does

- **Producer**: a configurable rate (default 10 msg/s) to a topic, via the **global bootstrap
  URL** — not a cluster-specific endpoint.
- **Consumer**: same topic, also via the global bootstrap URL.
- Both connections are **long-lived and never reconfigured** — that's the entire point. When you
  trigger a failover, the only thing that changes is which physical cluster the global endpoint's
  DNS/Private Link routing points to; the client config is identical before and after.
- **Dashboard** (`public/index.html`): producer/consumer status and a live **Health** indicator
  (`ok` / `retrying… (Ns)` / `error` / `disconnected`, derived from time since the last actual
  success, not from `lastError` — see below), switchover pair phase + active member, which
  physical cluster is actually being served (derived from the Switchover Management REST API, not
  a native Kafka connection — see the stability-bug section below), a failover stopwatch with
  three extra milestones (producers resumed / consumers resumed / backup back in sync), topic
  offsets and consumer-group committed offsets/lag side by side, cluster-link direction and live
  mirror lag side by side, a Confluent Cloud Metrics API chart section, and a scrollable,
  clearable event log that includes the exact REST calls issued.
- **Trigger Failover** / **Fail Back** buttons, in both **Planned** and **Unplanned** flavors, plus
  a **Restore Standby** button, all call Confluent's real Switchover Management REST API
  (`POST https://api.confluent.cloud/switchover/v1/switchover-pairs/{id}:failover`, with
  `failover_type` of `PLANNED`, `UNPLANNED`, or `RESTORE`) — the exact request (method, URL, JSON
  body) is logged to the event pane as a distinct "REST call issued" entry every time. See
  "PLANNED vs UNPLANNED failover, and RESTORE" below for what each does.
- **Cluster link pane** also shows `io.confluent.kafka.server/cluster_link_mirror_topic_offset_lag`
  for the demo topic, queried against whichever side is currently the standby — the only side
  where this metric means anything.
- **Metrics section** shows `io.confluent.kafka.server/request_bytes` per cluster, broken down by
  Kafka protocol request type (Produce/Fetch/OffsetCommit), charted over the last 30 minutes.

### Architecture / credentials

| Component | Auth | Why |
|---|---|---|
| Producer, consumer | Kafka **global** API key | Global keys work against the global bootstrap URL *and* either named cluster directly |
| Primary/DR side-by-side offset comparison | Same global API key, direct to each cluster's own bootstrap | Only works for whichever side is currently *active* — see gotcha below |
| Failover/Fail-back buttons, "which side is active" detection | **Cloud API key** (`EnvironmentAdmin` role) | A different credential type/API surface (`api.confluent.cloud`, not Kafka data-plane) — the Kafka API keys above have no permission to call it |
| Metrics API (`request_bytes`, mirror lag) | Same Cloud API key as the Switchover API | The Metrics API (`api.telemetry.confluent.cloud`) accepts the same Cloud API key/secret pair |

No secrets are committed in `demo-app/` — `config.js` reads everything from the environment. Put
real values in this repo's `env.private` (gitignored) and in an `EnvironmentFile=` on the VM's
systemd service (also not committed, `chmod 600`).

### Gotchas found while building this

- **Confluent fences direct data-plane access to the standby cluster.** Connecting directly
  (native protocol, port 9092) to whichever cluster is currently the *passive* member of a
  switchover pair gets you a successful SASL login, immediately followed by the broker closing the
  connection on the first Metadata request — even with a correctly-scoped API key. The *active*
  cluster behaves normally for the same credential. This isn't a bug — it's Confluent intentionally
  preventing clients from bypassing the global bootstrap URL and talking to the standby directly.
  The dashboard shows this state as "standby — direct data-plane access intentionally restricted."
- **kafkajs-style admin clients must call `fetchTopicOffsets` and `fetchOffsets` sequentially, not
  in `Promise.all`**, right after `admin.connect()` — in parallel, `fetchOffsets` intermittently
  fails with `"No brokers in the broker pool"` (a broker-pool/coordinator-discovery race), even
  though `fetchTopicOffsets` on the same connection succeeds every time.
- **Real, measured failover duration**: typically well under a minute from triggering to the global
  bootstrap URL serving the new cluster and production/consumption resuming, with zero message
  loss (producer's sent count exactly matched consumer's received count across repeated test
  failovers/failbacks). See "Why does an UNPLANNED failover take ~1 minute?" below for a detailed
  breakdown of where that time actually goes.

### Customer-facing visibility: REST calls and metrics

Two useful things to show in a live demo: *which REST calls are being made to trigger the
failover*, and *which metrics can be used to observe client behaviour shifting between clusters*.

**REST calls.** The server logs the exact call it's about to make — method, URL, and full JSON
body — as a distinct `cmd`-level event, immediately before calling the Switchover API. Rendered in
the event pane in monospace with its own background, set apart from the narrative log lines.
Deliberately scoped to just the trigger call, not the routine status polls during a cutover watch.

**Metrics.** `metrics.js` is a thin client for the Confluent Cloud Metrics API
(`https://api.telemetry.confluent.cloud/v2/metrics/cloud/query` — pure HTTPS, same Cloud API key
as the Switchover API):

- `io.confluent.kafka.server/request_bytes`, filtered to `resource.kafka.id`, grouped by
  `metric.type` — polled per cluster once a minute (the API itself only samples every 60s, so
  polling faster would just repeat the same sample). Charted side by side for primary/DR so you
  can watch request volume move from one cluster to the other during a failover.
- `io.confluent.kafka.server/cluster_link_mirror_topic_offset_lag`, filtered to
  `resource.kafka.id` + `metric.topic` + `metric.link_name` — only queried against whichever side
  is currently the standby (the active side doesn't have a mirror copy of the topic, so the metric
  is meaningless there).

### Producer/consumer health, not just "last error"

A `lastError`-style field stays `"none"` for the *entire* duration of a cutover, which looks like a
bug but isn't: librdkafka buffers sends and retries subscription/fetch internally, well below the
JS promise layer, so nothing ever actually rejects during a transient outage — it just takes
longer. The real signal is elapsed time since the last *successful* send/receive. Both producer
and consumer track their own `connectedAt`/last-success timestamps; the frontend's
`computeHealth()` compares `now` against them (falling back to `connectedAt` before the first
success) against a threshold of `max(8 × expected inter-message gap, 3s)`, and reports `ok` /
`retrying… (Ns since last success)` / `error: ...` / `disconnected`.

### PLANNED vs UNPLANNED failover, and RESTORE

The Switchover Management API supports three `failover_type`s:

- **PLANNED** — graceful, waits for replication lag to reach zero, then reverses the cluster link.
  No data loss, no resync needed afterward.
- **UNPLANNED** — immediate, does *not* wait for replication to catch up. Models an actual outage
  where the active side can't be waited on. The cluster link is **stopped**, not reversed, which
  leaves the pair in a `READY_TO_RESTORE` phase (not back to `READY_TO_FAILOVER`) until it's
  explicitly resynced.
- **RESTORE** — re-establishes the cluster link after an UNPLANNED failover and resyncs the
  passive side as a proper standby again. It **never changes which side is active** — it's purely
  a repair operation, which is also why the REST API requires `active_member` to be omitted from
  the request entirely (not just empty) for this call.

The dashboard's stopwatch card has three button groups: **Planned**, **Unplanned**, and a single
**Restore Standby** button for after an unplanned failover. Button enablement checks the
switchover pair's actual `status.phase` (`READY_TO_FAILOVER` for any failover/fail-back,
`READY_TO_RESTORE` for Restore) — this stops you from firing a second failover while the pair is
stuck needing a restore first, mirroring the real API's own state machine.

### Why does an UNPLANNED failover take ~1 minute?

The actual Kafka-level work (stopping mirrors, flipping the cluster-link state) finishes in well
under 15 seconds. Tracing a real UNPLANNED fail-back through broker logs showed the dominant cost
is Confluent's own status-reporting path: the component that writes the completion status back to
the control plane (so the Switchover API reflects it) runs on a periodic poll rather than reacting
instantly to the broker's own state change, and the pair isn't reported "done" until **both**
cluster members have independently reported their side of the operation complete. If you're
investigating similar latency, broker logs (filtered to your cluster IDs) around a trigger will
show the gap between the link-state flip and the completion status write.

### Running it / redeploying

```
# Check status / logs
az vm run-command invoke -g <resource-group> -n <vm-name> --command-id RunShellScript \
  --scripts "systemctl status <service-name> --no-pager -l"

# Redeploy a changed file (example: server.js)
B64=$(base64 -i demo-app/server.js)
az vm run-command invoke -g <resource-group> -n <vm-name> --command-id RunShellScript --scripts "
  echo '\$B64' | base64 -d > /opt/<app-dir>/server.js
  systemctl restart <service-name>
"
```

The app and `node_modules` live on the VM, deployed via a base64-encoded tarball through
`az vm run-command` (no SCP needed without SSH access). Full history of how the demo app was built
is in `COMMANDS.md`.

## Cleanup

Delete, in dependency order: switchover endpoint → switchover pair → cluster link (both sides) →
VM → NSG → private endpoints (one per zone per cluster) → VNet peerings → private DNS zone
links/records/zones → VNets → (Confluent) private-link access grants → (Confluent) networks →
connector → service accounts + their API keys + role bindings.

For the demo app: `systemctl disable --now <service-name>` on the VM, then delete the app
directory and its systemd unit; delete the demo topic; delete the backend service account + its
Cloud API key + role binding.
