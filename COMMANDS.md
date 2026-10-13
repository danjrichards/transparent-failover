# Setup commands

A step-by-step command reference for building this from scratch: Confluent Cloud networking and
Dedicated clusters, Azure Private Link plumbing, the jump-box VM, Cluster Linking, the switchover
pair/endpoint, and the demo app. Commands use shell variables for every resource name/ID — set
these to your own values first, then the rest can be copy-pasted section by section.

```bash
# Confluent Cloud
ENV_ID=env-xxxxxxx                  # confluent environment list
ORG_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx   # confluent organization describe -o json
PRIMARY_NET_NAME=tf-primary-network
DR_NET_NAME=tf-dr-network
PRIMARY_CLUSTER_NAME=tf-primary
DR_CLUSTER_NAME=tf-dr

# Azure
RESOURCE_GROUP=tf-rg
AZURE_SUBSCRIPTION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
PRIMARY_REGION=westeurope
DR_REGION=northeurope
VM_NAME=tf-vm
MY_IP=203.0.113.4/32                # the only IP allowed to SSH into the VM
```

## 1. Confluent Cloud networking (classic Private Link, per cluster)

Dedicated clusters use the classic model: one Confluent `network` per cluster, with individual
zonal Private Endpoints — not Enterprise's shared Ingress Gateway/Access Point model. If you're
building this against Enterprise clusters, the Private Link setup is different; see Confluent's
docs for that flow.

```bash
confluent network create $PRIMARY_NET_NAME --cloud azure --region $PRIMARY_REGION \
  --connection-types privatelink --dns-resolution private --environment $ENV_ID
confluent network create $DR_NET_NAME --cloud azure --region $DR_REGION \
  --connection-types privatelink --dns-resolution private --environment $ENV_ID
# Poll `confluent network describe <id>` until phase=READY for both (several minutes each)

confluent network private-link access create tf-primary-pl-access \
  --network <primary-network-id> --cloud azure --cloud-account $AZURE_SUBSCRIPTION_ID \
  --environment $ENV_ID
confluent network private-link access create tf-dr-pl-access \
  --network <dr-network-id> --cloud azure --cloud-account $AZURE_SUBSCRIPTION_ID \
  --environment $ENV_ID

confluent network describe <primary-network-id> -o json   # note dns_domain and azure_private_link_service_resource_ids
confluent network describe <dr-network-id> -o json
```

## 2. Dedicated Kafka clusters

```bash
confluent kafka cluster create ${PRIMARY_CLUSTER_NAME}-NODELETE \
  --cloud azure --region $PRIMARY_REGION --type dedicated --cku 1 --availability single-zone \
  --network <primary-network-id> --environment $ENV_ID

confluent kafka cluster create ${DR_CLUSTER_NAME}-NODELETE \
  --cloud azure --region $DR_REGION --type dedicated --cku 1 --availability single-zone \
  --network <dr-network-id> --environment $ENV_ID
# `--network` is only accepted for `--type dedicated`. Cluster provisioning can take tens of
# minutes to over an hour for Dedicated, longer than Enterprise — poll in the background.

confluent kafka cluster describe <primary-cluster-id> --environment $ENV_ID
# endpoint: <primary-cluster-id>.<dns-domain>.westeurope.azure.confluent.cloud:9092
# Dedicated clusters use the network's own dns_domain directly as the bootstrap hostname.

confluent api-key create --resource <primary-cluster-id> --environment $ENV_ID
# -> save the key/secret to env.private, never commit it
```

## 3. Datagen connector (sample data on the primary cluster)

```bash
confluent connect cluster create --config-file datagen-connector.json \
  --cluster <primary-cluster-id> --environment $ENV_ID
```

`datagen-connector.json`:
```json
{
  "name": "tf-datagen-users",
  "connector.class": "DatagenSource",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "kafka.api.key": "<api-key-from-step-2>",
  "kafka.api.secret": "<api-secret-from-step-2>",
  "kafka.topic": "tf-users",
  "output.data.format": "JSON",
  "quickstart": "USERS",
  "max.interval": "1000",
  "tasks.max": "1"
}
```

## 4. Azure networking

```bash
ssh-keygen -t ed25519 -f keys/${VM_NAME} -N "" -C "${VM_NAME}"

az network vnet create -g $RESOURCE_GROUP -n tf-primary-vnet -l $PRIMARY_REGION \
  --address-prefix 10.20.0.0/16 --subnet-name vm-subnet --subnet-prefix 10.20.0.0/24
az network vnet create -g $RESOURCE_GROUP -n tf-dr-vnet -l $DR_REGION \
  --address-prefix 10.21.0.0/16 --subnet-name vm-subnet --subnet-prefix 10.21.0.0/24

az network vnet subnet create -g $RESOURCE_GROUP --vnet-name tf-primary-vnet \
  -n privatelink-subnet --address-prefix 10.20.1.0/24 --private-endpoint-network-policies Disabled
az network vnet subnet create -g $RESOURCE_GROUP --vnet-name tf-dr-vnet \
  -n privatelink-subnet --address-prefix 10.21.1.0/24 --private-endpoint-network-policies Disabled

az network vnet peering create -g $RESOURCE_GROUP -n primary-to-dr \
  --vnet-name tf-primary-vnet --remote-vnet tf-dr-vnet \
  --allow-vnet-access true --allow-forwarded-traffic true
az network vnet peering create -g $RESOURCE_GROUP -n dr-to-primary \
  --vnet-name tf-dr-vnet --remote-vnet tf-primary-vnet \
  --allow-vnet-access true --allow-forwarded-traffic true

# NSG for the VM — restrict SSH to your own IP, open 80/443/9092 for the demo app and Kafka
az network nsg create -g $RESOURCE_GROUP -n tf-vm-nsg -l $DR_REGION
az network nsg rule create -g $RESOURCE_GROUP --nsg-name tf-vm-nsg -n Allow-SSH \
  --priority 100 --direction Inbound --access Allow --protocol Tcp \
  --source-address-prefixes $MY_IP --destination-port-ranges 22
az network nsg rule create -g $RESOURCE_GROUP --nsg-name tf-vm-nsg -n Allow-Inbound \
  --priority 110 --direction Inbound --access Allow --protocol Tcp \
  --source-address-prefixes '*' --destination-port-ranges 80 443 9092
az network nsg rule create -g $RESOURCE_GROUP --nsg-name tf-vm-nsg -n Allow-Outbound \
  --priority 120 --direction Outbound --access Allow --protocol Tcp \
  --source-address-prefixes '*' --destination-port-ranges 80 443 9092

# Private endpoints: one per zone per cluster, targeting that network's zonal PLS resource IDs
# (from `confluent network describe` above). --manual-request true is needed — auto-approval
# (--manual-request false) fails with LinkedAuthorizationFailed; the private-link access grant in
# step 1 lets Confluent's side auto-approve the manual request instead.
for z in 1 2 3; do
  az network private-endpoint create -g $RESOURCE_GROUP -n tf-primary-pe-az$z -l $PRIMARY_REGION \
    --vnet-name tf-primary-vnet --subnet privatelink-subnet \
    --connection-name tf-primary-conn-az$z \
    --private-connection-resource-id "<zonal-pls-resource-id-az$z>" \
    --manual-request true
done
for z in 1 2 3; do
  az network private-endpoint create -g $RESOURCE_GROUP -n tf-dr-pe-az$z -l $DR_REGION \
    --vnet-name tf-dr-vnet --subnet privatelink-subnet \
    --connection-name tf-dr-conn-az$z \
    --private-connection-resource-id "<zonal-pls-resource-id-az$z>" \
    --manual-request true
done
# Note the resulting private IPs (az network nic show on each PE's NIC) — you'll need them below.

# Private DNS zones, named exactly after each network's dns_domain (from `confluent network
# describe`). Link BOTH zones to BOTH VNets — if you skip linking the primary zone to the DR
# VNet (or vice versa), DNS lookups from a VM in that VNet silently fall through to Confluent's
# own public answer for the same hostname, which resolves to internal-only IPs that aren't
# reachable from your network.
az network private-dns zone create -g $RESOURCE_GROUP -n <primary-dns-domain>
az network private-dns zone create -g $RESOURCE_GROUP -n <dr-dns-domain>
az network private-dns link vnet create -g $RESOURCE_GROUP -n primary-zone-primary-vnet-link \
  --zone-name <primary-dns-domain> --virtual-network tf-primary-vnet --registration-enabled false
az network private-dns link vnet create -g $RESOURCE_GROUP -n primary-zone-dr-vnet-link \
  --zone-name <primary-dns-domain> --virtual-network tf-dr-vnet --registration-enabled false
az network private-dns link vnet create -g $RESOURCE_GROUP -n dr-zone-dr-vnet-link \
  --zone-name <dr-dns-domain> --virtual-network tf-dr-vnet --registration-enabled false
az network private-dns link vnet create -g $RESOURCE_GROUP -n dr-zone-primary-vnet-link \
  --zone-name <dr-dns-domain> --virtual-network tf-primary-vnet --registration-enabled false

# Wildcard + per-zone A records (repeat per zone/IP collected above)
az network private-dns record-set a create -g $RESOURCE_GROUP -z <primary-dns-domain> -n "*"
az network private-dns record-set a add-record -g $RESOURCE_GROUP -z <primary-dns-domain> -n "*" -a <az1-ip>
az network private-dns record-set a add-record -g $RESOURCE_GROUP -z <primary-dns-domain> -n "*" -a <az2-ip>
az network private-dns record-set a add-record -g $RESOURCE_GROUP -z <primary-dns-domain> -n "*" -a <az3-ip>
az network private-dns record-set a create -g $RESOURCE_GROUP -z <primary-dns-domain> -n "*.az1"
az network private-dns record-set a add-record -g $RESOURCE_GROUP -z <primary-dns-domain> -n "*.az1" -a <az1-ip>
# ...and so on for *.az2/*.az3, and the same pattern for the DR zone.
```

## 5. VM

```bash
az vm create -g $RESOURCE_GROUP -n $VM_NAME -l $DR_REGION \
  --image Ubuntu2204 --size Standard_B1s \
  --vnet-name tf-dr-vnet --subnet vm-subnet --nsg tf-vm-nsg \
  --public-ip-sku Standard --public-ip-address ${VM_NAME}-pip \
  --admin-username azureuser --ssh-key-values keys/${VM_NAME}.pub
# If your target region has no regional vCPU quota headroom, deploy into the other region's VNet
# instead (check with `az vm list-usage -l <region> -o table`) — peering + the DNS zone links
# above mean the VM still reaches both clusters privately either way.
```

Verify connectivity from the VM:

```bash
openssl s_client -connect <primary-cluster-id>.<primary-dns-domain>.westeurope.azure.confluent.cloud:9092 \
  -servername <primary-cluster-id>.<primary-dns-domain>.westeurope.azure.confluent.cloud
openssl s_client -connect <dr-cluster-id>.<dr-dns-domain>.northeurope.azure.confluent.cloud:9092 \
  -servername <dr-cluster-id>.<dr-dns-domain>.northeurope.azure.confluent.cloud
# -> CONNECTED on both

kafkacat -b <primary-cluster-id>.<primary-dns-domain>.westeurope.azure.confluent.cloud:9092 \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN \
  -X sasl.username=<api-key> -X sasl.password=<api-secret> \
  -t tf-users -C -c 3 -e -o -3
# -> real datagen records
```

If SSH access to the VM is restricted in your environment, `az vm run-command invoke -g
$RESOURCE_GROUP -n $VM_NAME --command-id RunShellScript --scripts "..."` runs shell commands on
the VM without needing SSH — used for everything below.

## 6. Cluster Linking (DR Automatic Failover prerequisite)

Needs a recent Confluent CLI — the `switchover` command family is relatively new
(`brew upgrade confluentinc/tap/cli` if `confluent switchover ...` isn't recognized).

`confluent kafka link`/`mirror` commands talk directly to each cluster's own Kafka REST endpoint
(only resolvable from inside the private network) — run them from the VM. `confluent switchover
pair`/`endpoint` commands hit the public control-plane API and work from anywhere with an
authenticated CLI session.

```bash
confluent iam service-account create tf-dr-link-sa --description "Cluster link identity"
confluent iam rbac role-binding create --principal User:<link-sa-id> \
  --role CloudClusterAdmin --environment $ENV_ID --cloud-cluster <primary-cluster-id>
confluent iam rbac role-binding create --principal User:<link-sa-id> \
  --role CloudClusterAdmin --environment $ENV_ID --cloud-cluster <dr-cluster-id>
confluent api-key create --resource global --service-account <link-sa-id>
# -> save to env.private; this key is global and cluster-agnostic
```

`dr-link.config` — copy [`dr-link-example.config`](./dr-link-example.config) and fill in the link
service account's key/secret on both `sasl.jaas.config` and `local.sasl.jaas.config` (both sides
use the same identity here). Keep the real file out of version control (already gitignored).

On the VM:

```bash
confluent kafka link create dr-link --remote-cluster <dr-cluster-id> \
  --remote-bootstrap-server <dr-cluster-id>.<dr-dns-domain>.northeurope.azure.confluent.cloud:9092 \
  --config /home/azureuser/dr-link.config --cluster <primary-cluster-id> --environment $ENV_ID

confluent kafka link create dr-link --remote-cluster <primary-cluster-id> \
  --remote-bootstrap-server <primary-cluster-id>.<primary-dns-domain>.westeurope.azure.confluent.cloud:9092 \
  --config /home/azureuser/dr-link.config --cluster <dr-cluster-id> --environment $ENV_ID

confluent kafka link describe dr-link --cluster <primary-cluster-id> --environment $ENV_ID   # ACTIVE
confluent kafka link describe dr-link --cluster <dr-cluster-id> --environment $ENV_ID        # ACTIVE
confluent kafka mirror describe tf-users --link dr-link --cluster <dr-cluster-id> --environment $ENV_ID
# Mirror Status: ACTIVE, Partition Mirror Lag: 0
```

## 7. Switchover pair and endpoint

```bash
confluent switchover pair create tf-dr-pair \
  --member name=primary,crn=crn://confluent.cloud/organization=$ORG_ID/environment=$ENV_ID/cloud-cluster=<primary-cluster-id> \
  --member name=secondary,crn=crn://confluent.cloud/organization=$ORG_ID/environment=$ENV_ID/cloud-cluster=<dr-cluster-id> \
  --active-member primary --environment $ENV_ID
# Poll `confluent switchover pair describe <pair-id> --environment $ENV_ID` until
# status.phase: READY_TO_FAILOVER

# Dedicated clusters use classic `network` resources (no gateway/access-point), so the endpoint's
# network= CRN component points directly at your `network` IDs:
confluent switchover endpoint create tf-dr-pair-endpoint \
  --parent-resource-crn crn://confluent.cloud/organization=$ORG_ID/environment=$ENV_ID/switchover-pair=<pair-id> \
  --endpoint name=primary-ep,type=private,network=crn://confluent.cloud/organization=$ORG_ID/environment=$ENV_ID/network=<primary-network-id> \
  --endpoint name=secondary-ep,type=private,network=crn://confluent.cloud/organization=$ORG_ID/environment=$ENV_ID/network=<dr-network-id> \
  --environment $ENV_ID
# Poll until status.phase: READY. The response's endpoints[].hostname fields confirm the right
# network was matched to each side.
```

Global bootstrap URL: `glkc-<pair-id>-<endpoint-id>.global.glb.confluent.cloud:9092`

```bash
nslookup glkc-<pair-id>-<endpoint-id>.global.glb.confluent.cloud
# CNAME chase:
#   glkc-<pair-id>-<endpoint-id>.global.glb.confluent.cloud
#     -> CNAME glkc-<pair-id>.<endpoint-id>.global.confluent.cloud   (glb stripped, dash->dot)
#     -> CNAME placeholder.<primary-dns-domain>.westeurope.azure.confluent.cloud   (your own private zone)
#     -> A <your 3 zonal IPs>

openssl s_client -connect glkc-<pair-id>-<endpoint-id>.global.glb.confluent.cloud:9092 \
  -servername glkc-<pair-id>-<endpoint-id>.global.glb.confluent.cloud
# -> CONNECTED, cert CN = *.global.glb.confluent.cloud
```

Testing through the global endpoint needs a **global** (not cluster-scoped) API key, plus RBAC on
both clusters:

```bash
confluent iam service-account create tf-dr-client-sa --description "Global bootstrap test client"
confluent api-key create --resource global --service-account <client-sa-id>

for cluster in <primary-cluster-id> <dr-cluster-id>; do
  confluent iam rbac role-binding create --principal User:<client-sa-id> --role DeveloperWrite \
    --resource Topic:tf-users --environment $ENV_ID --cloud-cluster $cluster --kafka-cluster $cluster
  confluent iam rbac role-binding create --principal User:<client-sa-id> --role DeveloperRead \
    --resource Topic:tf-users --environment $ENV_ID --cloud-cluster $cluster --kafka-cluster $cluster
  confluent iam rbac role-binding create --principal User:<client-sa-id> --role DeveloperRead \
    --resource Group:dr-test-consumer --environment $ENV_ID --cloud-cluster $cluster --kafka-cluster $cluster
done
# `confluent iam rbac role-binding list` hides Topic/Group-scoped bindings by default -- add
# --inclusive to see them (they aren't silently failing to create).

kafkacat -b glkc-<pair-id>-<endpoint-id>.global.glb.confluent.cloud:9092 \
  -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN \
  -X sasl.username=<global-api-key> -X sasl.password=<secret> \
  -t tf-users -C -c 3 -e -o -3 -X group.id=dr-test-consumer
# -> real records consumed through the global bootstrap URL
```

## 8. Demo app

```bash
confluent iam service-account create tf-demo-backend-sa \
  --description "Calls the Switchover Management API for the demo dashboard"
confluent iam rbac role-binding create --principal User:<backend-sa-id> \
  --role EnvironmentAdmin --environment $ENV_ID
confluent api-key create --resource cloud --service-account <backend-sa-id>
# -> a Cloud API key (different surface from the Kafka data-plane keys above); also works for the
#    Metrics API (api.telemetry.confluent.cloud) -- no separate credential needed.

confluent kafka topic create tf-orders --partitions 6 --cluster <primary-cluster-id> --environment $ENV_ID
# Auto-mirrors to DR immediately if dr-link's auto.create.mirror.topics.filters covers it (a
# wildcard filter, as in dr-link-example.config, covers any new topic).

for cluster in <primary-cluster-id> <dr-cluster-id>; do
  confluent iam rbac role-binding create --principal User:<client-sa-id> --role DeveloperWrite \
    --resource Topic:tf-orders --environment $ENV_ID --cloud-cluster $cluster --kafka-cluster $cluster
  confluent iam rbac role-binding create --principal User:<client-sa-id> --role DeveloperRead \
    --resource Topic:tf-orders --environment $ENV_ID --cloud-cluster $cluster --kafka-cluster $cluster
  confluent iam rbac role-binding create --principal User:<client-sa-id> --role DeveloperRead \
    --resource Group:tf-orders-consumer --environment $ENV_ID --cloud-cluster $cluster --kafka-cluster $cluster
done
```

On the VM:

```bash
# Node.js 20 LTS
curl -fsSL https://deb.nodesource.com/setup_20.x -o /tmp/nodesource_setup.sh
sudo bash /tmp/nodesource_setup.sh
sudo apt-get install -y nodejs
```

Deploy by tarring `demo-app/` locally (excluding `node_modules`), base64-encoding it, and
writing+extracting it via `az vm run-command` (no SCP without SSH access):

```bash
tar -czf /tmp/demo-app.tar.gz --exclude=node_modules -C demo-app .
B64=$(base64 -i /tmp/demo-app.tar.gz | tr -d '\n')
az vm run-command invoke -g $RESOURCE_GROUP -n $VM_NAME --command-id RunShellScript --scripts "
  mkdir -p /opt/tf-demo
  echo '$B64' | base64 -d > /tmp/demo-app.tar.gz
  tar -xzf /tmp/demo-app.tar.gz -C /opt/tf-demo
  cd /opt/tf-demo && npm install --omit=dev
"
# macOS tar adds AppleDouble ._* metadata files to the archive -- harmless, but
# `find /opt/tf-demo -name '._*' -delete` cleans them up if you want to.
```

Write real credentials to `/opt/tf-demo/demo.env` on the VM separately (never in the committed
tarball), `chmod 600`. Values containing spaces/parens (e.g. cluster labels) must be quoted for
systemd's `EnvironmentFile=` parser — unquoted, it silently mis-parses them.

Systemd service (`/etc/systemd/system/tf-demo.service`), `User=root` (needed to bind port 80
directly), `EnvironmentFile=demo.env`, `Restart=on-failure`, `ExecStart=/usr/bin/node server.js`,
`WorkingDirectory=/opt/tf-demo`:

```bash
systemctl daemon-reload && systemctl enable --now tf-demo
systemctl status tf-demo --no-pager -l
journalctl -u tf-demo -f
```

Redeploy a changed file (example: `server.js`):

```bash
B64=$(base64 -i demo-app/server.js | tr -d '\n')
az vm run-command invoke -g $RESOURCE_GROUP -n $VM_NAME --command-id RunShellScript --scripts "
  echo '$B64' | base64 -d > /opt/tf-demo/server.js
  systemctl restart tf-demo
"
```

## 9. Running a failover test

```bash
confluent switchover pair failover <pair-id> --active-member secondary --failover-type PLANNED \
  --environment $ENV_ID
# --active-member is required for PLANNED/UNPLANNED, and must be OMITTED ENTIRELY (not empty) for
# RESTORE -- the API rejects a RESTORE request that includes it.
confluent switchover pair describe <pair-id> --environment $ENV_ID   # poll until settled
```

Or just use the demo dashboard's Planned/Unplanned/Restore buttons, which call the same REST API
directly (`POST https://api.confluent.cloud/switchover/v1/switchover-pairs/{id}:failover`) and log
the exact request to the event pane.

Expect producer/consumer to show zero or near-zero message loss across a failover — sent/received
counts should match once traffic resumes. See README.md's "Why does an UNPLANNED failover take ~1
minute?" if a failover's control-plane-reported duration looks surprisingly long relative to how
fast the actual mirror-topic work completes.

## Cleanup

Delete in dependency order:

```bash
confluent switchover endpoint delete <endpoint-id> --environment $ENV_ID --force
confluent switchover pair delete <pair-id> --environment $ENV_ID --force
confluent kafka link delete dr-link --cluster <primary-cluster-id> --environment $ENV_ID --force
confluent kafka link delete dr-link --cluster <dr-cluster-id> --environment $ENV_ID --force

az vm delete -g $RESOURCE_GROUP -n $VM_NAME -y
az network nsg delete -g $RESOURCE_GROUP -n tf-vm-nsg
for z in 1 2 3; do
  az network private-endpoint delete -g $RESOURCE_GROUP -n tf-primary-pe-az$z
  az network private-endpoint delete -g $RESOURCE_GROUP -n tf-dr-pe-az$z
done
az network vnet peering delete -g $RESOURCE_GROUP -n primary-to-dr --vnet-name tf-primary-vnet
az network vnet peering delete -g $RESOURCE_GROUP -n dr-to-primary --vnet-name tf-dr-vnet
# Delete the private DNS zone links/records/zones and the two VNets similarly.

confluent network private-link access delete <pla-id-primary> --environment $ENV_ID --force
confluent network private-link access delete <pla-id-dr> --environment $ENV_ID --force
confluent network delete <primary-network-id> --environment $ENV_ID --force
confluent network delete <dr-network-id> --environment $ENV_ID --force

confluent connect cluster delete <connector-id> --cluster <primary-cluster-id> --environment $ENV_ID --force
confluent iam service-account delete <link-sa-id> --force
confluent iam service-account delete <client-sa-id> --force
confluent iam service-account delete <backend-sa-id> --force
# Deleting a service account also revokes its API keys and role bindings.
```

For the demo app: `systemctl disable --now tf-demo` on the VM, then delete `/opt/tf-demo` and
`/etc/systemd/system/tf-demo.service`.
