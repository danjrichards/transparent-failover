// All configuration comes from the environment (set via systemd's EnvironmentFile= on the VM).
// Nothing secret is hardcoded here or anywhere else in this directory.

function required(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

module.exports = {
  port: parseInt(process.env.PORT || '80', 10),

  globalBootstrap: required('GLOBAL_BOOTSTRAP'),
  primaryBootstrap: required('PRIMARY_BOOTSTRAP'),
  drBootstrap: required('DR_BOOTSTRAP'),
  primaryClusterId: required('PRIMARY_CLUSTER_ID'),
  drClusterId: required('DR_CLUSTER_ID'),
  primaryLabel: process.env.PRIMARY_LABEL || 'Primary (westeurope)',
  drLabel: process.env.DR_LABEL || 'DR (northeurope)',

  kafkaApiKey: required('KAFKA_API_KEY'),
  kafkaApiSecret: required('KAFKA_API_SECRET'),

  cloudApiKey: required('CLOUD_API_KEY'),
  cloudApiSecret: required('CLOUD_API_SECRET'),
  envId: required('CONFLUENT_ENV_ID'),
  orgId: required('CONFLUENT_ORG_ID'),
  switchoverPairId: required('SWITCHOVER_PAIR_ID'),

  topic: process.env.TOPIC || 'danr-tf-orders',
  group: process.env.CONSUMER_GROUP || 'danr-tf-orders-consumer',
  // NOTHING | ERROR | WARN | INFO | DEBUG -- applies to the producer/consumer (global) client only.
  kafkaLogLevel: (process.env.KAFKA_LOG_LEVEL || 'INFO').toUpperCase(),
  producePerSec: parseInt(process.env.PRODUCE_RATE_PER_SEC || '10', 10),
};
