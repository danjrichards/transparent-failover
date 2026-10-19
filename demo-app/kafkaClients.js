// Confluent's own Node.js client (librdkafka-based), used via its KafkaJS-compatible API so the
// rest of this app's code (written against kafkajs's shape) didn't need a rewrite. Note: unlike
// kafkajs itself, this client requires KafkaJS-specific options (clientId, ssl, sasl, timeouts,
// retry, logLevel) nested under a `kafkaJS` block -- anything at the top level is treated as a
// native librdkafka global config property instead, and e.g. a top-level `clientId` throws.
const { Kafka, logLevel } = require('@confluentinc/kafka-javascript').KafkaJS;
const cfg = require('./config');

function make(clientId, brokers, level = 'NOTHING') {
  return new Kafka({
    kafkaJS: {
      clientId,
      brokers: [brokers],
      ssl: true,
      sasl: { mechanism: 'plain', username: cfg.kafkaApiKey, password: cfg.kafkaApiSecret },
      connectionTimeout: 8000,
      requestTimeout: 12000,
      retry: { retries: 10, initialRetryTime: 300, maxRetryTime: 5000 },
      logLevel: logLevel ? logLevel[level] ?? logLevel.INFO : undefined,
    },
  });
}

// Long-lived: the producer and consumer talk ONLY to the global bootstrap URL. This is the
// whole point of the demo -- the client config never changes across a failover.
const kafkaGlobal = make('danr-tf-demo-global', cfg.globalBootstrap, cfg.kafkaLogLevel);

// Direct-to-cluster admin connections, used purely for the dashboard's side-by-side
// primary/DR comparison (topic + consumer-group offsets, proof of mirroring). Real clients in
// this demo never use these -- only the monitoring layer does.
const kafkaPrimaryAdmin = make('danr-tf-demo-primary-admin', cfg.primaryBootstrap);
const kafkaDrAdmin = make('danr-tf-demo-dr-admin', cfg.drBootstrap);

module.exports = { kafkaGlobal, kafkaPrimaryAdmin, kafkaDrAdmin };
