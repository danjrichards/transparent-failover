const cfg = require('./config');
const events = require('./events');

class OrderConsumer {
  constructor(kafka) {
    this.kafka = kafka;
    this.consumer = kafka.consumer({
      kafkaJS: {
        groupId: cfg.group,
        sessionTimeout: 15000,
        retry: { retries: 10, initialRetryTime: 300, maxRetryTime: 5000 },
        fromBeginning: false,
      },
    });
    this.state = {
      connected: false,
      running: false,
      receivedTotal: 0,
      lastError: null,
      lastReceivedAt: null,
      lastSeq: null,
      lastPartition: null,
      consecutiveFailures: 0,
      connectedAt: null,
    };
  }

  async start() {
    await this.consumer.connect();
    this.state.connected = true;
    this.state.connectedAt = Date.now();
    await this.consumer.subscribe({ topic: cfg.topic });
    events.log('info', 'Consumer connected (via global bootstrap URL)');

    // Note: this client's KafkaJS compat layer doesn't implement consumer.events/.on() the way
    // kafkajs does (`consumer.events.CRASH` throws "Not implemented"). eachMessage's own
    // try/catch below is how we track consecutiveFailures/lastError instead.
    await this.consumer.run({
      kafkaJS: { autoCommitInterval: 1000 },
      eachMessage: async ({ partition, message }) => {
        this.state.running = true;
        try {
          this.state.receivedTotal++;
          this.state.lastReceivedAt = Date.now();
          this.state.lastPartition = partition;
          try {
            const parsed = JSON.parse(message.value.toString());
            this.state.lastSeq = parsed.seq;
          } catch {
            // ignore parse errors, counts still increment
          }
          if (this.state.consecutiveFailures > 0) {
            events.log('ok', 'Consumer resumed receiving messages');
            this.state.consecutiveFailures = 0;
            this.state.lastError = null;
          }
        } catch (err) {
          this.state.consecutiveFailures++;
          this.state.lastError = err.message;
          if (this.state.consecutiveFailures === 1) {
            events.log('warn', `Consumer eachMessage error: ${err.message}`);
          }
        }
      },
    });
  }

  async stop() {
    this.state.running = false;
    await this.consumer.disconnect();
  }
}

module.exports = { OrderConsumer };
