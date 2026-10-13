const cfg = require('./config');
const events = require('./events');

class OrderProducer {
  constructor(kafka) {
    this.kafka = kafka;
    this.producer = kafka.producer({ kafkaJS: { allowAutoTopicCreation: false } });
    this.seq = 0;
    this.state = {
      connected: false,
      running: false,
      sentTotal: 0,
      failedTotal: 0,
      consecutiveFailures: 0,
      lastError: null,
      lastSendAt: null,
      lastSuccessAt: null,
      connectedAt: null,
      ratePerSec: cfg.producePerSec,
    };
  }

  async start() {
    await this.producer.connect();
    this.state.connected = true;
    this.state.running = true;
    this.state.connectedAt = Date.now();
    events.log('info', 'Producer connected (via global bootstrap URL)');
    this._tick();
  }

  _tick() {
    const intervalMs = Math.round(1000 / cfg.producePerSec);
    this.timer = setInterval(() => this._send(), intervalMs);
  }

  async _send() {
    const seq = this.seq++;
    const payload = JSON.stringify({ seq, ts: Date.now() });
    this.state.lastSendAt = Date.now();
    try {
      await this.producer.send({
        topic: cfg.topic,
        messages: [{ key: String(seq), value: payload }],
      });
      this.state.sentTotal++;
      if (this.state.consecutiveFailures >= 3) {
        events.log('ok', `Producer resumed after ${this.state.consecutiveFailures} failed attempt(s)`);
      }
      this.state.consecutiveFailures = 0;
      this.state.lastError = null;
      this.state.lastSuccessAt = Date.now();
    } catch (err) {
      this.state.failedTotal++;
      this.state.consecutiveFailures++;
      this.state.lastError = err.message;
      if (this.state.consecutiveFailures === 1) {
        events.log('warn', `Producer send failed: ${err.message}`);
      }
    }
  }

  async stop() {
    clearInterval(this.timer);
    this.state.running = false;
    await this.producer.disconnect();
  }
}

module.exports = { OrderProducer };
