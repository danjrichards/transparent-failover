// High enough to retain a full demo session's history for post-hoc review (the frontend pane is
// a fixed-height scroll region, not a rolling few-lines view) -- each entry is tiny, so even a
// few thousand of these costs essentially nothing in memory.
const MAX_EVENTS = 2000;
const events = [];

function log(level, msg) {
  events.push({ ts: Date.now(), level, msg });
  if (events.length > MAX_EVENTS) events.shift();
  // eslint-disable-next-line no-console
  console.log(`[${level}] ${msg}`);
}

function recent() {
  return events.slice().reverse();
}

function clear() {
  events.length = 0;
}

module.exports = { log, recent, clear };
