// OrderCreated producer.
// Message KEY = tenant_id  -> Kafka hashes the key (murmur2) to pick a partition,
// so all events of one tenant land on the SAME partition => strict ordering per tenant.
const { buildKafka, Partitioners } = require('./client');
const config = require('../config');

let producer;
let connecting;

async function getProducer() {
  if (producer) return producer;
  if (!connecting) {
    const p = buildKafka(config.kafka.clientId).producer({
      idempotent: true,          // no duplicates on retry
      maxInFlightRequests: 1,    // keeps ordering guarantees with retries
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    connecting = p.connect().then(() => { producer = p; return p; })
      .catch((e) => { connecting = null; throw e; });
  }
  return connecting;
}

async function publishOrderCreated(event) {
  const p = await getProducer();
  const [meta] = await p.send({
    topic: config.kafka.topic,
    acks: -1, // wait for all in-sync replicas
    messages: [{
      key: event.tenantId,
      value: JSON.stringify(event),
      headers: { 'event-type': 'OrderCreated', 'tenant-id': event.tenantId, 'event-id': event.eventId },
    }],
  });
  return { partition: meta.partition, offset: meta.baseOffset };
}

async function disconnectProducer() {
  if (producer) await producer.disconnect().catch(() => {});
  producer = null; connecting = null;
}

module.exports = { publishOrderCreated, disconnectProducer, getProducer };
