// Standalone consumer process:  npm run consumer
const { startConsumer } = require('./kafka/consumer');
const config = require('./config');

(async () => {
  console.log(`[consumer] starting group="${config.kafka.groupId}" topic="${config.kafka.topic}" brokers=${config.kafka.brokers.join(',')}`);
  const consumer = await startConsumer();
  const shutdown = async (sig) => {
    console.log(`[consumer] ${sig} received, disconnecting...`);
    await consumer.disconnect().catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
})().catch((e) => { console.error('[consumer] fatal', e); process.exit(1); });
