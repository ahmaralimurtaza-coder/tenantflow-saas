// Kafka admin helper
//   npm run kafka:topic   -> create tenant.orders (partitions = KAFKA_PARTITIONS)
//   npm run kafka:status  -> per-partition high-watermark, committed offset, LAG (viva!)
//   npm run kafka:watch   -> refresh status every 2s
const { buildKafka } = require('../src/kafka/client');
const config = require('../src/config');
const { color } = require('./_db');

const cmd = process.argv[2] || 'status';

async function status(admin) {
  const topic = config.kafka.topic;
  const [ends, committed, groups] = await Promise.all([
    admin.fetchTopicOffsets(topic),
    admin.fetchOffsets({ groupId: config.kafka.groupId, topics: [topic] }),
    admin.describeGroups([config.kafka.groupId]),
  ]);
  const comm = new Map((committed[0] ? committed[0].partitions : []).map((p) => [p.partition, Number(p.offset)]));
  const g = groups.groups[0];
  console.log(color.b(`topic=${topic}  group=${config.kafka.groupId}  state=${g.state}  members=${g.members.length}  ${new Date().toLocaleTimeString()}`));
  console.log('partition | high-watermark | committed | lag');
  let totalLag = 0;
  for (const p of ends.sort((a, b) => a.partition - b.partition)) {
    const hw = Number(p.high); const c = comm.get(p.partition); const cv = c >= 0 ? c : 0; const lag = hw - cv;
    totalLag += lag;
    console.log(`${String(p.partition).padStart(9)} | ${String(hw).padStart(14)} | ${String(c >= 0 ? c : '-').padStart(9)} | ${lag ? color.y(lag) : color.g(0)}`);
  }
  console.log(`total lag: ${totalLag ? color.y(totalLag) : color.g(0)}\n`);
}

(async () => {
  const admin = buildKafka('tenantflow-admin').admin();
  await admin.connect();
  try {
    if (cmd === 'topic') {
      const created = await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: config.kafka.topic, numPartitions: config.kafka.partitions, replicationFactor: Number(process.env.KAFKA_REPLICATION || -1) }],
      });
      console.log(created ? color.g(`✔ created ${config.kafka.topic} with ${config.kafka.partitions} partitions`) : color.y(`topic ${config.kafka.topic} already exists`));
      const md = await admin.fetchTopicMetadata({ topics: [config.kafka.topic] });
      for (const p of md.topics[0].partitions) console.log(`  partition ${p.partitionId} leader=${p.leader} replicas=${p.replicas.join(',')}`);
    } else if (cmd === 'watch') {
      for (;;) { console.clear(); await status(admin); await new Promise((r) => setTimeout(r, 2000)); }
    } else {
      await status(admin);
    }
  } finally {
    if (cmd !== 'watch') await admin.disconnect();
  }
})().catch((e) => { console.error(color.r('✘'), e.message); process.exit(1); });
