const fs = require('fs');
const path = require('path');
const { adminPool, color } = require('./_db');

(async () => {
  const sql = fs.readFileSync(path.join(__dirname, '..', 'src', 'db', 'schema.sql'), 'utf8');
  await adminPool.query(sql);
  console.log(color.g('✔ schema migrated (tables + RLS policies + app_tenant role)'));
  await adminPool.end();
})().catch((e) => { console.error(color.r('✘ migration failed:'), e.message); process.exit(1); });
