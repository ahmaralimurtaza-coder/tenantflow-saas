// Serverless cold vs warm start benchmark
//   INVOICE_FUNCTION_URL=https://<app>.vercel.app/api/invoice npm run bench:coldstart
// Tip: run once after the function has been idle 15+ minutes (or right after a redeploy)
// so the first call is a genuine cold start.
const fs = require('fs');
const path = require('path');
const config = require('../src/config');
const { color, median } = require('./_db');

const URL_ = process.env.INVOICE_FUNCTION_URL || config.invoiceFunctionUrl;
const WARM = Number(process.env.WARM_CALLS || 10);
const order = { id: '00000000-0000-4000-8000-000000000001', customerName: 'Benchmark Customer', createdAt: new Date().toISOString(), totalCents: 4500, items: [{ name: 'USB-C Hub', quantity: 1, unitPriceCents: 4500 }] };

async function call() {
  const s = Date.now();
  const r = await fetch(URL_, { method: 'POST', headers: { 'content-type': 'application/json', 'x-invoice-secret': config.invoiceSecret }, body: JSON.stringify({ order, tenantSlug: 'bench' }) });
  await r.arrayBuffer();
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return { roundtrip: Date.now() - s, cold: r.headers.get('x-cold-start') === 'true', fn: Number(r.headers.get('x-exec-ms')), init: Number(r.headers.get('x-init-ms') || 0), instance: r.headers.get('x-instance-id') };
}

(async () => {
  if (!URL_) throw new Error('Set INVOICE_FUNCTION_URL');
  console.log(color.b(`Cold/Warm benchmark → ${URL_}`));
  const rows = [];
  for (let i = 0; i <= WARM; i++) {
    const r = await call();
    rows.push(r);
    console.log(`#${String(i).padStart(2)} ${r.cold ? color.y('COLD') : color.g('WARM')} roundtrip=${r.roundtrip}ms fn=${r.fn}ms init=${r.init}ms instance=${r.instance}`);
  }
  const cold = rows.filter((r) => r.cold); const warm = rows.filter((r) => !r.cold);
  const line = (label, arr) => arr.length ? `| ${label} | ${arr.length} | ${median(arr.map((r) => r.roundtrip))} | ${median(arr.map((r) => r.fn))} | ${median(arr.map((r) => r.init))} |` : `| ${label} | 0 | – | – | – |`;
  const md = `| Start type | Calls | Median round-trip (ms) | Median handler time (ms) | Module init (ms) |\n|---|---:|---:|---:|---:|\n${line('Cold start', cold)}\n${line('Warm start', warm)}\n\n_${URL_.replace(/^https?:\/\//, '').split('/')[0]}, ${new Date().toISOString()}_\n`;
  fs.mkdirSync(path.join(__dirname, '..', 'docs', 'benchmarks'), { recursive: true });
  fs.writeFileSync(path.join(__dirname, '..', 'docs', 'benchmarks', 'coldstart.md'), md);
  console.log('\n' + md);
  if (!cold.length) console.log(color.y('No cold start captured — wait ~15 min idle (or redeploy) and run again.'));
})().catch((e) => { console.error(color.r('✘'), e.message); process.exit(1); });
