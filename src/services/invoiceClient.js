// Calls the Serverless invoice function (Vercel) and records cold/warm latency.
const config = require('../config');

async function requestInvoice(order, { tenantSlug } = {}) {
  if (!config.invoiceFunctionUrl) return null;
  const started = Date.now();
  const res = await fetch(config.invoiceFunctionUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-invoice-secret': config.invoiceSecret },
    body: JSON.stringify({ order, tenantSlug }),
    signal: AbortSignal.timeout(20000),
  });
  const buf = Buffer.from(await res.arrayBuffer());
  if (!res.ok) throw new Error(`invoice function ${res.status}: ${buf.toString().slice(0, 200)}`);
  return {
    pdf: buf,
    coldStart: res.headers.get('x-cold-start') === 'true',
    instanceId: res.headers.get('x-instance-id'),
    functionMs: Math.round(Number(res.headers.get('x-exec-ms') || 0)),
    roundtripMs: Date.now() - started,
  };
}

module.exports = { requestInvoice };
