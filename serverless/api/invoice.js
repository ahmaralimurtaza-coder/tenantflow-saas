// Vercel Serverless Function: POST /api/invoice -> application/pdf
// Cold-start detection: module scope runs ONCE per container instance.
// The first invocation on a fresh instance is a cold start; later ones reuse it (warm).
const MODULE_LOADED_AT = Date.now();
const initStart = process.hrtime.bigint();
const crypto = require('crypto');
const { buildInvoicePdf } = require('../lib/invoicePdf'); // pulls in pdfkit (heavy) -> part of init cost
const INIT_MS = Number(process.hrtime.bigint() - initStart) / 1e6;
const INSTANCE_ID = crypto.randomBytes(4).toString('hex');
let invocations = 0;

module.exports = async (req, res) => {
  const t0 = process.hrtime.bigint();
  const coldStart = invocations === 0;
  invocations += 1;

  res.setHeader('x-cold-start', String(coldStart));
  res.setHeader('x-instance-id', INSTANCE_ID);
  res.setHeader('x-init-ms', coldStart ? INIT_MS.toFixed(1) : '0');
  res.setHeader('x-invocation', String(invocations));

  if (req.method === 'GET') {
    return res.status(200).json({ ok: true, coldStart, instanceId: INSTANCE_ID, invocations, initMs: INIT_MS, uptimeMs: Date.now() - MODULE_LOADED_AT });
  }
  if (req.method !== 'POST') return res.status(405).json({ error: 'METHOD_NOT_ALLOWED' });
  if (process.env.INVOICE_SECRET && req.headers['x-invoice-secret'] !== process.env.INVOICE_SECRET) {
    return res.status(401).json({ error: 'UNAUTHORIZED' });
  }
  const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
  if (!body.order || !body.order.id) return res.status(400).json({ error: 'order required' });

  const pdf = await buildInvoicePdf(body.order, body.tenantSlug);
  const execMs = Number(process.hrtime.bigint() - t0) / 1e6;
  console.log(JSON.stringify({ fn: 'invoice', coldStart, instanceId: INSTANCE_ID, invocation: invocations, initMs: coldStart ? INIT_MS : 0, execMs, order: body.order.id, tenant: body.tenantSlug }));
  res.setHeader('x-exec-ms', execMs.toFixed(1));
  res.setHeader('content-type', 'application/pdf');
  res.setHeader('content-disposition', `inline; filename="invoice-${String(body.order.id).slice(0, 8)}.pdf"`);
  return res.status(200).send(pdf);
};
