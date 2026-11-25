// Optional: same worker as an AWS Lambda (Function URL) handler.
const initStart = Date.now();
const crypto = require('crypto');
const { buildInvoicePdf } = require('../lib/invoicePdf');
const INIT_MS = Date.now() - initStart;
const INSTANCE_ID = crypto.randomBytes(4).toString('hex');
let invocations = 0;

exports.handler = async (event) => {
  const t0 = Date.now();
  const coldStart = invocations++ === 0;
  const body = JSON.parse(event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString() : event.body || '{}');
  if (process.env.INVOICE_SECRET && (event.headers || {})['x-invoice-secret'] !== process.env.INVOICE_SECRET) {
    return { statusCode: 401, body: 'UNAUTHORIZED' };
  }
  const pdf = await buildInvoicePdf(body.order, body.tenantSlug);
  return {
    statusCode: 200,
    isBase64Encoded: true,
    headers: {
      'content-type': 'application/pdf', 'x-cold-start': String(coldStart), 'x-instance-id': INSTANCE_ID,
      'x-init-ms': String(coldStart ? INIT_MS : 0), 'x-exec-ms': String(Date.now() - t0),
    },
    body: pdf.toString('base64'),
  };
};
