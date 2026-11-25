// Local runner for the Vercel function (no Vercel CLI needed):  node serverless/dev-server.js
// -> http://localhost:3001/api/invoice
const http = require('http');
const handler = require('./api/invoice');

http.createServer((req, res) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', async () => {
    try { req.body = raw ? JSON.parse(raw) : {}; } catch { req.body = {}; }
    res.status = (c) => { res.statusCode = c; return res; };
    res.json = (o) => { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(o)); return res; };
    res.send = (b) => { res.end(b); return res; };
    if (!req.url.startsWith('/api/invoice')) return res.status(404).json({ error: 'not found' });
    try { await handler(req, res); } catch (e) { res.status(500).json({ error: e.message }); }
  });
}).listen(process.env.FN_PORT || 3001, () => console.log(`[fn] invoice worker on http://localhost:${process.env.FN_PORT || 3001}/api/invoice`));
