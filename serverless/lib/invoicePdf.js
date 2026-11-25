const PDFDocument = require('pdfkit');

const money = (c) => `$${(Number(c || 0) / 100).toFixed(2)}`;

function buildInvoicePdf(order, tenantSlug) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: 'A4', margin: 50 });
    const chunks = [];
    doc.on('data', (c) => chunks.push(c));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    doc.fillColor('#1e293b').fontSize(22).text('INVOICE', { align: 'right' });
    doc.fontSize(10).fillColor('#64748b')
      .text(`Tenant: ${tenantSlug || '-'}`, { align: 'right' })
      .text(`Invoice #: INV-${String(order.id).slice(0, 8).toUpperCase()}`, { align: 'right' })
      .text(`Date: ${new Date(order.createdAt || Date.now()).toLocaleString('en-GB')}`, { align: 'right' });
    doc.moveDown(2).fillColor('#0f172a').fontSize(12).text(`Bill to: ${order.customerName || '-'}`);
    doc.moveDown();

    const top = doc.y + 10;
    doc.fontSize(10).fillColor('#475569');
    doc.text('Item', 50, top).text('Qty', 330, top).text('Unit', 390, top).text('Amount', 470, top);
    doc.moveTo(50, top + 15).lineTo(545, top + 15).strokeColor('#cbd5e1').stroke();
    let y = top + 25;
    doc.fillColor('#0f172a');
    for (const it of order.items || []) {
      doc.text(it.name, 50, y, { width: 270 }).text(String(it.quantity), 330, y)
        .text(money(it.unitPriceCents), 390, y).text(money(it.unitPriceCents * it.quantity), 470, y);
      y += 20;
    }
    doc.moveTo(50, y + 5).lineTo(545, y + 5).stroke();
    doc.fontSize(12).text(`Total: ${money(order.totalCents)}`, 390, y + 15);
    doc.fontSize(8).fillColor('#94a3b8').text('Generated asynchronously by the TenantFlow serverless invoice worker.', 50, 760, { align: 'center' });
    doc.end();
  });
}

module.exports = { buildInvoicePdf };
