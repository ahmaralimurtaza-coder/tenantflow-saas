const { Kafka, logLevel, Partitioners } = require('kafkajs');
const config = require('../config');

// Accept PEM text directly, PEM with literal "\n", or base64 of the PEM file
function pem(value) {
  if (!value) return undefined;
  if (value.includes('-----BEGIN')) return value.replace(/\\n/g, '\n');
  return Buffer.from(value, 'base64').toString('utf8');
}

function buildKafka(clientId = config.kafka.clientId) {
  const k = config.kafka;
  const opts = {
    clientId,
    brokers: k.brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 8 },
  };
  if (k.ssl) {
    opts.ssl = {
      rejectUnauthorized: true,
      ca: k.caCert ? [pem(k.caCert)] : undefined,
      cert: pem(k.clientCert),
      key: pem(k.clientKey),
    };
  }
  if (k.saslMechanism) {
    opts.sasl = { mechanism: k.saslMechanism, username: k.username, password: k.password };
  }
  return new Kafka(opts);
}

module.exports = { buildKafka, Partitioners };
