require('dotenv').config({ quiet: true });

const env = (k, d) => (process.env[k] !== undefined && process.env[k] !== '' ? process.env[k] : d);

module.exports = {
  port: Number(env('PORT', 4000)),
  databaseUrl: env('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/tenantflow'),
  dbSsl: env('DB_SSL', 'false') === 'true',
  pool: {
    max: Number(env('PG_POOL_MAX', 10)),
    idleTimeoutMillis: Number(env('PG_POOL_IDLE_MS', 30000)),
    connectionTimeoutMillis: Number(env('PG_POOL_CONN_TIMEOUT_MS', 5000)),
  },
  rlsRole: env('RLS_ROLE', 'app_tenant'), // set RLS_ROLE=none to disable SET ROLE
  jwtSecret: env('JWT_SECRET', 'dev-only-change-me'),
  jwtExpiresIn: env('JWT_EXPIRES_IN', '8h'),
  rootDomain: env('ROOT_DOMAIN', ''), // e.g. tenantflow.app -> acme.tenantflow.app
  kafka: {
    brokers: env('KAFKA_BROKERS', 'localhost:19092').split(',').map((s) => s.trim()),
    clientId: env('KAFKA_CLIENT_ID', 'tenantflow-api'),
    topic: env('KAFKA_TOPIC', 'tenant.orders'),
    partitions: Number(env('KAFKA_PARTITIONS', 2)), // Aiven free tier = max 2 partitions
    groupId: env('KAFKA_GROUP_ID', 'inventory-service'),
    ssl: env('KAFKA_SSL', 'false') === 'true',
    caCert: env('KAFKA_CA_CERT', ''),          // PEM text or base64 of ca.pem
    clientCert: env('KAFKA_CLIENT_CERT', ''),  // PEM/base64 of service.cert (Aiven cert auth)
    clientKey: env('KAFKA_CLIENT_KEY', ''),    // PEM/base64 of service.key
    saslMechanism: env('KAFKA_SASL_MECHANISM', ''), // scram-sha-256 | scram-sha-512 | plain
    username: env('KAFKA_USERNAME', ''),
    password: env('KAFKA_PASSWORD', ''),
  },
  runConsumerInApi: env('RUN_CONSUMER_IN_API', 'false') === 'true',
  invoiceFunctionUrl: env('INVOICE_FUNCTION_URL', ''),
  invoiceSecret: env('INVOICE_SECRET', 'dev-invoice-secret'),
};
