const aliases = {
  ELASTICSEARCH: 'ES',
  POSTGRESQL: 'PGSQL',
  RDB: 'MYSQL',
  PULSARMQ: 'PULSAR',
  MQ: 'RABBITMQ'
}

export function normalizeTargetType(type) {
  const value = String(type || 'LOGGER').trim().toUpperCase()
  return aliases[value] || value || 'LOGGER'
}
