<template>
  <div>
    <a-divider orientation="left">目标端配置</a-divider>
    <div class="target-grid">
      <a-form-item v-for="item in fields" :key="item.key" :label="item.label">
        <a-input-number
          v-if="item.input === 'number'"
          :value="numberValue(item.key, item.defaultValue)"
          :min="item.min || 1"
          style="width:100%"
          @change="value => update(item.key, value)"
        />
        <a-select
          v-else-if="item.input === 'select'"
          :value="valueOf(item.key, item.defaultValue)"
          :options="item.options"
          @change="value => update(item.key, value)"
        />
        <a-switch
          v-else-if="item.input === 'switch'"
          :checked="valueOf(item.key, item.defaultValue) === 'true'"
          @change="checked => update(item.key, checked ? 'true' : 'false')"
        />
        <a-input-password
          v-else-if="item.input === 'password'"
          :value="valueOf(item.key, item.defaultValue)"
          @input="update(item.key, $event.target.value)"
        />
        <a-textarea
          v-else-if="item.input === 'textarea'"
          :value="valueOf(item.key, item.defaultValue)"
          :placeholder="item.placeholder"
          :rows="item.rows || 4"
          @input="update(item.key, $event.target.value)"
        />
        <a-input
          v-else
          :value="valueOf(item.key, item.defaultValue)"
          :placeholder="item.placeholder"
          @input="update(item.key, $event.target.value)"
        />
      </a-form-item>
    </div>
  </div>
</template>

<script setup>
import { computed, watch } from 'vue'
import { normalizeTargetType } from '../utils/targetType'

const props = defineProps({
  type: { type: String, default: 'LOGGER' },
  modelValue: { type: Object, required: true }
})
const emit = defineEmits(['update:modelValue'])

const commonJdbc = [
  { key: 'adapterKey', label: 'Adapter Key', defaultValue: 'target1' },
  { key: 'jdbcUrl', label: 'JDBC URL', defaultValue: 'jdbc:mysql://localhost:3306/target?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true' },
  { key: 'username', label: '用户名', defaultValue: 'root' },
  { key: 'password', label: '密码', input: 'password', defaultValue: '12345678' },
  { key: 'sourceTable', label: '源表名', placeholder: 'user' },
  { key: 'targetTable', label: '目标表名', placeholder: 'target_db.user' },
  { key: 'targetPk', label: '目标主键', defaultValue: 'id' },
  { key: 'commitBatch', label: '提交批量', input: 'number', defaultValue: 3000 },
  { key: 'threads', label: '并发线程', input: 'number', defaultValue: 1 }
]

const presets = {
  LOGGER: [
    { key: 'adapterKey', label: 'Adapter Key', defaultValue: 'logger' }
  ],
  KAFKA: [
    { key: 'bootstrapServers', label: 'Bootstrap Servers', defaultValue: '127.0.0.1:9092' },
    { key: 'topic', label: 'Topic', placeholder: 'canal.user_profile' },
    { key: 'messageKey', label: 'Message Key 模板', placeholder: '{id}' },
    { key: 'partitionKey', label: '分区字段', placeholder: 'id' },
    { key: 'messageFormat', label: '消息格式', input: 'select', defaultValue: 'JSON', options: [{ label: 'JSON', value: 'JSON' }, { label: 'CANAL_JSON', value: 'CANAL_JSON' }] },
    { key: 'consumerGroup', label: '消费组约定', placeholder: 'user-profile-sync-group' },
    { key: 'acks', label: 'Acks', defaultValue: 'all' },
    { key: 'retries', label: 'Retries', input: 'number', defaultValue: 3, min: 0 },
    { key: 'partitionHash', label: '分区 Hash', placeholder: 'db.table:id' },
    { key: 'dynamicTopic', label: '动态 Topic', placeholder: 'topic:db.table,.*\\..*' }
  ],
  ROCKETMQ: [
    { key: 'nameServer', label: 'NameServer', defaultValue: '127.0.0.1:9876' },
    { key: 'producerGroup', label: 'Producer Group', defaultValue: 'canal-web-producer' },
    { key: 'topic', label: 'Topic', placeholder: 'canal_user_profile' },
    { key: 'messageKey', label: 'Message Key 模板', placeholder: '{id}' },
    { key: 'partitionKey', label: '分区字段', placeholder: 'id' },
    { key: 'messageFormat', label: '消息格式', input: 'select', defaultValue: 'JSON', options: [{ label: 'JSON', value: 'JSON' }, { label: 'CANAL_JSON', value: 'CANAL_JSON' }] },
    { key: 'consumerGroup', label: '消费组约定', placeholder: 'user-profile-rocket-group' },
    { key: 'retries', label: 'Retries', input: 'number', defaultValue: 3, min: 0 },
    { key: 'tag', label: 'Tag', placeholder: 'user_profile' },
    { key: 'namesrvAddr', label: '兼容 NameServer' }
  ],
  PULSAR: [
    { key: 'serviceUrl', label: 'Service URL', defaultValue: 'pulsar://127.0.0.1:6650' },
    { key: 'topic', label: 'Topic', placeholder: 'persistent://public/default/user_profile' },
    { key: 'subscriptionName', label: 'Subscription', defaultValue: 'canal-web-subscription' },
    { key: 'messageKey', label: 'Message Key 模板', placeholder: '{id}' },
    { key: 'partitionKey', label: '分区字段', placeholder: 'id' },
    { key: 'messageFormat', label: '消息格式', input: 'select', defaultValue: 'JSON', options: [{ label: 'JSON', value: 'JSON' }, { label: 'CANAL_JSON', value: 'CANAL_JSON' }] },
    { key: 'consumerGroup', label: '消费组约定', placeholder: 'user-profile-pulsar-group' },
    { key: 'retries', label: 'Retries', input: 'number', defaultValue: 3, min: 0 },
    { key: 'serverUrl', label: '兼容 Server URL' }
  ],
  RABBITMQ: [
    { key: 'host', label: 'Host', defaultValue: '127.0.0.1' },
    { key: 'port', label: 'Port', input: 'number', defaultValue: 5672 },
    { key: 'username', label: '用户名', defaultValue: 'guest' },
    { key: 'password', label: '密码', input: 'password', defaultValue: 'guest' },
    { key: 'virtualHost', label: 'Virtual Host', defaultValue: '/' },
    { key: 'exchange', label: 'Exchange', placeholder: 'canal.exchange' },
    { key: 'routingKey', label: 'Routing Key 模板', placeholder: '{database}.{table}.{id}' },
    { key: 'topic', label: 'Topic/Queue 约定', placeholder: 'canal.user_profile' },
    { key: 'messageKey', label: 'Message Key 模板', placeholder: '{id}' },
    { key: 'partitionKey', label: '分区字段', placeholder: 'id' },
    { key: 'messageFormat', label: '消息格式', input: 'select', defaultValue: 'JSON', options: [{ label: 'JSON', value: 'JSON' }, { label: 'CANAL_JSON', value: 'CANAL_JSON' }] },
    { key: 'consumerGroup', label: '消费组约定', placeholder: 'user-profile-rabbit-group' }
  ],
  REDIS: [
    { key: 'redisMode', label: '模式', input: 'select', defaultValue: 'standalone', options: [{ label: 'standalone', value: 'standalone' }, { label: 'cluster', value: 'cluster' }] },
    { key: 'host', label: 'Host', defaultValue: 'localhost' },
    { key: 'port', label: 'Port', input: 'number', defaultValue: 6379 },
    { key: 'password', label: '密码', input: 'password' },
    { key: 'database', label: 'DB', input: 'number', defaultValue: 6, min: 0 },
    { key: 'sourceTable', label: '源表名' },
    { key: 'keyPattern', label: 'Key 模板', defaultValue: 'record:{id}', placeholder: 'user:{id}' },
    { key: 'valueType', label: '数据结构', input: 'select', defaultValue: 'HASH', options: [{ label: 'HASH', value: 'HASH' }, { label: 'JSON', value: 'JSON' }, { label: 'STRING', value: 'STRING' }] },
    { key: 'valueMapping', label: 'Value 字段', placeholder: 'id,name,status,updated_at' },
    { key: 'ttlSeconds', label: 'TTL 秒数', input: 'number', defaultValue: 0, min: 0 },
    { key: 'deletePolicy', label: '删除策略', input: 'select', defaultValue: 'DELETE_KEY', options: [{ label: '删除 Key', value: 'DELETE_KEY' }, { label: '标记删除', value: 'MARK_DELETED' }, { label: '忽略', value: 'IGNORE' }] },
    { key: 'keyPrefix', label: '兼容 Key 前缀' },
    { key: 'keyField', label: '兼容 Key 字段', defaultValue: 'id' },
    { key: 'expireSeconds', label: '兼容过期秒数', input: 'number', defaultValue: 0, min: 0 }
  ],
  MYSQL: [
    { key: 'driverClassName', label: '驱动类', defaultValue: 'com.mysql.jdbc.Driver' },
    { key: 'targetDatabase', label: '目标库名', placeholder: 'report_center' },
    { key: 'targetTableName', label: '目标表名', placeholder: 'user_profile' },
    { key: 'primaryKey', label: '主键/唯一键', defaultValue: 'id' },
    { key: 'writeMode', label: '写入模式', input: 'select', defaultValue: 'UPSERT', options: [{ label: 'UPSERT', value: 'UPSERT' }, { label: 'INSERT', value: 'INSERT' }, { label: 'UPDATE', value: 'UPDATE' }] },
    { key: 'createTableSql', label: '建库建表示例', input: 'textarea', rows: 6, placeholder: 'CREATE DATABASE ...;\\nCREATE TABLE ...;' },
    ...commonJdbc
  ],
  PGSQL: [
    { key: 'driverClassName', label: '驱动类', defaultValue: 'org.postgresql.Driver' },
    { key: 'adapterKey', label: 'Adapter Key', defaultValue: 'pgsql1' },
    { key: 'jdbcUrl', label: 'JDBC URL', defaultValue: 'jdbc:postgresql://localhost:5432/postgres' },
    { key: 'username', label: '用户名', defaultValue: 'postgres' },
    { key: 'password', label: '密码', input: 'password', defaultValue: '123456' },
    { key: 'targetSchema', label: '目标 Schema', defaultValue: 'public' },
    { key: 'sourceTable', label: '源表名' },
    { key: 'targetTable', label: '目标表名' },
    { key: 'targetPk', label: '目标主键', defaultValue: 'id' },
    { key: 'writeMode', label: '写入模式', input: 'select', defaultValue: 'UPSERT', options: [{ label: 'UPSERT', value: 'UPSERT' }, { label: 'INSERT', value: 'INSERT' }, { label: 'UPDATE', value: 'UPDATE' }] },
    { key: 'commitBatch', label: '提交批量', input: 'number', defaultValue: 3000 },
    { key: 'threads', label: '并发线程', input: 'number', defaultValue: 1 }
  ],
  CLICKHOUSE: [
    { key: 'adapterKey', label: 'Adapter Key', defaultValue: 'clickhouse1' },
    { key: 'jdbcUrl', label: 'JDBC URL', defaultValue: 'jdbc:clickhouse://127.0.0.1:8123/default' },
    { key: 'username', label: '用户名', defaultValue: 'default' },
    { key: 'password', label: '密码', input: 'password' },
    { key: 'sourceTable', label: '源表名' },
    { key: 'targetTable', label: '目标表名' },
    { key: 'targetPk', label: '目标主键', defaultValue: 'id' },
    { key: 'batchSize', label: '批量大小', input: 'number', defaultValue: 3000 },
    { key: 'scheduleTime', label: '调度秒数', input: 'number', defaultValue: 600 },
    { key: 'threads', label: '并发线程', input: 'number', defaultValue: 3 }
  ],
  ES: [
    { key: 'adapterKey', label: 'Adapter Key', defaultValue: 'es1' },
    { key: 'hosts', label: 'Hosts', defaultValue: '127.0.0.1:9200' },
    { key: 'mode', label: '连接模式', input: 'select', defaultValue: 'rest', options: [{ label: 'rest', value: 'rest' }, { label: 'transport', value: 'transport' }] },
    { key: 'clusterName', label: 'Cluster Name', defaultValue: 'elasticsearch' },
    { key: 'securityAuth', label: '认证', placeholder: 'user:password' },
    { key: 'indexName', label: '索引名', placeholder: 'user_profile_v1' },
    { key: 'documentId', label: 'Document ID 模板', defaultValue: '{id}' },
    { key: 'mapping', label: 'Mapping JSON', input: 'textarea', rows: 8, placeholder: '{ "properties": { "id": { "type": "long" } } }' },
    { key: 'writeMode', label: '写入模式', input: 'select', defaultValue: 'UPSERT', options: [{ label: 'INDEX', value: 'INDEX' }, { label: 'UPDATE', value: 'UPDATE' }, { label: 'UPSERT', value: 'UPSERT' }] },
    { key: 'deletePolicy', label: '删除策略', input: 'select', defaultValue: 'DELETE_DOCUMENT', options: [{ label: '删除文档', value: 'DELETE_DOCUMENT' }, { label: '标记删除', value: 'MARK_DELETED' }, { label: '忽略', value: 'IGNORE' }] },
    { key: 'index', label: '兼容索引名' },
    { key: 'idField', label: '兼容文档 ID 字段', defaultValue: 'id' },
    { key: 'upsert', label: '兼容 Upsert', input: 'switch', defaultValue: 'true' },
    { key: 'commitBatch', label: '提交批量', input: 'number', defaultValue: 3000 }
  ],
  HBASE: [
    { key: 'adapterKey', label: 'Adapter Key', defaultValue: 'hbase1' },
    { key: 'zkQuorum', label: 'ZK Quorum', defaultValue: '127.0.0.1' },
    { key: 'zkClientPort', label: 'ZK 端口', input: 'number', defaultValue: 2181 },
    { key: 'zkParent', label: 'ZNode Parent', defaultValue: '/hbase' },
    { key: 'mode', label: '写入模式', input: 'select', defaultValue: 'STRING', options: [{ label: 'STRING', value: 'STRING' }, { label: 'NATIVE', value: 'NATIVE' }, { label: 'PHOENIX', value: 'PHOENIX' }] },
    { key: 'sourceTable', label: '源表名' },
    { key: 'hbaseTable', label: 'HBase 表名', placeholder: 'namespace:user_profile' },
    { key: 'family', label: 'Column Family', defaultValue: 'CF' },
    { key: 'rowKey', label: 'RowKey 字段', defaultValue: 'id', placeholder: 'id 或 id,type' },
    { key: 'uppercaseQualifier', label: 'Qualifier 大写', input: 'switch', defaultValue: 'true' },
    { key: 'etlCondition', label: 'ETL 条件', placeholder: 'where updated_at >= {}' },
    { key: 'commitBatch', label: '提交批量', input: 'number', defaultValue: 3000 },
    { key: 'createTableShell', label: '建表示例', input: 'textarea', rows: 5, placeholder: "create_namespace 'canal'\\ncreate 'canal:user_profile', 'CF'" }
  ],
  TABLESTORE: [
    { key: 'adapterKey', label: 'Adapter Key', defaultValue: 'tablestore1' },
    { key: 'endpoint', label: 'Endpoint', placeholder: 'https://instance.cn-hangzhou.ots.aliyuncs.com' },
    { key: 'instanceName', label: 'Instance Name' },
    { key: 'accessKeyId', label: 'AccessKey ID' },
    { key: 'accessKeySecret', label: 'AccessKey Secret', input: 'password' },
    { key: 'sourceTable', label: '源表名' },
    { key: 'tableName', label: 'TableStore 表名' },
    { key: 'primaryKey', label: '主键字段', defaultValue: 'id' },
    { key: 'writeMode', label: '写入模式', input: 'select', defaultValue: 'UPSERT', options: [{ label: 'UPSERT', value: 'UPSERT' }, { label: 'PUT', value: 'PUT' }, { label: 'UPDATE', value: 'UPDATE' }] },
    { key: 'deletePolicy', label: '删除策略', input: 'select', defaultValue: 'DELETE_ROW', options: [{ label: '删除行', value: 'DELETE_ROW' }, { label: '标记删除', value: 'MARK_DELETED' }, { label: '忽略', value: 'IGNORE' }] },
    { key: 'commitBatch', label: '提交批量', input: 'number', defaultValue: 200 }
  ]
}

const normalizedType = computed(() => normalizeTargetType(props.type))
const fields = computed(() => presets[normalizedType.value] || presets.LOGGER)

watch(() => props.type, () => {
  const next = sanitizeConfig(props.modelValue)
  fields.value.forEach(item => {
    if ((next[item.key] === undefined || next[item.key] === '') && item.defaultValue !== undefined) {
      next[item.key] = String(item.defaultValue)
    }
  })
  emit('update:modelValue', next)
}, { immediate: true })

function valueOf(key, defaultValue = '') {
  const value = props.modelValue[key]
  return value === undefined || value === null || value === '' ? String(defaultValue || '') : String(value)
}

function numberValue(key, defaultValue = 1) {
  const value = Number(props.modelValue[key] || defaultValue)
  return Number.isFinite(value) ? value : defaultValue
}

function update(key, value) {
  emit('update:modelValue', { ...sanitizeConfig(props.modelValue), [key]: value == null ? '' : String(value) })
}

function sanitizeConfig(config) {
  const allowedKeys = new Set(fields.value.map(item => item.key))
  return Object.fromEntries(Object.entries(config || {}).filter(([key]) => allowedKeys.has(key)))
}
</script>
