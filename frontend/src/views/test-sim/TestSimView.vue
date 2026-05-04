<template>
  <div>
    <div class="page-head">
      <h1 class="page-title">测试模拟</h1>
      <div class="toolbar">
        <a-button :loading="statusLoading" @click="loadLocalStatus">
          <RefreshCw :size="15" />刷新状态
        </a-button>
        <a-button type="primary" :loading="runAllLoading" @click="runAll">
          <PlugZap :size="15" />全部跑通
        </a-button>
      </div>
    </div>

    <div class="panel">
      <a-alert
        :type="localStatus.ok ? 'success' : 'warning'"
        :message="`本机服务 ${localStatus.runningCount || 0}/${localStatus.total || 0} 可用`"
        show-icon
      />
      <a-table
        class="local-stack-table"
        :data-source="localStatus.services || []"
        :columns="statusColumns"
        row-key="key"
        :pagination="tablePagination(10)"
        size="small"
      >
        <template #bodyCell="{ column, record }">
          <a-tag v-if="column.key === 'status'" :color="record.ok ? 'green' : 'red'">
            {{ record.status }}
          </a-tag>
        </template>
      </a-table>
    </div>

    <div class="test-sim-grid">
      <div v-for="item in testItems" :key="item.key" class="panel test-sim-card">
        <div class="test-sim-card-head">
          <div>
            <h2>{{ item.name }}</h2>
            <p>{{ item.summary }}</p>
          </div>
          <a-tag :color="resultColor(results[item.key])">{{ resultText(results[item.key]) }}</a-tag>
        </div>

        <div class="test-sim-config-list">
          <div v-for="row in item.rows" :key="row.label" class="test-sim-config-row">
            <div class="test-sim-config-key">{{ row.label }}</div>
            <a-tooltip :title="row.value">
              <div class="test-sim-config-value">{{ row.value }}</div>
            </a-tooltip>
          </div>
        </div>

        <div class="toolbar test-sim-actions">
          <a-button type="primary" :loading="loadingKey === item.key" @click="runTarget(item)">
            <PlugZap :size="15" />跑通
          </a-button>
          <a-button @click="copyPayload(item)">
            <Copy :size="15" />复制配置
          </a-button>
        </div>

        <pre v-if="results[item.key]" class="code-block test-sim-output">{{ formatResult(results[item.key]) }}</pre>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import { message } from 'ant-design-vue'
import { Copy, PlugZap, RefreshCw } from 'lucide-vue-next'
import { systemApi, taskApi } from '../../api'
import { tablePagination } from '../../utils/pagination'
import { normalizeTargetType } from '../../utils/targetType'

const localStatus = ref({})
const results = ref({})
const loadingKey = ref('')
const statusLoading = ref(false)
const runAllLoading = ref(false)

const statusColumns = [
  { title: '组件', dataIndex: 'name', width: 150 },
  { title: '地址', dataIndex: 'target', width: 240 },
  { title: '状态', key: 'status', width: 90 },
  { title: '说明', dataIndex: 'message' }
]

const testItems = computed(() => [
  {
    key: 'mysql',
    name: 'MySQL',
    summary: 'localhost:3306 / root',
    targetType: 'MYSQL',
    config: {
      driverClassName: 'com.mysql.cj.jdbc.Driver',
      jdbcUrl: 'jdbc:mysql://localhost:3306/mysql?connectTimeout=5000&socketTimeout=10000&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true',
      username: 'root',
      password: '12345678'
    }
  },
  {
    key: 'redis',
    name: 'Redis',
    summary: 'localhost:6379 / database 6',
    targetType: 'REDIS',
    config: {
      host: 'localhost',
      port: '6379',
      database: '6',
      password: '',
      keyPattern: 'canal:test:{id}',
      valueType: 'HASH',
      ttlSeconds: '3600',
      deleteStrategy: 'DEL'
    }
  },
  {
    key: 'elasticsearch',
    name: 'Elasticsearch',
    summary: 'http://127.0.0.1:9200',
    targetType: 'ES',
    config: {
      hosts: 'http://127.0.0.1:9200',
      index: 'canal_web_verify',
      documentId: '{id}',
      writeMode: 'upsert'
    }
  },
  {
    key: 'pgsql',
    name: 'PostgreSQL',
    summary: 'localhost:5432 / postgres',
    targetType: 'PGSQL',
    config: {
      driverClassName: 'org.postgresql.Driver',
      jdbcUrl: 'jdbc:postgresql://localhost:5432/postgres',
      username: 'postgres',
      password: '123456'
    }
  },
  {
    key: 'kafka',
    name: 'Kafka',
    summary: '127.0.0.1:9092 / canal_web_verify',
    targetType: 'KAFKA',
    config: {
      bootstrapServers: '127.0.0.1:9092',
      topic: 'canal_web_verify',
      messageKey: '{id}',
      messageFormat: 'json',
      consumerGroup: 'canal-web-verify-group'
    }
  },
  {
    key: 'rocketmq',
    name: 'RocketMQ',
    summary: '127.0.0.1:9876 / canal_web_verify',
    targetType: 'ROCKETMQ',
    config: {
      nameServer: '127.0.0.1:9876',
      topic: 'canal_web_verify',
      producerGroup: 'canal-web-producer'
    }
  },
  {
    key: 'rabbitmq',
    name: 'RabbitMQ',
    summary: '127.0.0.1:5672 / canal_web_verify',
    targetType: 'RABBITMQ',
    config: {
      host: '127.0.0.1',
      port: '5672',
      username: 'guest',
      password: 'guest',
      topic: 'canal_web_verify',
      exchange: 'canal.web.verify',
      routingKey: 'canal.verify'
    }
  },
  {
    key: 'pulsar',
    name: 'Pulsar',
    summary: 'pulsar://127.0.0.1:6650',
    targetType: 'PULSAR',
    config: {
      serviceUrl: 'pulsar://127.0.0.1:6650',
      topic: 'persistent://public/default/canal_web_verify',
      subscriptionName: 'canal-web-verify-sub',
      messageKey: '{id}'
    }
  }
].map(item => ({ ...item, rows: configRows(item.config) })))

function configRows(config) {
  return Object.entries(config).map(([label, value]) => ({
    label,
    value: value === '' ? '空' : String(value)
  }))
}

async function loadLocalStatus() {
  statusLoading.value = true
  try {
    localStatus.value = await systemApi.localStackStatus()
  } catch (error) {
    localStatus.value = { ok: false, runningCount: 0, total: 0, services: [] }
  } finally {
    statusLoading.value = false
  }
}

async function runTarget(item) {
  loadingKey.value = item.key
  try {
    const result = await taskApi.targetTest({
      targetType: normalizeTargetType(item.targetType),
      targetConfig: item.config
    })
    results.value = { ...results.value, [item.key]: { ok: true, data: result } }
    message.success(`${item.name} 跑通`)
  } catch (error) {
    results.value = { ...results.value, [item.key]: { ok: false, message: error?.message || '连接失败' } }
    message.error(`${item.name} 未跑通`)
  } finally {
    loadingKey.value = ''
  }
}

async function runAll() {
  runAllLoading.value = true
  try {
    for (const item of testItems.value) {
      await runTarget(item)
    }
    await loadLocalStatus()
  } finally {
    runAllLoading.value = false
  }
}

async function copyPayload(item) {
  await navigator.clipboard.writeText(JSON.stringify({
    targetType: normalizeTargetType(item.targetType),
    targetConfig: item.config
  }, null, 2))
  message.success('配置已复制')
}

function resultColor(result) {
  if (!result) return 'default'
  return result.ok ? 'green' : 'red'
}

function resultText(result) {
  if (!result) return '未测试'
  return result.ok ? '已跑通' : '失败'
}

function formatResult(result) {
  return JSON.stringify(result, null, 2)
}

onMounted(() => loadLocalStatus().catch(() => {}))
</script>
