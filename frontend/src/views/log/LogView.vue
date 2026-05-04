<template>
  <div>
    <div class="page-head">
      <h1 class="page-title">日志中心</h1>
      <div class="toolbar">
        <a-select v-if="activeTab === 'task'" v-model:value="taskId" style="width:280px" :options="taskOptions" placeholder="选择任务" />
        <a-select v-else-if="activeTab === 'operation'" v-model:value="operationFilter.moduleName" style="width:140px" :options="moduleOptions" />
        <template v-else>
          <a-select v-model:value="alertFilter.acknowledged" style="width:140px" :options="alertStatusOptions" />
          <a-select v-model:value="alertFilter.moduleName" style="width:140px" :options="moduleOptions" />
        </template>
        <a-input v-if="activeTab === 'operation'" v-model:value="operationFilter.refId" style="width:220px" placeholder="对象 ID" />
        <a-input v-if="activeTab === 'alert'" v-model:value="alertFilter.refId" style="width:220px" placeholder="对象 ID" />
        <a-button type="primary" @click="load"><Search :size="15" />查询</a-button>
        <a-button @click="reset"><RotateCcw :size="15" />重置</a-button>
        <a-button v-if="activeTab === 'alert' && store.canOperate" @click="ackSelected" :disabled="!selectedAlertKeys.length">批量确认</a-button>
        <a-button v-if="activeTab === 'alert' && store.canOperate" @click="ackCurrentFilter">确认当前筛选</a-button>
      </div>
    </div>
    <div class="panel">
      <a-tabs v-model:activeKey="activeTab">
        <a-tab-pane key="task" tab="任务日志">
          <a-table :data-source="logs" :columns="columns" row-key="id" :pagination="tablePagination(10)">
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'logLevel'" :color="record.logLevel === 'ERROR' ? 'red' : 'blue'">{{ record.logLevel }}</a-tag>
            </template>
          </a-table>
        </a-tab-pane>
        <a-tab-pane key="operation" tab="操作日志">
          <a-table :data-source="operationLogs" :columns="operationColumns" row-key="id" :pagination="tablePagination(10)" />
        </a-tab-pane>
        <a-tab-pane key="alert" tab="告警日志">
          <div class="metric-grid compact-metrics">
            <div class="metric">
              <div class="metric-label">未确认</div>
              <div class="metric-value small-value">{{ alertStats.unacknowledged || 0 }}</div>
            </div>
            <div class="metric">
              <div class="metric-label">已确认</div>
              <div class="metric-value small-value">{{ alertStats.acknowledged || 0 }}</div>
            </div>
            <div class="metric">
              <div class="metric-label">总数</div>
              <div class="metric-value small-value">{{ alertStats.total || 0 }}</div>
            </div>
          </div>
          <a-table
            :data-source="alertLogs"
            :columns="alertColumns"
            row-key="id"
            :pagination="tablePagination(10)"
            :row-selection="store.canOperate ? { selectedRowKeys: selectedAlertKeys, onChange: keys => selectedAlertKeys = keys } : null"
          >
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'alertLevel'" color="red">{{ record.alertLevel }}</a-tag>
              <a-tag v-if="column.key === 'acknowledged'" :color="record.acknowledged === 1 ? 'green' : 'orange'">
                {{ record.acknowledged === 1 ? '已确认' : '未确认' }}
              </a-tag>
              <a-button v-if="column.key === 'actions' && record.acknowledged !== 1 && store.canOperate" size="small" @click="ackAlert(record.id)">确认</a-button>
            </template>
          </a-table>
        </a-tab-pane>
      </a-tabs>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import { message, Modal } from 'ant-design-vue'
import { RotateCcw, Search } from 'lucide-vue-next'
import { logApi } from '../../api'
import { useAppStore } from '../../stores/useAppStore'
import { tablePagination } from '../../utils/pagination'

const store = useAppStore()
const logLimit = 1000
const activeTab = ref('task')
const taskId = ref('')
const logs = ref([])
const operationLogs = ref([])
const alertLogs = ref([])
const alertStats = ref({})
const selectedAlertKeys = ref([])
const operationFilter = ref({ moduleName: '', refId: '' })
const alertFilter = ref({ acknowledged: 0, moduleName: '', refId: '' })
const taskOptions = computed(() => store.tasks.map(item => ({ label: item.taskName, value: item.id })))
const moduleOptions = [
  { label: '全部模块', value: '' },
  { label: '任务', value: 'task' },
  { label: '数据源', value: 'datasource' },
  { label: 'Canal Runtime', value: 'canal-runtime' },
  { label: '系统用户', value: 'system-user' }
]
const alertStatusOptions = [
  { label: '未确认', value: 0 },
  { label: '已确认', value: 1 },
  { label: '全部', value: '' }
]
const columns = [
  { title: '级别', key: 'logLevel', width: 100 },
  { title: '内容', dataIndex: 'logContent' },
  { title: '时间', dataIndex: 'createTime', width: 210 }
]
const operationColumns = [
  { title: '模块', dataIndex: 'moduleName', width: 120 },
  { title: '动作', dataIndex: 'actionName', width: 120 },
  { title: '对象', dataIndex: 'refId', width: 180 },
  { title: '内容', dataIndex: 'logContent' },
  { title: '时间', dataIndex: 'createTime', width: 210 }
]
const alertColumns = [
  { title: '级别', key: 'alertLevel', width: 90 },
  { title: '模块', dataIndex: 'moduleName', width: 100 },
  { title: '对象', dataIndex: 'refId', width: 160 },
  { title: '标题', dataIndex: 'alertTitle', width: 140 },
  { title: '内容', dataIndex: 'alertContent' },
  { title: '状态', key: 'acknowledged', width: 100 },
  { title: '时间', dataIndex: 'createTime', width: 210 },
  { title: '操作', key: 'actions', width: 90 }
]

async function load() {
  if (activeTab.value === 'task') {
    if (!taskId.value) return
    logs.value = await logApi.task(taskId.value, logLimit)
    return
  }
  if (activeTab.value === 'alert') {
    await loadAlerts()
    return
  }
  operationLogs.value = await logApi.operation({ ...operationFilter.value, limit: logLimit })
}

async function reset() {
  taskId.value = ''
  logs.value = []
  operationFilter.value = { moduleName: '', refId: '' }
  alertFilter.value = { acknowledged: 0, moduleName: '', refId: '' }
  operationLogs.value = await logApi.operation({ limit: logLimit })
  await loadAlerts()
}

function normalizeAlertFilter() {
  const params = {}
  if (alertFilter.value.acknowledged !== '') params.acknowledged = alertFilter.value.acknowledged
  if (alertFilter.value.moduleName) params.moduleName = alertFilter.value.moduleName
  if (alertFilter.value.refId) params.refId = alertFilter.value.refId
  return params
}

async function loadAlerts() {
  selectedAlertKeys.value = []
  const [logs, stats] = await Promise.all([
    logApi.alert({ ...normalizeAlertFilter(), limit: logLimit }),
    logApi.alertStats()
  ])
  alertLogs.value = logs
  alertStats.value = stats
}

async function ackAlert(id) {
  await logApi.ackAlert(id)
  await loadAlerts()
  await store.refreshDashboard()
}

async function ackSelected() {
  await logApi.ackAlertBatch(selectedAlertKeys.value)
  await loadAlerts()
  await store.refreshDashboard()
  message.success('选中告警已确认')
}

function ackCurrentFilter() {
  Modal.confirm({
    title: '确认当前筛选告警',
    content: '确认后当前筛选条件下的未确认告警都会标记为已确认。',
    async onOk() {
      const count = await logApi.ackAlertByFilter(normalizeAlertFilter())
      await loadAlerts()
      await store.refreshDashboard()
      message.success(`已确认 ${count || 0} 条告警`)
    }
  })
}

onMounted(() => {
  Promise.allSettled([
    logApi.operation({ limit: logLimit }).then(data => operationLogs.value = data),
    loadAlerts()
  ])
})
</script>
