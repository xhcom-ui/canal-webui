<template>
  <div>
    <div class="page-head">
      <h1 class="page-title">监控大盘</h1>
      <div class="toolbar">
        <a-button @click="refresh"><RefreshCw :size="15" />刷新</a-button>
      </div>
    </div>

    <div class="metric-grid">
      <div class="metric" v-for="item in metrics" :key="item.label">
        <div class="metric-label">{{ item.label }}</div>
        <div class="metric-value">{{ item.value ?? 0 }}</div>
      </div>
    </div>

    <div class="dashboard-grid">
      <div class="panel">
        <div class="section-head">
          <h2>未确认告警</h2>
          <a-tag color="red">{{ unacknowledgedAlerts.length }}</a-tag>
        </div>
        <a-table :data-source="unacknowledgedAlerts" :columns="alertColumns" row-key="id" :pagination="false" size="small">
          <template #bodyCell="{ column, record }">
            <a-tag v-if="column.key === 'alertLevel'" color="red">{{ record.alertLevel }}</a-tag>
            <a-button v-if="column.key === 'actions' && store.canOperate" size="small" @click="ackAlert(record.id)">确认</a-button>
          </template>
        </a-table>
      </div>

      <div class="panel">
        <div class="section-head">
          <h2>异常任务</h2>
          <a-tag :color="errorTasks.length ? 'red' : 'green'">{{ errorTasks.length }}</a-tag>
        </div>
        <a-table :data-source="errorTasks" :columns="errorTaskColumns" row-key="id" :pagination="false" size="small">
          <template #bodyCell="{ column, record }">
            <a-tag v-if="column.key === 'taskStatus'" color="red">{{ record.taskStatus }}</a-tag>
          </template>
        </a-table>
      </div>
    </div>

    <div class="panel">
      <div class="section-head">
        <h2>任务运行概览</h2>
      </div>
      <a-table :data-source="store.tasks" :columns="columns" row-key="id" :pagination="tablePagination(10)">
        <template #bodyCell="{ column, record }">
          <a-tag v-if="column.key === 'taskStatus'" :color="statusColor(record.taskStatus)">
            {{ record.taskStatus }}
          </a-tag>
        </template>
      </a-table>
    </div>

    <div class="panel">
      <div class="section-head">
        <h2>最近操作</h2>
      </div>
      <a-table :data-source="recentOperations" :columns="operationColumns" row-key="id" :pagination="tablePagination(10)" size="small" />
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted } from 'vue'
import { message } from 'ant-design-vue'
import { RefreshCw } from 'lucide-vue-next'
import { logApi } from '../../api'
import { useAppStore } from '../../stores/useAppStore'
import { tablePagination } from '../../utils/pagination'

const store = useAppStore()
const metrics = computed(() => [
  { label: '数据源', value: store.stats.datasourceCount },
  { label: '同步任务', value: store.stats.taskCount },
  { label: '运行中', value: store.stats.runningCount },
  { label: '异常任务', value: store.stats.errorCount },
  { label: '日志条数', value: store.stats.logCount },
  { label: '同步总数', value: store.stats.totalSyncCount },
  { label: '失败总数', value: store.stats.failCount },
  { label: '平均延迟(ms)', value: store.stats.avgDelayMs },
  { label: '未确认告警', value: store.stats.unacknowledgedAlertCount }
])
const unacknowledgedAlerts = computed(() => store.dashboardOverview.unacknowledgedAlerts || [])
const errorTasks = computed(() => store.dashboardOverview.errorTasks || [])
const recentOperations = computed(() => store.dashboardOverview.recentOperations || [])

const columns = [
  { title: '任务名称', dataIndex: 'taskName' },
  { title: '数据源', dataIndex: 'dataSourceKey' },
  { title: '目标端', dataIndex: 'targetType' },
  { title: '模式', dataIndex: 'syncMode' },
  { title: '同步数', dataIndex: 'totalCount' },
  { title: '失败数', dataIndex: 'failCount' },
  { title: '延迟(ms)', dataIndex: 'lastDelayMs' },
  { title: '状态', dataIndex: 'taskStatus', key: 'taskStatus' }
]
const alertColumns = [
  { title: '级别', key: 'alertLevel', width: 80 },
  { title: '模块', dataIndex: 'moduleName', width: 90 },
  { title: '对象', dataIndex: 'refId', width: 130 },
  { title: '标题', dataIndex: 'alertTitle', width: 140 },
  { title: '内容', dataIndex: 'alertContent' },
  { title: '操作', key: 'actions', width: 80 }
]
const errorTaskColumns = [
  { title: '任务名称', dataIndex: 'taskName' },
  { title: '数据源', dataIndex: 'dataSourceKey', width: 140 },
  { title: '失败数', dataIndex: 'failCount', width: 90 },
  { title: '状态', key: 'taskStatus', width: 90 }
]
const operationColumns = [
  { title: '模块', dataIndex: 'moduleName', width: 120 },
  { title: '动作', dataIndex: 'actionName', width: 120 },
  { title: '对象', dataIndex: 'refId', width: 180 },
  { title: '内容', dataIndex: 'logContent' },
  { title: '时间', dataIndex: 'createTime', width: 210 }
]

async function refresh() {
  await Promise.all([store.refreshDashboard(), store.refreshTasks()])
}

async function ackAlert(id) {
  await logApi.ackAlert(id)
  await refresh()
  message.success('告警已确认')
}

function statusColor(status) {
  return status === 'RUNNING' ? 'green' : status === 'ERROR' ? 'red' : 'default'
}

onMounted(() => refresh().catch(() => {}))
</script>
