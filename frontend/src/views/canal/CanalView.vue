<template>
  <div>
    <div class="page-head">
      <h1 class="page-title">Canal 运维</h1>
      <div class="toolbar">
        <a-button @click="loadAll">刷新</a-button>
        <a-button v-if="store.canOperate" type="primary" @click="refreshCanal">刷新配置并重启</a-button>
        <a-button v-if="store.canOperate" @click="startCanal">启动</a-button>
        <a-button v-if="store.canOperate" danger @click="stopCanal">停止</a-button>
      </div>
    </div>

    <div class="metric-grid">
      <div class="metric">
        <div class="metric-heading">
          <div class="metric-label">Canal Server</div>
          <a-button v-if="store.isAdmin" size="small" class="icon-button" title="编辑路径" @click="openPathEditor">
            <Pencil :size="14" />
          </a-button>
        </div>
        <div class="metric-value status-value">
          <a-tag :color="canalStatus?.canalServer?.running ? 'green' : 'red'">
            {{ canalStatus?.canalServer?.running ? '运行中' : '已停止' }}
          </a-tag>
        </div>
        <div class="muted-path">{{ canalStatus?.canalServer?.home }}</div>
        <div v-if="store.canOperate" class="toolbar component-actions">
          <a-button size="small" @click="componentAction('server', 'start')">启动</a-button>
          <a-button size="small" @click="componentAction('server', 'restart')">重启</a-button>
          <a-button size="small" danger @click="componentAction('server', 'stop')">停止</a-button>
        </div>
      </div>
      <div class="metric">
        <div class="metric-heading">
          <div class="metric-label">Canal Adapter</div>
          <a-button v-if="store.isAdmin" size="small" class="icon-button" title="编辑路径" @click="openPathEditor">
            <Pencil :size="14" />
          </a-button>
        </div>
        <div class="metric-value status-value">
          <a-tag :color="canalStatus?.canalAdapter?.running ? 'green' : 'red'">
            {{ canalStatus?.canalAdapter?.running ? '运行中' : '已停止' }}
          </a-tag>
        </div>
        <div class="muted-path">{{ canalStatus?.canalAdapter?.home }}</div>
        <div v-if="store.canOperate" class="toolbar component-actions">
          <a-button size="small" @click="componentAction('adapter', 'start')">启动</a-button>
          <a-button size="small" @click="componentAction('adapter', 'restart')">重启</a-button>
          <a-button size="small" danger @click="componentAction('adapter', 'stop')">停止</a-button>
        </div>
      </div>
      <div class="metric">
        <div class="metric-heading">
          <div class="metric-label">Runtime 根目录</div>
          <a-button v-if="store.isAdmin" size="small" class="icon-button" title="编辑路径" @click="openPathEditor">
            <Pencil :size="14" />
          </a-button>
        </div>
        <div class="metric-value small-value">{{ runtimeSummary.instances }}</div>
        <div class="muted-path">{{ canalStatus?.rootDir }}</div>
      </div>
      <div class="metric">
        <div class="metric-label">能力覆盖</div>
        <div class="metric-value small-value">{{ runtimeSummary.connected }}/{{ capabilities.length }}</div>
        <div class="muted-path">已接入与部分接入能力</div>
      </div>
    </div>

    <div class="panel">
      <a-tabs v-model:activeKey="activeTab">
        <a-tab-pane key="runtime" tab="运行状态">
          <a-descriptions bordered :column="1">
            <a-descriptions-item label="Server PID">{{ canalStatus?.canalServer?.pidFile }}</a-descriptions-item>
            <a-descriptions-item label="Adapter PID">{{ canalStatus?.canalAdapter?.pidFile }}</a-descriptions-item>
          </a-descriptions>
        </a-tab-pane>

        <a-tab-pane key="diagnostics" tab="运行自检">
          <a-alert
            :type="diagnostics.ok ? 'success' : 'warning'"
            :message="`通过 ${diagnostics.okCount || 0}/${diagnostics.total || 0} 项检查`"
            show-icon
          />
          <a-table :data-source="diagnostics.checks || []" :columns="diagnosticColumns" row-key="name" :pagination="tablePagination(10)">
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'ok'" :color="record.ok ? 'green' : 'red'">{{ record.ok ? '正常' : '异常' }}</a-tag>
            </template>
          </a-table>
        </a-tab-pane>

        <a-tab-pane key="consistency" tab="配置一致性">
          <a-alert
            :type="configConsistency.ok ? 'success' : 'warning'"
            :message="`一致 ${configConsistency.okCount || 0}/${configConsistency.total || 0} 个文件`"
            show-icon
          />
          <a-table :data-source="configConsistency.items || []" :columns="consistencyColumns" row-key="name" :pagination="tablePagination(10)">
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'ok'" :color="record.ok ? 'green' : 'orange'">{{ record.ok ? '一致' : '不一致' }}</a-tag>
              <span v-if="column.key === 'size'">{{ record.actualSize }} / {{ record.expectedSize }}</span>
            </template>
            <template #expandedRowRender="{ record }">
              <a-tabs>
                <a-tab-pane key="expected" tab="期望配置">
                  <pre class="code-block">{{ record.expectedPreview || '暂无配置' }}</pre>
                </a-tab-pane>
                <a-tab-pane key="actual" tab="实际配置">
                  <pre class="code-block">{{ record.actualPreview || '暂无配置' }}</pre>
                </a-tab-pane>
              </a-tabs>
            </template>
          </a-table>
        </a-tab-pane>

        <a-tab-pane key="stale" tab="失效配置">
          <a-alert
            :type="staleFiles.count ? 'warning' : 'success'"
            :message="`发现 ${staleFiles.count || 0} 个失效运行配置`"
            show-icon
          />
          <div class="toolbar stale-actions" v-if="store.canOperate">
            <a-button danger :disabled="!staleFiles.count" @click="cleanStaleFiles">清理失效配置</a-button>
          </div>
          <a-table :data-source="staleFiles.items || []" :columns="staleColumns" row-key="path" :pagination="tablePagination(10)" />
        </a-tab-pane>

        <a-tab-pane key="setting" tab="页面化配置">
          <a-alert v-if="!store.isAdmin" type="info" message="当前角色可查看配置，修改 Canal 全局参数需要管理员权限" show-icon />
          <a-form class="canal-setting-form" layout="vertical" :model="settingForm" :disabled="!store.isAdmin">
            <a-divider orientation="left">运行模式</a-divider>
            <div class="setting-grid setting-grid-runtime">
              <a-form-item label="ServerMode">
                <a-select v-model:value="settingForm.serverMode" :options="serverModeOptions" />
              </a-form-item>
              <a-form-item label="AccessChannel">
                <a-select v-model:value="settingForm.accessChannel" :options="accessChannelOptions" />
              </a-form-item>
              <a-form-item label="FlatMessage">
                <a-switch :checked="settingForm.flatMessage === 1" @change="checked => settingForm.flatMessage = checked ? 1 : 0" />
              </a-form-item>
              <a-form-item label="BatchSize">
                <a-input-number v-model:value="settingForm.canalBatchSize" :min="1" style="width:100%" />
              </a-form-item>
              <a-form-item label="GetTimeout(ms)">
                <a-input-number v-model:value="settingForm.canalGetTimeout" :min="1" style="width:100%" />
              </a-form-item>
              <a-form-item label="Zookeeper 地址">
                <a-input v-model:value="settingForm.zkServers" placeholder="127.0.0.1:2181" />
              </a-form-item>
            </div>

            <a-divider orientation="left">路径配置</a-divider>
            <div class="setting-grid setting-grid-paths">
              <a-form-item label="Runtime 根目录" class="span-2">
                <a-input v-model:value="settingForm.runtimeRootDir" placeholder="./canal-runtime（留空读取项目默认）" />
              </a-form-item>
              <a-form-item label="Canal Server 目录">
                <a-input v-model:value="settingForm.canalServerHome" placeholder="./canal-runtime/canal-server（留空自动推导）" />
              </a-form-item>
              <a-form-item label="Canal Adapter 目录">
                <a-input v-model:value="settingForm.canalAdapterHome" placeholder="./canal-runtime/canal-adapter（留空自动推导）" />
              </a-form-item>
              <a-form-item label="dbsync 源码 Jar">
                <a-input v-model:value="settingForm.dbsyncSourceJar" placeholder="/path/to/canal.parse.dbsync-1.1.9-SNAPSHOT.jar" />
              </a-form-item>
              <a-form-item label="dbsync Runtime Jar">
                <a-input v-model:value="settingForm.dbsyncRuntimeJar" placeholder="/path/to/canal-server/lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar" />
              </a-form-item>
              <a-form-item label="Generated 目录" class="span-2">
                <a-textarea v-model:value="settingForm.generatedPaths" :rows="3" placeholder="多个目录用逗号分隔，例如 /path/canal-runtime/generated" />
              </a-form-item>
            </div>

            <a-divider orientation="left">MQ 投递</a-divider>
            <div class="setting-grid setting-grid-mq">
              <a-form-item label="MQ 地址" class="span-2">
                <a-input v-model:value="settingForm.mqServers" placeholder="Kafka/RocketMQ/RabbitMQ/Pulsar 地址" />
              </a-form-item>
              <a-form-item label="MQ 用户名">
                <a-input v-model:value="settingForm.mqUsername" />
              </a-form-item>
              <a-form-item label="MQ 密码">
                <a-input-password v-model:value="settingForm.mqPassword" />
              </a-form-item>
              <a-form-item label="默认 Topic">
                <a-input v-model:value="settingForm.mqTopic" />
              </a-form-item>
              <a-form-item label="分区 Hash 字段">
                <a-input v-model:value="settingForm.mqPartitionHash" placeholder="schema.table:pk" />
              </a-form-item>
              <a-form-item label="动态 Topic 规则">
                <a-input v-model:value="settingForm.mqDynamicTopic" placeholder="topic:schema.table,.*\\..*" />
              </a-form-item>
            </div>

            <a-divider orientation="left">Canal Admin 兼容</a-divider>
            <a-alert
              :type="settingForm.adminAutoRegister === 1 ? 'warning' : 'info'"
              :message="adminRegisterMessage"
              show-icon
              class="setting-section-alert"
            />
            <div class="toolbar setting-section-actions">
              <a-button @click="fillAdminDefaults">填充默认值</a-button>
              <a-button :loading="adminTesting" @click="testCanalAdmin">联调 Admin 注册</a-button>
              <a-tag v-if="adminTestResult.message" :color="adminTestColor">
                {{ adminTestResult.message }}
              </a-tag>
            </div>
            <a-table
              v-if="adminTestResult.checks?.length"
              class="admin-check-table"
              size="small"
              :data-source="adminTestResult.checks"
              :columns="adminCheckColumns"
              row-key="name"
              :pagination="false"
            >
              <template #bodyCell="{ column, record }">
                <a-tag v-if="column.key === 'ok'" :color="record.ok ? 'green' : 'red'">{{ record.ok ? '正常' : '异常' }}</a-tag>
              </template>
              <template #expandedRowRender="{ record }">
                <pre v-if="record.nodes?.length" class="code-block">{{ JSON.stringify(record.nodes, null, 2) }}</pre>
                <div v-else class="muted-path">{{ record.detail || '暂无明细' }}</div>
              </template>
            </a-table>
            <div class="setting-grid setting-grid-admin">
              <a-form-item label="Admin Manager" class="span-2">
                <a-input v-model:value="settingForm.adminManager" placeholder="127.0.0.1:8089" />
              </a-form-item>
              <a-form-item label="Admin 用户名">
                <a-input v-model:value="settingForm.adminUser" placeholder="admin" />
              </a-form-item>
              <a-form-item label="Admin 密文">
                <a-input-password v-model:value="settingForm.adminPassword" placeholder="server 侧 passwd 密文" />
              </a-form-item>
              <a-form-item label="自动注册">
                <a-switch :checked="settingForm.adminAutoRegister === 1" @change="setAdminAutoRegister" />
              </a-form-item>
              <a-form-item label="注册集群">
                <a-input v-model:value="settingForm.adminCluster" placeholder="留空表示不绑定集群" />
              </a-form-item>
              <a-form-item label="注册名称">
                <a-input v-model:value="settingForm.adminName" placeholder="canal-web" />
              </a-form-item>
            </div>

            <a-form-item label="高级追加 properties" class="setting-extra-properties">
              <a-textarea v-model:value="settingForm.extraProperties" class="editor" :rows="8" placeholder="每行一个 canal.properties 配置项" />
            </a-form-item>

            <div class="toolbar setting-actions">
              <a-button v-if="store.isAdmin" type="primary" @click="saveSetting">保存页面配置</a-button>
              <a-button v-if="store.isAdmin" @click="saveSettingAndRestart">保存并重启 Canal</a-button>
            </div>
          </a-form>
        </a-tab-pane>

        <a-tab-pane key="config" tab="生成配置">
          <a-collapse>
            <a-collapse-panel key="server" header="canal-server/conf/canal.properties">
              <pre class="code-block">{{ runtimeConfig.canalProperties || '暂无配置' }}</pre>
            </a-collapse-panel>
            <a-collapse-panel key="adapter" header="canal-adapter/conf/application.yml">
              <pre class="code-block">{{ runtimeConfig.adapterApplication || '暂无配置' }}</pre>
            </a-collapse-panel>
            <a-collapse-panel key="instances" header="实例 instance.properties">
              <a-collapse>
                <a-collapse-panel v-for="item in runtimeConfig.instances || []" :key="item.destination" :header="item.destination">
                  <div class="muted-path">{{ item.path }}</div>
                  <pre class="code-block">{{ item.content }}</pre>
                </a-collapse-panel>
              </a-collapse>
            </a-collapse-panel>
            <a-collapse-panel key="tasks" header="任务插件规格">
              <a-collapse>
                <a-collapse-panel v-for="item in runtimeConfig.taskSpecs || []" :key="item.name" :header="item.name">
                  <div class="muted-path">{{ item.path }}</div>
                  <pre class="code-block">{{ item.content }}</pre>
                </a-collapse-panel>
              </a-collapse>
            </a-collapse-panel>
          </a-collapse>
        </a-tab-pane>

        <a-tab-pane key="logs" tab="日志">
          <a-tabs>
            <a-tab-pane key="serverLogs" tab="Server">
              <a-collapse>
                <a-collapse-panel v-for="item in runtimeLogs.canalServer || []" :key="item.path" :header="item.name">
                  <div class="muted-path">{{ item.path }}</div>
                  <pre class="code-block">{{ item.content || '暂无日志' }}</pre>
                </a-collapse-panel>
              </a-collapse>
            </a-tab-pane>
            <a-tab-pane key="adapterLogs" tab="Adapter">
              <a-collapse>
                <a-collapse-panel v-for="item in runtimeLogs.canalAdapter || []" :key="item.path" :header="item.name">
                  <div class="muted-path">{{ item.path }}</div>
                  <pre class="code-block">{{ item.content || '暂无日志' }}</pre>
                </a-collapse-panel>
              </a-collapse>
            </a-tab-pane>
          </a-tabs>
        </a-tab-pane>

        <a-tab-pane key="metrics" tab="指标">
          <a-alert :type="metrics.available ? 'success' : 'warning'" :message="metrics.message || '暂无指标'" show-icon />
          <div class="muted-path metrics-endpoint">{{ metrics.endpoint }}</div>
          <a-table :data-source="metrics.samples || []" :columns="metricColumns" row-key="metric" :pagination="tablePagination(10)" />
          <a-collapse>
            <a-collapse-panel key="raw" header="Prometheus 原始指标">
              <pre class="code-block">{{ metrics.raw || '暂无指标' }}</pre>
            </a-collapse-panel>
          </a-collapse>
        </a-tab-pane>

        <a-tab-pane key="capabilities" tab="能力矩阵">
          <div class="capability-summary">
            <div class="capability-stat">
              <span class="capability-stat-label">已接入</span>
              <strong>{{ capabilityStatusCounts.connected }}</strong>
            </div>
            <div class="capability-stat">
              <span class="capability-stat-label">部分接入</span>
              <strong>{{ capabilityStatusCounts.partial }}</strong>
            </div>
            <div class="capability-stat">
              <span class="capability-stat-label">待接入</span>
              <strong>{{ capabilityStatusCounts.pending }}</strong>
            </div>
            <div class="capability-stat">
              <span class="capability-stat-label">覆盖率</span>
              <strong>{{ capabilityCoverage }}%</strong>
            </div>
          </div>
          <div class="toolbar capability-toolbar">
            <a-input-search
              v-model:value="capabilityKeyword"
              placeholder="搜索能力、入口、配置项"
              allow-clear
              style="width:280px"
            />
            <a-select
              v-model:value="capabilityStatusFilter"
              :options="capabilityStatusOptions"
              style="width:140px"
            />
            <a-button @click="activeTab = 'diagnostics'">运行自检</a-button>
            <a-button @click="activeTab = 'setting'">全局配置</a-button>
          </div>
          <a-table
            :data-source="filteredCapabilities"
            :columns="capabilityColumns"
            row-key="key"
            :pagination="tablePagination(10)"
          >
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'status'" :color="statusColor(record.status)">{{ record.status }}</a-tag>
              <a-space v-if="column.key === 'actions'">
                <a-button size="small" @click="openCapabilityEntry(record)">打开入口</a-button>
                <a-button size="small" @click="showCapabilityDetail(record)">详情</a-button>
              </a-space>
            </template>
            <template #expandedRowRender="{ record }">
              <a-descriptions bordered :column="1" size="small">
                <a-descriptions-item label="能力 Key">{{ record.key }}</a-descriptions-item>
                <a-descriptions-item label="能力说明">{{ record.name }}</a-descriptions-item>
                <a-descriptions-item label="入口">{{ record.entry }}</a-descriptions-item>
                <a-descriptions-item label="配置项">{{ record.configKeys }}</a-descriptions-item>
                <a-descriptions-item label="下一步">{{ record.nextAction }}</a-descriptions-item>
              </a-descriptions>
            </template>
          </a-table>
        </a-tab-pane>
      </a-tabs>
    </div>

    <a-modal
      v-model:open="pathEditorOpen"
      title="运行路径配置"
      width="880px"
      :confirm-loading="pathSaving"
      ok-text="保存路径"
      cancel-text="取消"
      @ok="savePathSetting"
    >
      <a-alert
        type="info"
        message="保存后会刷新 Canal Runtime 状态、自检和配置一致性；如 Server 或 Adapter 目录变更，建议再执行刷新配置并重启。"
        show-icon
        class="path-editor-alert"
      />
      <a-form layout="vertical" :model="pathForm">
        <div class="toolbar path-editor-actions">
          <a-button @click="useProjectRuntimeDefaults">恢复项目默认路径</a-button>
        </div>
        <a-form-item label="项目根目录">
          <a-input v-model:value="pathForm.projectDir" placeholder="/path/to/canal-web（留空读取启动参数或当前目录）" />
        </a-form-item>
        <div class="target-grid">
          <a-form-item label="Runtime 根目录">
            <a-input v-model:value="pathForm.runtimeRootDir" placeholder="./canal-runtime（留空读取项目默认）" />
          </a-form-item>
          <a-form-item label="Canal Server 目录">
            <a-input v-model:value="pathForm.canalServerHome" placeholder="./canal-runtime/canal-server（留空自动推导）" />
          </a-form-item>
          <a-form-item label="Canal Adapter 目录">
            <a-input v-model:value="pathForm.canalAdapterHome" placeholder="./canal-runtime/canal-adapter（留空自动推导）" />
          </a-form-item>
          <a-form-item label="dbsync 源码 Jar">
            <a-input v-model:value="pathForm.dbsyncSourceJar" placeholder="/path/to/canal.parse.dbsync-1.1.9-SNAPSHOT.jar" />
          </a-form-item>
          <a-form-item label="dbsync Runtime Jar">
            <a-input v-model:value="pathForm.dbsyncRuntimeJar" placeholder="/path/to/canal-server/lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar" />
          </a-form-item>
          <a-form-item label="Generated 目录">
            <a-textarea v-model:value="pathForm.generatedPaths" :rows="2" placeholder="多个目录用逗号分隔，例如 /path/canal-runtime/generated" />
          </a-form-item>
        </div>
      </a-form>
    </a-modal>

    <a-modal v-model:open="capabilityDetailOpen" title="能力详情" :footer="null" width="760px">
      <a-descriptions bordered :column="1" size="small" v-if="selectedCapability">
        <a-descriptions-item label="分类">{{ selectedCapability.category }}</a-descriptions-item>
        <a-descriptions-item label="能力">{{ selectedCapability.name }}</a-descriptions-item>
        <a-descriptions-item label="状态">
          <a-tag :color="statusColor(selectedCapability.status)">{{ selectedCapability.status }}</a-tag>
        </a-descriptions-item>
        <a-descriptions-item label="入口">{{ selectedCapability.entry }}</a-descriptions-item>
        <a-descriptions-item label="配置项">{{ selectedCapability.configKeys }}</a-descriptions-item>
        <a-descriptions-item label="下一步">{{ selectedCapability.nextAction }}</a-descriptions-item>
      </a-descriptions>
    </a-modal>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import { message, Modal } from 'ant-design-vue'
import { Pencil } from 'lucide-vue-next'
import { systemApi } from '../../api'
import { useAppStore } from '../../stores/useAppStore'
import { tablePagination } from '../../utils/pagination'

const store = useAppStore()
const activeTab = ref('runtime')
const canalStatus = ref(null)
const runtimeConfig = ref({})
const runtimeLogs = ref({})
const metrics = ref({})
const capabilities = ref([])
const capabilityKeyword = ref('')
const capabilityStatusFilter = ref('')
const capabilityDetailOpen = ref(false)
const selectedCapability = ref(null)
const diagnostics = ref({})
const configConsistency = ref({})
const staleFiles = ref({})
const settingForm = ref(defaultSetting())
const pathEditorOpen = ref(false)
const pathSaving = ref(false)
const pathForm = ref(defaultPathSetting())
const adminTesting = ref(false)
const adminTestResult = ref({})

const serverModeOptions = [
  { label: 'TCP 本地 Adapter', value: 'tcp' },
  { label: 'Kafka', value: 'kafka' },
  { label: 'RocketMQ', value: 'rocketMQ' },
  { label: 'RabbitMQ', value: 'rabbitMQ' },
  { label: 'PulsarMQ', value: 'pulsarMQ' }
]

const accessChannelOptions = [
  { label: 'local', value: 'local' },
  { label: 'zookeeper', value: 'zookeeper' }
]

const capabilityStatusOptions = [
  { label: '全部状态', value: '' },
  { label: '已接入', value: '已接入' },
  { label: '部分接入', value: '部分接入' },
  { label: '待接入', value: '待接入' }
]

const runtimeSummary = computed(() => {
  const connected = capabilities.value.filter(item => item.status === '已接入' || item.status === '部分接入').length
  return {
    instances: runtimeConfig.value.instances?.length || 0,
    connected
  }
})

const capabilityStatusCounts = computed(() => ({
  connected: capabilities.value.filter(item => item.status === '已接入').length,
  partial: capabilities.value.filter(item => item.status === '部分接入').length,
  pending: capabilities.value.filter(item => item.status === '待接入').length
}))

const capabilityCoverage = computed(() => {
  if (!capabilities.value.length) return 0
  const score = capabilityStatusCounts.value.connected + capabilityStatusCounts.value.partial * 0.5
  return Math.round((score / capabilities.value.length) * 100)
})

const filteredCapabilities = computed(() => {
  const keyword = capabilityKeyword.value.trim().toLowerCase()
  return capabilities.value.filter(item => {
    const statusMatched = !capabilityStatusFilter.value || item.status === capabilityStatusFilter.value
    const text = [item.category, item.name, item.entry, item.configKeys, item.nextAction, item.status]
      .join(' ')
      .toLowerCase()
    return statusMatched && (!keyword || text.includes(keyword))
  })
})

const adminTestColor = computed(() => {
  if (adminTestResult.value.skipped) return 'blue'
  return adminTestResult.value.reachable ? 'green' : 'red'
})

const adminRegisterMessage = computed(() => {
  if (settingForm.value.adminAutoRegister === 1) {
    return '已启用 Canal Admin 自动注册，启动或刷新 Canal 前需要确保 Admin Manager 可连接。'
  }
  return '未启用 Canal Admin 自动注册，系统会生成 Admin 配置但运行自检会跳过 Manager 连通性检查。'
})

const metricColumns = [
  { title: '指标', dataIndex: 'metric' },
  { title: '值', dataIndex: 'value', width: 180 }
]

const capabilityColumns = [
  { title: '分类', dataIndex: 'category', width: 140 },
  { title: '能力', dataIndex: 'name' },
  { title: '入口', dataIndex: 'entry', width: 180 },
  { title: '配置项', dataIndex: 'configKeys' },
  { title: '下一步', dataIndex: 'nextAction' },
  { title: '状态', key: 'status', width: 110 },
  { title: '操作', key: 'actions', width: 160 }
]

const diagnosticColumns = [
  { title: '检查项', dataIndex: 'name', width: 220 },
  { title: '结果', key: 'ok', width: 100 },
  { title: '说明', dataIndex: 'message', width: 160 },
  { title: '路径', dataIndex: 'path' }
]

const adminCheckColumns = [
  { title: '检查项', dataIndex: 'name', width: 180 },
  { title: '结果', key: 'ok', width: 90 },
  { title: '说明', dataIndex: 'message', width: 220 },
  { title: '明细', dataIndex: 'detail' }
]
const consistencyColumns = [
  { title: '文件', dataIndex: 'name', width: 220 },
  { title: '状态', key: 'ok', width: 100 },
  { title: '说明', dataIndex: 'message', width: 220 },
  { title: '大小(实际/期望)', key: 'size', width: 140 },
  { title: '路径', dataIndex: 'path' }
]
const staleColumns = [
  { title: '类型', dataIndex: 'type', width: 140 },
  { title: '名称', dataIndex: 'name', width: 220 },
  { title: '说明', dataIndex: 'message', width: 260 },
  { title: '路径', dataIndex: 'path' }
]

function statusColor(status) {
  if (status === '已接入') return 'green'
  if (status === '部分接入') return 'orange'
  return 'blue'
}

function showCapabilityDetail(record) {
  selectedCapability.value = record
  capabilityDetailOpen.value = true
}

function openCapabilityEntry(record) {
  const entry = record.entry || ''
  if (entry.includes('运行状态')) activeTab.value = 'runtime'
  else if (entry.includes('指标')) activeTab.value = 'metrics'
  else if (entry.includes('日志')) activeTab.value = 'logs'
  else if (entry.includes('配置') || entry.includes('MQ') || entry.includes('HA') || entry.includes('Admin')) activeTab.value = 'setting'
  else if (entry.includes('生成')) activeTab.value = 'config'
  else {
    message.info(`入口在「${entry}」`)
    return
  }
  message.success(`已打开入口：${entry}`)
}

async function loadAll() {
  const requests = await Promise.allSettled([
    systemApi.canalStatus(),
    systemApi.canalConfig(),
    systemApi.canalLogs(),
    systemApi.canalMetrics(),
    systemApi.canalCapabilities(),
    systemApi.canalSetting(),
    systemApi.canalDiagnostics(),
    systemApi.canalConfigConsistency(),
    systemApi.canalStaleFiles()
  ])
  const firstError = requests.find(result => result.status === 'rejected')
  if (firstError?.reason?.__loginExpired) return
  const values = requests.map(result => result.status === 'fulfilled' ? result.value : null)
  const [status, config, logs, metricsResult, capabilityResult, settingResult, diagnosticResult, consistencyResult, staleResult] = values
  canalStatus.value = status || canalStatus.value || {}
  runtimeConfig.value = config || runtimeConfig.value || {}
  runtimeLogs.value = logs || runtimeLogs.value || {}
  metrics.value = metricsResult || metrics.value || {}
  capabilities.value = capabilityResult || capabilities.value || []
  diagnostics.value = diagnosticResult || diagnostics.value || {}
  configConsistency.value = consistencyResult || configConsistency.value || {}
  staleFiles.value = staleResult || staleFiles.value || {}
  settingForm.value = { ...defaultSetting(), ...(settingResult || settingForm.value || {}) }
  if (firstError) throw firstError.reason
}

function defaultSetting() {
  return {
    serverMode: 'tcp',
    zkServers: '',
    flatMessage: 1,
    canalBatchSize: 50,
    canalGetTimeout: 100,
    accessChannel: 'local',
    mqServers: '',
    mqUsername: '',
    mqPassword: '',
    mqTopic: '',
    mqPartitionHash: '',
    mqDynamicTopic: '',
    adminManager: '127.0.0.1:8089',
    adminUser: 'admin',
    adminPassword: '',
    adminAutoRegister: 0,
    adminCluster: '',
    adminName: 'canal-web',
    projectDir: '',
    runtimeRootDir: '',
    canalServerHome: '',
    canalAdapterHome: '',
    dbsyncSourceJar: '',
    dbsyncRuntimeJar: '',
    generatedPaths: '',
    extraProperties: ''
  }
}

function defaultPathSetting() {
  return {
    projectDir: '',
    runtimeRootDir: '',
    canalServerHome: '',
    canalAdapterHome: '',
    dbsyncSourceJar: '',
    dbsyncRuntimeJar: '',
    generatedPaths: ''
  }
}

function openPathEditor() {
  pathForm.value = {
    projectDir: settingForm.value.projectDir || runtimeConfig.value?.paths?.projectDir || '',
    runtimeRootDir: settingForm.value.runtimeRootDir || canalStatus.value?.rootDir || '',
    canalServerHome: settingForm.value.canalServerHome || canalStatus.value?.canalServer?.home || '',
    canalAdapterHome: settingForm.value.canalAdapterHome || canalStatus.value?.canalAdapter?.home || '',
    dbsyncSourceJar: settingForm.value.dbsyncSourceJar || '',
    dbsyncRuntimeJar: settingForm.value.dbsyncRuntimeJar || '',
    generatedPaths: settingForm.value.generatedPaths || ''
  }
  pathEditorOpen.value = true
}

function useProjectRuntimeDefaults() {
  pathForm.value = {
    projectDir: '',
    runtimeRootDir: '',
    canalServerHome: '',
    canalAdapterHome: '',
    dbsyncSourceJar: '',
    dbsyncRuntimeJar: '',
    generatedPaths: ''
  }
}

async function savePathSetting() {
  pathSaving.value = true
  try {
    const payload = { ...settingForm.value, ...pathForm.value }
    const saved = await systemApi.saveCanalSetting(payload)
    settingForm.value = { ...defaultSetting(), ...(saved || {}) }
    pathEditorOpen.value = false
    await loadAll()
    message.success('运行路径已保存')
  } finally {
    pathSaving.value = false
  }
}

async function saveSetting() {
  applySettingDefaults()
  const saved = await systemApi.saveCanalSetting(settingForm.value)
  settingForm.value = { ...defaultSetting(), ...(saved || {}) }
  await loadAll()
  message.success('Canal 页面配置已保存')
}

async function saveSettingAndRestart() {
  applySettingDefaults()
  await systemApi.saveCanalSetting(settingForm.value)
  await systemApi.refreshCanal()
  await loadAll()
  message.success('Canal 配置已保存并重启')
}

async function testCanalAdmin() {
  applySettingDefaults()
  adminTesting.value = true
  try {
    const result = await systemApi.testCanalAdmin(settingForm.value)
    adminTestResult.value = result || {}
    if (result?.skipped) {
      message.info(result.message || '未启用 Canal Admin 自动注册，已跳过检查')
    } else if (result?.ok) {
      message.success('Canal Admin 注册联调通过')
    } else {
      message.warning(result?.message || 'Canal Admin 注册联调未通过')
    }
  } finally {
    adminTesting.value = false
  }
}

function setAdminAutoRegister(checked) {
  settingForm.value.adminAutoRegister = checked ? 1 : 0
  if (checked) {
    fillAdminDefaults()
  }
}

function fillAdminDefaults() {
  settingForm.value.adminManager = settingForm.value.adminManager || '127.0.0.1:8089'
  settingForm.value.adminUser = settingForm.value.adminUser || 'admin'
  settingForm.value.adminCluster = settingForm.value.adminCluster || 'default'
  settingForm.value.adminName = settingForm.value.adminName || 'canal-web'
  adminTestResult.value = {}
}

async function refreshCanal() {
  await systemApi.refreshCanal()
  await loadAll()
  message.success('Canal 配置已刷新')
}

async function startCanal() {
  await systemApi.startCanal()
  await loadAll()
  message.success('Canal 已启动')
}

async function stopCanal() {
  await systemApi.stopCanal()
  await loadAll()
  message.success('Canal 已停止')
}

async function componentAction(component, action) {
  const actions = {
    server: {
      start: systemApi.startCanalServer,
      stop: systemApi.stopCanalServer,
      restart: systemApi.restartCanalServer
    },
    adapter: {
      start: systemApi.startCanalAdapter,
      stop: systemApi.stopCanalAdapter,
      restart: systemApi.restartCanalAdapter
    }
  }
  await actions[component][action]()
  await loadAll()
  message.success(`${component === 'server' ? 'Canal Server' : 'Canal Adapter'} 操作完成`)
}

function cleanStaleFiles() {
  Modal.confirm({
    title: '清理失效运行配置',
    content: '会删除未启用数据源的 instance 目录，以及无效任务或无效 destination 的 Adapter 插件文件。确认继续？',
    async onOk() {
      const result = await systemApi.cleanCanalStaleFiles()
      await loadAll()
      message.success(`已清理 ${result.cleaned || 0} 个失效配置`)
    }
  })
}

function applySettingDefaults() {
  if (!settingForm.value.adminManager) settingForm.value.adminManager = '127.0.0.1:8089'
  if (!settingForm.value.adminUser) settingForm.value.adminUser = 'admin'
  if (!settingForm.value.adminName) settingForm.value.adminName = 'canal-web'
}

onMounted(() => loadAll().catch(() => {}))
</script>
