<template>
  <div>
    <div class="page-head">
      <h1 class="page-title">数据源管理</h1>
      <div class="toolbar">
        <a-button v-if="store.canOperate" @click="batchToggle(true)" :disabled="!selectedRowKeys.length">批量启用</a-button>
        <a-button v-if="store.canOperate" @click="batchToggle(false)" :disabled="!selectedRowKeys.length">批量禁用</a-button>
        <a-button v-if="store.canOperate" type="primary" @click="openCreate"><Plus :size="15" />新增数据源</a-button>
      </div>
    </div>
    <div class="panel">
      <a-table
        :data-source="store.datasources"
        :columns="columns"
        row-key="id"
        :pagination="tablePagination(10)"
        :row-selection="{ selectedRowKeys, onChange: keys => selectedRowKeys = keys }"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'status'">
            <a-switch :checked="record.status === 1" :disabled="!store.canOperate" @change="checked => toggle(record, checked)" />
          </template>
          <template v-if="column.key === 'actions'">
            <a-space>
              <a-button v-if="store.canOperate" size="small" @click="edit(record)"><Pencil :size="14" />编辑</a-button>
              <a-button v-if="store.canOperate" size="small" @click="cloneDatasource(record)"><Copy :size="14" />复制</a-button>
              <a-button v-if="store.canOperate" size="small" @click="test(record)"><PlugZap :size="14" />测试</a-button>
              <a-button size="small" @click="openDiagnostics(record)"><Activity :size="14" />自检</a-button>
              <span v-if="!store.canOperate" class="muted-path">只读</span>
            </a-space>
          </template>
        </template>
      </a-table>
    </div>

    <a-modal v-model:open="open" title="数据源与 Canal 实例配置" width="980px" :footer="null">
      <a-alert
        type="info"
        message="保存前建议先测试连接并读取当前 Binlog 位点；Binlog 文件和 Position 留空时 Canal 会从当前位点开始。"
        show-icon
        class="datasource-form-alert"
      />
      <a-form ref="formRef" layout="vertical" :model="form">
        <a-tabs>
          <a-tab-pane key="base" tab="基础连接">
            <a-row :gutter="12">
              <a-col :span="12"><a-form-item label="数据源标识" name="dataSourceKey" :rules="requiredRules('请输入数据源标识')"><a-input v-model:value="form.dataSourceKey" placeholder="例如 verify_mysql_source" @blur="normalizeIdentity" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="Canal Destination" name="canalDestination" :rules="requiredRules('请输入 Canal Destination')"><a-input v-model:value="form.canalDestination" placeholder="例如 verify_mysql_dest" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="主机" name="host" :rules="requiredRules('请输入主机')"><a-input v-model:value="form.host" placeholder="localhost 或 127.0.0.1" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="端口" name="port" :rules="requiredRules('请输入端口')"><a-input-number v-model:value="form.port" :min="1" :max="65535" style="width:100%" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="用户名" name="username" :rules="requiredRules('请输入用户名')"><a-input v-model:value="form.username" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="密码" name="password" :rules="requiredRules('请输入密码')"><a-input-password v-model:value="form.password" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="数据库" name="dbName" :rules="requiredRules('请输入数据库名')"><a-input v-model:value="form.dbName" placeholder="例如 canal_sync_verify" @blur="applyDefaultFilterRegex" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="Server ID" name="serverId"><a-input-number v-model:value="form.serverId" :min="1" style="width:100%" placeholder="Canal slaveId，不能等于 MySQL server_id" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="Binlog 文件"><a-input v-model:value="form.binlogFile" placeholder="留空从当前位点开始" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="Binlog Position"><a-input-number v-model:value="form.binlogPosition" style="width:100%" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="Binlog Timestamp"><a-input-number v-model:value="form.binlogTimestamp" :min="0" style="width:100%" placeholder="毫秒时间戳，可留空" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="GTID"><a-input v-model:value="form.gtid" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="启用 GTID"><a-switch :checked="form.gtidEnabled === 1" @change="checked => form.gtidEnabled = checked ? 1 : 0" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="SSL 模式"><a-select v-model:value="form.sslMode" :options="sslModeOptions" /></a-form-item></a-col>
            </a-row>
            <div class="toolbar datasource-inline-actions">
              <a-button :loading="actionLoading === 'test-form'" @click="testForm"><PlugZap :size="15" />测试连接</a-button>
              <a-button :loading="actionLoading === 'position'" @click="readBinlogPosition"><LocateFixed :size="15" />读取当前位点</a-button>
              <span class="muted-path" v-if="positionPreview.file">当前位点：{{ positionPreview.file }} / {{ positionPreview.position }}</span>
            </div>
          </a-tab-pane>

          <a-tab-pane key="filter" tab="过滤规则">
            <a-row :gutter="12">
              <a-col :span="12"><a-form-item label="订阅表正则"><a-input v-model:value="form.filterRegex" placeholder="db\\.table 或 db\\..*" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="黑名单正则"><a-input v-model:value="form.filterBlackRegex" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="字段白名单"><a-input v-model:value="form.fieldFilter" placeholder="db.table:col1/col2" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="字段黑名单"><a-input v-model:value="form.fieldBlackFilter" placeholder="db.table:col1/col2" /></a-form-item></a-col>
            </a-row>
            <div class="switch-grid">
              <a-checkbox :checked="form.filterDmlInsert === 1" @change="e => form.filterDmlInsert = e.target.checked ? 1 : 0">过滤 INSERT</a-checkbox>
              <a-checkbox :checked="form.filterDmlUpdate === 1" @change="e => form.filterDmlUpdate = e.target.checked ? 1 : 0">过滤 UPDATE</a-checkbox>
              <a-checkbox :checked="form.filterDmlDelete === 1" @change="e => form.filterDmlDelete = e.target.checked ? 1 : 0">过滤 DELETE</a-checkbox>
              <a-checkbox :checked="form.filterQueryDml === 1" @change="e => form.filterQueryDml = e.target.checked ? 1 : 0">过滤 Query DML</a-checkbox>
              <a-checkbox :checked="form.filterQueryDcl === 1" @change="e => form.filterQueryDcl = e.target.checked ? 1 : 0">过滤 Query DCL</a-checkbox>
              <a-checkbox :checked="form.filterQueryDdl === 1" @change="e => form.filterQueryDdl = e.target.checked ? 1 : 0">过滤 Query DDL</a-checkbox>
              <a-checkbox :checked="form.filterRows === 1" @change="e => form.filterRows = e.target.checked ? 1 : 0">过滤 Row 数据</a-checkbox>
              <a-checkbox :checked="form.filterTableError === 1" @change="e => form.filterTableError = e.target.checked ? 1 : 0">过滤表错误</a-checkbox>
              <a-checkbox :checked="form.filterTransactionEntry === 1" @change="e => form.filterTransactionEntry = e.target.checked ? 1 : 0">过滤事务 Entry</a-checkbox>
              <a-checkbox :checked="form.ddlIsolation === 1" @change="e => form.ddlIsolation = e.target.checked ? 1 : 0">DDL 隔离</a-checkbox>
            </div>
          </a-tab-pane>

          <a-tab-pane key="tsdb" tab="TSDB">
            <a-row :gutter="12">
              <a-col :span="12"><a-form-item label="启用 TSDB"><a-switch :checked="form.tsdbEnable === 1" @change="checked => form.tsdbEnable = checked ? 1 : 0" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="TSDB JDBC URL"><a-input v-model:value="form.tsdbUrl" placeholder="jdbc:mysql://127.0.0.1:3306/canal_tsdb" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="TSDB 用户名"><a-input v-model:value="form.tsdbUsername" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="TSDB 密码"><a-input-password v-model:value="form.tsdbPassword" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="快照间隔(小时)"><a-input-number v-model:value="form.tsdbSnapshotInterval" :min="1" style="width:100%" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="快照保留(小时)"><a-input-number v-model:value="form.tsdbSnapshotExpire" :min="1" style="width:100%" /></a-form-item></a-col>
            </a-row>
          </a-tab-pane>

          <a-tab-pane key="ha" tab="主备与云 RDS">
            <a-row :gutter="12">
              <a-col :span="12"><a-form-item label="备用库地址"><a-input v-model:value="form.standbyAddress" placeholder="127.0.0.1:3306" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="备用 Binlog 文件"><a-input v-model:value="form.standbyJournalName" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="备用 Position"><a-input-number v-model:value="form.standbyPosition" style="width:100%" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="备用 Timestamp"><a-input-number v-model:value="form.standbyTimestamp" style="width:100%" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="备用 GTID"><a-input v-model:value="form.standbyGtid" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="RDS AccessKey"><a-input v-model:value="form.rdsAccesskey" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="RDS SecretKey"><a-input-password v-model:value="form.rdsSecretkey" /></a-form-item></a-col>
              <a-col :span="12"><a-form-item label="RDS InstanceId"><a-input v-model:value="form.rdsInstanceId" /></a-form-item></a-col>
            </a-row>
          </a-tab-pane>

          <a-tab-pane key="advanced" tab="高级参数">
            <a-form-item label="追加 instance.properties">
              <a-textarea v-model:value="form.extraProperties" class="editor" :rows="10" placeholder="每行一个 canal.instance.* 或 canal.mq.* 参数" />
            </a-form-item>
          </a-tab-pane>
        </a-tabs>
      </a-form>
      <div class="toolbar datasource-modal-footer">
        <a-button @click="open = false">取消</a-button>
        <a-button :loading="actionLoading === 'test-form'" @click="testForm"><PlugZap :size="15" />测试连接</a-button>
        <a-button :loading="actionLoading === 'position'" @click="readBinlogPosition"><LocateFixed :size="15" />读取位点</a-button>
        <a-button type="primary" :loading="actionLoading === 'save'" @click="submit"><Save :size="15" />保存</a-button>
        <a-button type="primary" :loading="actionLoading === 'save-diagnostics'" @click="submitAndDiagnostics"><Activity :size="15" />保存并自检</a-button>
      </div>
    </a-modal>

    <a-drawer v-model:open="diagnosticsOpen" title="数据源接入自检" width="760px">
      <a-alert
        :type="diagnostics.ok ? 'success' : 'warning'"
        :message="`通过 ${diagnostics.okCount || 0}/${diagnostics.total || 0} 项检查`"
        show-icon
      />
      <a-descriptions bordered :column="1" size="small" style="margin:12px 0">
        <a-descriptions-item label="数据源">{{ diagnostics.dataSourceKey }}</a-descriptions-item>
        <a-descriptions-item label="地址">{{ diagnostics.host }}:{{ diagnostics.port }}/{{ diagnostics.dbName }}</a-descriptions-item>
        <a-descriptions-item label="Destination">{{ diagnostics.canalDestination }}</a-descriptions-item>
        <a-descriptions-item label="当前 Binlog">
          {{ diagnostics.binlogStatus?.file || '-' }} / {{ diagnostics.binlogStatus?.position || '-' }}
        </a-descriptions-item>
      </a-descriptions>

      <a-table :data-source="diagnostics.checks || []" :columns="diagnosticColumns" row-key="name" :pagination="tablePagination(10)" size="small">
        <template #bodyCell="{ column, record }">
          <a-tag v-if="column.key === 'ok'" :color="record.ok ? 'green' : 'red'">{{ record.ok ? '通过' : '异常' }}</a-tag>
        </template>
      </a-table>

      <a-divider orientation="left">订阅表匹配</a-divider>
      <a-table :data-source="diagnostics.matchedTables || []" :columns="matchedTableColumns" row-key="tableName" :pagination="tablePagination(10)" size="small" />

      <a-divider orientation="left">当前用户权限</a-divider>
      <pre class="code-block">{{ (diagnostics.grants || []).join('\n') || '暂无权限信息' }}</pre>
    </a-drawer>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue'
import { message } from 'ant-design-vue'
import { Activity, Copy, LocateFixed, Pencil, PlugZap, Plus, Save } from 'lucide-vue-next'
import { datasourceApi } from '../../api'
import { useAppStore } from '../../stores/useAppStore'
import { tablePagination } from '../../utils/pagination'

const store = useAppStore()
const open = ref(false)
const diagnosticsOpen = ref(false)
const diagnostics = ref({})
const selectedRowKeys = ref([])
const formRef = ref(null)
const actionLoading = ref('')
const positionPreview = ref({})
const form = reactive(defaultForm())
const columns = [
  { title: '标识', dataIndex: 'dataSourceKey' },
  { title: '地址', customRender: ({ record }) => `${record.host}:${record.port}` },
  { title: '数据库', dataIndex: 'dbName' },
  { title: 'Destination', dataIndex: 'canalDestination' },
  { title: '启用', key: 'status' },
  { title: '操作', key: 'actions', width: 260 }
]
const diagnosticColumns = [
  { title: '检查项', dataIndex: 'name', width: 150 },
  { title: '结果', key: 'ok', width: 80 },
  { title: '说明', dataIndex: 'message' },
  { title: '处理建议', dataIndex: 'detail' }
]
const matchedTableColumns = [
  { title: '表名', dataIndex: 'tableName' }
]
const sslModeOptions = [
  { label: 'DISABLED', value: 'DISABLED' },
  { label: 'PREFERRED', value: 'PREFERRED' },
  { label: 'REQUIRED', value: 'REQUIRED' },
  { label: 'VERIFY_CA', value: 'VERIFY_CA' },
  { label: 'VERIFY_IDENTITY', value: 'VERIFY_IDENTITY' }
]

function defaultForm() {
  return {
    id: null,
    dataSourceKey: '',
    host: 'localhost',
    port: 3306,
    username: 'root',
    password: '',
    dbName: '',
    canalDestination: 'mysql_dest',
    filterRegex: '',
    filterBlackRegex: 'mysql\\.slave_.*',
    binlogFile: '',
    binlogPosition: null,
    binlogTimestamp: null,
    gtid: '',
    gtidEnabled: 0,
    serverId: null,
    fieldFilter: '',
    fieldBlackFilter: '',
    filterDmlInsert: 0,
    filterDmlUpdate: 0,
    filterDmlDelete: 0,
    filterQueryDml: 0,
    filterQueryDcl: 0,
    filterQueryDdl: 0,
    filterRows: 0,
    filterTableError: 0,
    filterTransactionEntry: 0,
    ddlIsolation: 0,
    tsdbEnable: 0,
    tsdbUrl: '',
    tsdbUsername: '',
    tsdbPassword: '',
    tsdbSnapshotInterval: 24,
    tsdbSnapshotExpire: 360,
    standbyAddress: '',
    standbyJournalName: '',
    standbyPosition: null,
    standbyTimestamp: null,
    standbyGtid: '',
    rdsAccesskey: '',
    rdsSecretkey: '',
    rdsInstanceId: '',
    sslMode: 'DISABLED',
    extraProperties: '',
    status: 1
  }
}

function assign(data) {
  Object.assign(form, defaultForm(), data)
  positionPreview.value = {}
}

function openCreate() {
  assign({})
  open.value = true
}

function edit(record) {
  assign(record)
  open.value = true
}

async function submit() {
  await saveDatasource('save')
}

async function test(record) {
  await datasourceApi.test(record)
  message.success('连接成功')
}

async function testForm() {
  await validateBaseForm()
  await withLoading('test-form', async () => {
    await datasourceApi.test(form)
    message.success('连接成功')
  })
}

async function readBinlogPosition() {
  await validateBaseForm()
  await withLoading('position', async () => {
    const result = await datasourceApi.binlogPosition(form)
    positionPreview.value = result || {}
    if (result?.file) {
      form.binlogFile = result.file
      form.binlogPosition = Number(result.position || 0)
    }
    message.success('当前 Binlog 位点已回填')
  })
}

async function submitAndDiagnostics() {
  const saved = await saveDatasource('save-diagnostics', false)
  if (saved?.dataSourceKey) {
    diagnostics.value = await datasourceApi.diagnostics(saved.dataSourceKey)
    diagnosticsOpen.value = true
  }
  open.value = false
}

async function saveDatasource(loadingKey = 'save', closeAfterSave = true) {
  await validateBaseForm()
  normalizeIdentity()
  applyDefaultFilterRegex()
  return withLoading(loadingKey, async () => {
    const saved = await datasourceApi.save(form)
    await store.refreshDatasources()
    await store.refreshStats()
    if (closeAfterSave) open.value = false
    message.success('数据源已保存')
    return saved
  })
}

async function cloneDatasource(record) {
  const cloned = await datasourceApi.clone(record.id)
  await store.refreshDatasources()
  await store.refreshStats()
  message.success(`已复制数据源：${cloned?.dataSourceKey || '新数据源'}，默认禁用`)
}

async function openDiagnostics(record) {
  diagnostics.value = await datasourceApi.diagnostics(record.dataSourceKey)
  diagnosticsOpen.value = true
}

async function toggle(record, enabled) {
  await datasourceApi.enable({ id: record.id, enabled })
  await store.refreshDatasources()
}

async function batchToggle(enabled) {
  await Promise.all(selectedRowKeys.value.map(id => datasourceApi.enable({ id, enabled })))
  selectedRowKeys.value = []
  await store.refreshDatasources()
  await store.refreshStats()
  message.success(enabled ? '批量启用完成' : '批量禁用完成')
}

async function validateBaseForm() {
  if (formRef.value) {
    await formRef.value.validate()
  }
}

async function withLoading(key, fn) {
  actionLoading.value = key
  try {
    return await fn()
  } finally {
    actionLoading.value = ''
  }
}

function requiredRules(messageText) {
  return [{ required: true, message: messageText, trigger: ['blur', 'change'] }]
}

function normalizeIdentity() {
  if (form.dataSourceKey) {
    form.dataSourceKey = form.dataSourceKey.trim()
  }
  if (!form.canalDestination || form.canalDestination === 'mysql_dest') {
    form.canalDestination = form.dataSourceKey ? `${form.dataSourceKey.replace(/_source$/, '')}_dest` : 'mysql_dest'
  }
}

function applyDefaultFilterRegex() {
  if (!form.filterRegex && form.dbName) {
    form.filterRegex = `${escapeRegex(form.dbName)}\\..*`
  }
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}
</script>
