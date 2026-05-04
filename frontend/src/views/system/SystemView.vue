<template>
  <div>
    <div class="page-head">
      <h1 class="page-title">系统管理</h1>
    </div>

    <div class="panel">
      <a-descriptions bordered :column="1">
        <a-descriptions-item label="认证方案">Sa-Token 登录已启用，默认开发账号 admin/admin123</a-descriptions-item>
        <a-descriptions-item label="运行数据库">MySQL，启动时自动初始化表结构</a-descriptions-item>
        <a-descriptions-item label="运维入口">Canal Server、Adapter、日志、指标与能力矩阵统一在 Canal 运维页面操作</a-descriptions-item>
      </a-descriptions>

      <a-divider />
      <div class="page-head">
        <h1 class="page-title">本机链路</h1>
        <div class="toolbar">
          <a-input v-model:value="verifyId" style="width:140px" placeholder="验证 ID" />
          <a-button :loading="localStackLoading" @click="loadLocalStack">刷新状态</a-button>
          <a-button v-if="store.canOperate" type="primary" :loading="verifyLoading" @click="verifyLocalStack">一键验证</a-button>
          <a-button v-if="store.isAdmin" :loading="actionLoading === 'start'" @click="runLocalStackAction('start')">启动</a-button>
          <a-button v-if="store.isAdmin" :loading="actionLoading === 'restart'" @click="runLocalStackAction('restart')">重启</a-button>
          <a-button v-if="store.isAdmin" danger :loading="actionLoading === 'stop'" @click="runLocalStackAction('stop')">停止</a-button>
        </div>
      </div>
      <a-alert
        :type="localStack.ok ? 'success' : 'warning'"
        :message="`本机服务 ${localStack.runningCount || 0}/${localStack.total || 0} 可用`"
        show-icon
      />
      <a-table
        class="local-stack-table"
        :data-source="localStack.services || []"
        :columns="localStackColumns"
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
      <a-descriptions class="local-stack-meta" bordered :column="1" size="small">
        <a-descriptions-item label="脚本">{{ localStack.script || '-' }}</a-descriptions-item>
        <a-descriptions-item label="LaunchAgent">
          <pre class="inline-code">{{ localStack.launchAgents?.stdout || '暂无输出' }}</pre>
        </a-descriptions-item>
        <a-descriptions-item label="Kafka Service">
          <pre class="inline-code">{{ localStack.kafkaService?.stdout || '暂无输出' }}</pre>
        </a-descriptions-item>
      </a-descriptions>
      <div v-if="verifyResult.action" class="verify-result">
        <a-alert
          :type="verifyResult.ok ? 'success' : 'error'"
          :message="`${verifyActionName(verifyResult.action)}${verifyResult.ok ? '成功' : '失败'}，耗时 ${verifyResult.durationMs || 0}ms`"
          show-icon
        />
        <pre class="code-block">{{ commandOutput(verifyResult) }}</pre>
      </div>

      <a-divider />
      <div class="page-head">
        <h1 class="page-title">用户</h1>
        <a-button v-if="store.isAdmin" type="primary" @click="openUserCreate">新增用户</a-button>
      </div>
      <a-table :data-source="users" :columns="columns" row-key="id" :pagination="tablePagination(10)">
        <template #bodyCell="{ column, record }">
          <a-tag v-if="column.key === 'status'" :color="record.status === 1 ? 'green' : 'red'">
            {{ record.status === 1 ? '启用' : '禁用' }}
          </a-tag>
          <a-space v-if="column.key === 'actions' && store.isAdmin">
            <a-button size="small" @click="editUser(record)">编辑</a-button>
            <a-button size="small" @click="toggleUser(record)">{{ record.status === 1 ? '禁用' : '启用' }}</a-button>
            <a-button size="small" @click="resetPassword(record)">重置密码</a-button>
          </a-space>
        </template>
      </a-table>

      <a-divider />
      <div class="page-head">
        <h1 class="page-title">配置包</h1>
        <div class="toolbar">
          <a-button @click="exportPackage">导出配置包</a-button>
          <a-button v-if="store.isAdmin" type="primary" @click="importPackage">导入配置包</a-button>
        </div>
      </div>
      <a-alert
        type="info"
        message="配置包包含 Canal 全局运行配置、数据源和同步任务，适合环境迁移、备份恢复。导入后会刷新 Canal 运行配置。"
        show-icon
      />
      <a-textarea
        v-model:value="packageText"
        class="editor package-editor"
        :rows="12"
        placeholder="点击导出生成 JSON，或粘贴配置包 JSON 后导入"
      />

      <a-divider />
      <div class="page-head">
        <h1 class="page-title">配置版本</h1>
        <div class="toolbar">
          <a-select v-model:value="versionFilter.configType" style="width:160px" :options="versionTypeOptions" />
          <a-input v-model:value="versionFilter.refId" style="width:240px" placeholder="对象 ID" />
          <a-button @click="loadVersions">查询</a-button>
          <a-button @click="resetVersions">重置</a-button>
        </div>
      </div>
      <a-table :data-source="versions" :columns="versionColumns" row-key="id" :pagination="tablePagination(10)">
        <template #bodyCell="{ column, record }">
          <a-tag v-if="column.key === 'rollbackable'" :color="isRollbackable(record) ? 'green' : 'default'">
            {{ isRollbackable(record) ? '可回滚' : '仅查看' }}
          </a-tag>
          <a-button
            v-if="column.key === 'actions' && store.isAdmin && isRollbackable(record)"
            size="small"
            @click="rollbackVersion(record)"
          >
            回滚
          </a-button>
        </template>
        <template #expandedRowRender="{ record }">
          <pre class="code-block">{{ record.configContent }}</pre>
        </template>
      </a-table>
    </div>

    <a-modal v-model:open="userOpen" title="用户配置" @ok="submitUser">
      <a-form layout="vertical" :model="userForm">
        <a-form-item label="用户名"><a-input v-model:value="userForm.username" /></a-form-item>
        <a-form-item label="昵称"><a-input v-model:value="userForm.nickname" /></a-form-item>
        <a-form-item label="角色"><a-select v-model:value="userForm.roleCode" :options="roleOptions" /></a-form-item>
        <a-form-item label="密码"><a-input-password v-model:value="userForm.password" placeholder="新增默认 123456，编辑留空不修改" /></a-form-item>
        <a-form-item label="启用"><a-switch :checked="userForm.status === 1" @change="checked => userForm.status = checked ? 1 : 0" /></a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>

<script setup>
import { reactive, onMounted, ref } from 'vue'
import { message, Modal } from 'ant-design-vue'
import { systemApi } from '../../api'
import { useAppStore } from '../../stores/useAppStore'
import { tablePagination } from '../../utils/pagination'

const store = useAppStore()
const users = ref([])
const versions = ref([])
const packageText = ref('')
const versionFilter = ref({ configType: '', refId: '' })
const userOpen = ref(false)
const userForm = reactive(defaultUserForm())
const localStack = ref({})
const verifyResult = ref({})
const verifyId = ref(String(new Date().getHours()).padStart(2, '0') + String(new Date().getMinutes()).padStart(2, '0') + String(new Date().getSeconds()).padStart(2, '0'))
const localStackLoading = ref(false)
const verifyLoading = ref(false)
const actionLoading = ref('')

const columns = [
  { title: '用户', dataIndex: 'username' },
  { title: '昵称', dataIndex: 'nickname' },
  { title: '角色', dataIndex: 'roleCode' },
  { title: '状态', key: 'status', width: 100 },
  { title: '操作', key: 'actions', width: 260 }
]

const roleOptions = [
  { label: '管理员', value: 'ADMIN' },
  { label: '运维人员', value: 'OPERATOR' },
  { label: '只读查看', value: 'VIEWER' }
]

const versionColumns = [
  { title: '类型', dataIndex: 'configType', width: 140 },
  { title: '对象', dataIndex: 'refId' },
  { title: '版本', dataIndex: 'versionNo', width: 100 },
  { title: '回滚', key: 'rollbackable', width: 100 },
  { title: '时间', dataIndex: 'createTime', width: 210 },
  { title: '操作', key: 'actions', width: 90 }
]

const localStackColumns = [
  { title: '组件', dataIndex: 'name', width: 150 },
  { title: '地址', dataIndex: 'target', width: 240 },
  { title: '状态', key: 'status', width: 90 },
  { title: '说明', dataIndex: 'message' }
]

const versionTypeOptions = [
  { label: '全部类型', value: '' },
  { label: '数据源', value: 'datasource' },
  { label: '任务', value: 'task' },
  { label: '任务 Adapter', value: 'task-adapter' },
  { label: 'Canal Runtime', value: 'canal-runtime' }
]

async function loadVersions() {
  versions.value = await systemApi.configVersions(versionFilter.value)
}

async function loadLocalStack() {
  localStackLoading.value = true
  try {
    localStack.value = await systemApi.localStackStatus()
  } catch (error) {
    localStack.value = { ok: false, runningCount: 0, total: 0, services: [] }
  } finally {
    localStackLoading.value = false
  }
}

async function verifyLocalStack() {
  verifyLoading.value = true
  try {
    verifyResult.value = await systemApi.verifyLocalStack({ verifyId: verifyId.value })
    await loadLocalStack()
    if (verifyResult.value.ok) {
      message.success('本机链路验证通过')
    } else {
      message.error('本机链路验证失败')
    }
  } finally {
    verifyLoading.value = false
  }
}

async function runLocalStackAction(action) {
  actionLoading.value = action
  try {
    const api = {
      start: systemApi.startLocalStack,
      stop: systemApi.stopLocalStack,
      restart: systemApi.restartLocalStack
    }[action]
    verifyResult.value = await api()
    await loadLocalStack()
    message[verifyResult.value.ok ? 'success' : 'error'](`${verifyActionName(action)}${verifyResult.value.ok ? '成功' : '失败'}`)
  } finally {
    actionLoading.value = ''
  }
}

function verifyActionName(action) {
  return { verify: '验证', start: '启动', stop: '停止', restart: '重启' }[action] || action
}

function commandOutput(result) {
  return [result.stdout, result.stderr].filter(Boolean).join('\n\n') || '暂无输出'
}

async function resetVersions() {
  versionFilter.value = { configType: '', refId: '' }
  await loadVersions()
}

function isRollbackable(record) {
  const content = record?.configContent || ''
  return content.trim().startsWith('{') && ['datasource', 'task', 'canal-runtime'].includes(record.configType)
}

function rollbackVersion(record) {
  Modal.confirm({
    title: '回滚配置版本',
    content: `确认将 ${record.configType}/${record.refId} 回滚到 v${record.versionNo}？回滚后会刷新 Canal 运行配置。`,
    async onOk() {
      await systemApi.rollbackConfigVersion(record.id)
      await loadVersions()
      message.success('配置已回滚')
    }
  })
}

async function exportPackage() {
  const data = await systemApi.exportConfigPackage()
  packageText.value = JSON.stringify(data, null, 2)
  message.success('配置包已生成')
}

function importPackage() {
  Modal.confirm({
    title: '导入配置包',
    content: '导入会覆盖同名数据源和同 ID 任务，并刷新 Canal 运行配置。确认继续？',
    async onOk() {
      const data = JSON.parse(packageText.value)
      const result = await systemApi.importConfigPackage(data)
      await Promise.all([loadVersions(), store.refreshAll()])
      message.success(`导入完成：数据源 ${result.datasourceCount || 0} 个，任务 ${result.taskCount || 0} 个`)
    }
  })
}

function defaultUserForm() {
  return { id: null, username: '', nickname: '', password: '', roleCode: 'OPERATOR', status: 1 }
}

function assignUser(data) {
  Object.assign(userForm, defaultUserForm(), data, { password: '' })
}

function openUserCreate() {
  assignUser({})
  userOpen.value = true
}

function editUser(record) {
  assignUser(record)
  userOpen.value = true
}

async function submitUser() {
  await systemApi.saveUser(userForm)
  userOpen.value = false
  users.value = await systemApi.users()
  message.success('用户已保存')
}

async function toggleUser(record) {
  await systemApi.enableUser({ id: record.id, enabled: record.status !== 1 })
  users.value = await systemApi.users()
  message.success(record.status === 1 ? '用户已禁用' : '用户已启用')
}

function resetPassword(record) {
  Modal.confirm({
    title: '重置密码',
    content: `确认将「${record.username}」的密码重置为 123456？`,
    async onOk() {
      await systemApi.resetPassword({ id: record.id, password: '123456' })
      message.success('密码已重置为 123456')
    }
  })
}

onMounted(() => {
  Promise.allSettled([
    loadLocalStack(),
    systemApi.users().then(data => users.value = data),
    loadVersions()
  ])
})
</script>
