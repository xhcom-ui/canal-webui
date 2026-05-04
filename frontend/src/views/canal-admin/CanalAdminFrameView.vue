<template>
  <div class="canal-admin-frame-page">
    <div class="page-head">
      <h1 class="page-title">Canal Admin</h1>
      <div class="toolbar">
        <a-input v-model:value="adminUrl" class="canal-admin-url" placeholder="http://127.0.0.1:8089" />
        <a-input v-model:value="loginForm.username" class="canal-admin-credential" placeholder="账号" />
        <a-input-password v-model:value="loginForm.password" class="canal-admin-credential" placeholder="密码" />
        <a-button type="primary" :loading="loginLoading" @click="loginAdmin">
          <LogIn :size="15" />登录
        </a-button>
        <a-button v-if="store.isAdmin" :loading="passwordLoading" @click="passwordOpen = true">
          <KeyRound :size="15" />修改密码
        </a-button>
        <a-button :loading="loading" @click="loadSetting">
          <RefreshCw :size="15" />读取配置
        </a-button>
        <a-button @click="reloadFrame">
          <RotateCw :size="15" />刷新页面
        </a-button>
        <a-button @click="openAdmin">
          <ExternalLink :size="15" />新窗口打开
        </a-button>
      </div>
    </div>

    <div class="panel canal-admin-frame-panel">
      <a-alert
        type="info"
        :message="`当前嵌入地址：${frameUrl}`"
        show-icon
        class="canal-admin-frame-alert"
      />
      <iframe
        :key="frameKey"
        class="canal-admin-frame"
        :src="frameSrc"
        title="Canal Admin"
      />
    </div>

    <a-modal
      v-model:open="passwordOpen"
      title="修改 Canal Admin 密码"
      ok-text="修改并登录"
      cancel-text="取消"
      :confirm-loading="passwordLoading"
      @ok="updatePassword"
    >
      <a-form layout="vertical" :model="passwordForm">
        <a-form-item label="账号">
          <a-input v-model:value="passwordForm.username" />
        </a-form-item>
        <a-form-item label="旧密码">
          <a-input-password v-model:value="passwordForm.oldPassword" />
        </a-form-item>
        <a-form-item label="新密码">
          <a-input-password v-model:value="passwordForm.newPassword" />
        </a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import { message } from 'ant-design-vue'
import { ExternalLink, KeyRound, LogIn, RefreshCw, RotateCw } from 'lucide-vue-next'
import { systemApi } from '../../api'
import { useAppStore } from '../../stores/useAppStore'

const store = useAppStore()
const adminUrl = ref('http://127.0.0.1:8089')
const frameKey = ref(0)
const frameSrc = ref('http://127.0.0.1:8089')
const loading = ref(false)
const loginLoading = ref(false)
const passwordLoading = ref(false)
const passwordOpen = ref(false)
const loginForm = ref({
  username: 'admin',
  password: 'admin123'
})
const passwordForm = ref({
  username: 'admin',
  oldPassword: '123456',
  newPassword: 'admin123'
})

const frameUrl = computed(() => normalizeUrl(adminUrl.value))

function normalizeUrl(value) {
  const raw = (value || '').trim() || '127.0.0.1:8089'
  if (/^https?:\/\//i.test(raw)) {
    return raw
  }
  return `http://${raw}`
}

async function loadSetting() {
  loading.value = true
  try {
    const setting = await systemApi.canalSetting()
    adminUrl.value = normalizeUrl(setting?.adminManager || '127.0.0.1:8089')
    reloadFrame()
  } finally {
    loading.value = false
  }
}

function reloadFrame() {
  frameSrc.value = withReloadToken(frameUrl.value)
  frameKey.value += 1
}

async function loginAdmin() {
  loginLoading.value = true
  try {
    clearAdminToken()
    const result = await systemApi.loginCanalAdmin({
      adminUrl: frameUrl.value,
      username: loginForm.value.username,
      password: loginForm.value.password
    })
    if (!result?.token) {
      message.warning('登录接口未返回 token')
      return
    }
    setAdminToken(result.token)
    frameSrc.value = withReloadToken(frameUrl.value)
    reloadFrame()
    message.success(result.needPasswordChange === 'true' ? '登录成功，Canal Admin 提示需要修改默认密码' : 'Canal Admin 登录成功')
  } finally {
    loginLoading.value = false
  }
}

async function updatePassword() {
  passwordLoading.value = true
  try {
    const result = await systemApi.updateCanalAdminPassword({
      adminUrl: frameUrl.value,
      username: passwordForm.value.username,
      oldPassword: passwordForm.value.oldPassword,
      newPassword: passwordForm.value.newPassword
    })
    if (result?.token) {
      clearAdminToken()
      setAdminToken(result.token)
    }
    loginForm.value.username = passwordForm.value.username
    loginForm.value.password = passwordForm.value.newPassword
    passwordOpen.value = false
    reloadFrame()
    message.success('Canal Admin 密码已修改')
  } finally {
    passwordLoading.value = false
  }
}

function openAdmin() {
  window.open(frameUrl.value, '_blank', 'noopener,noreferrer')
}

function withReloadToken(url) {
  const separator = url.includes('?') ? '&' : '?'
  return `${url}${separator}_t=${Date.now()}`
}

function clearAdminToken() {
  document.cookie = 'canal_admin_token=; path=/; max-age=0'
}

function setAdminToken(token) {
  document.cookie = `canal_admin_token=${encodeURIComponent(token)}; path=/; max-age=86400`
}

onMounted(async () => {
  frameSrc.value = frameUrl.value
  try {
    await loadSetting()
  } catch (error) {
    message.warning('读取 Canal Admin 地址失败，已使用默认地址')
  }
})
</script>
