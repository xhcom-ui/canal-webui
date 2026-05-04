<template>
  <LoginView v-if="!store.token" />
  <a-layout v-else class="app-shell">
    <a-layout-sider class="side" :width="230">
      <div class="brand"><Database :size="20" /> CanalSync Admin</div>
      <a-menu v-model:selectedKeys="selectedKeys" theme="dark" mode="inline" @click="onMenu">
        <a-menu-item key="dashboard"><BarChart3 :size="16" /> <span>监控大盘</span></a-menu-item>
        <a-menu-item key="datasource"><Server :size="16" /> <span>数据源管理</span></a-menu-item>
        <a-menu-item key="task"><Workflow :size="16" /> <span>同步任务</span></a-menu-item>
        <a-menu-item key="test-sim"><PlugZap :size="16" /> <span>测试模拟</span></a-menu-item>
        <a-menu-item key="canal"><Cable :size="16" /> <span>Canal 运维</span></a-menu-item>
        <a-menu-item key="canal-admin"><PanelTop :size="16" /> <span>Canal Admin</span></a-menu-item>
        <a-menu-item key="log"><ScrollText :size="16" /> <span>日志中心</span></a-menu-item>
        <a-menu-item key="system" v-if="store.isAdmin"><Shield :size="16" /> <span>系统管理</span></a-menu-item>
      </a-menu>
    </a-layout-sider>
    <a-layout>
      <a-layout-header style="background:#fff;border-bottom:1px solid #e5e7eb;padding:0 20px">
        <div class="toolbar" style="height:56px;justify-content:space-between">
          <span>{{ titles[current] }}</span>
          <a-space>
            <span>{{ store.user?.nickname }}</span>
            <a-button @click="refreshAll"><RefreshCw :size="15" />刷新</a-button>
            <a-button @click="store.logout"><LogOut :size="15" />退出</a-button>
          </a-space>
        </div>
      </a-layout-header>
      <a-layout-content class="content">
        <DashboardView v-if="current === 'dashboard'" />
        <DatasourceView v-else-if="current === 'datasource'" />
        <SyncTaskView v-else-if="current === 'task'" />
        <TestSimView v-else-if="current === 'test-sim'" />
        <CanalView v-else-if="current === 'canal'" />
        <CanalAdminFrameView v-else-if="current === 'canal-admin'" />
        <LogView v-else-if="current === 'log'" />
        <SystemView v-else-if="store.isAdmin" />
        <DashboardView v-else />
      </a-layout-content>
    </a-layout>
  </a-layout>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { BarChart3, Cable, Database, LogOut, PanelTop, PlugZap, RefreshCw, ScrollText, Server, Shield, Workflow } from 'lucide-vue-next'
import DashboardView from './views/dashboard/DashboardView.vue'
import DatasourceView from './views/datasource/DatasourceView.vue'
import SyncTaskView from './views/sync-task/SyncTaskView.vue'
import TestSimView from './views/test-sim/TestSimView.vue'
import CanalView from './views/canal/CanalView.vue'
import CanalAdminFrameView from './views/canal-admin/CanalAdminFrameView.vue'
import LogView from './views/log/LogView.vue'
import SystemView from './views/system/SystemView.vue'
import LoginView from './views/LoginView.vue'
import { useAppStore } from './stores/useAppStore'

const store = useAppStore()
const defaultPage = 'dashboard'
const pagePaths = {
  dashboard: '/',
  datasource: '/datasource',
  task: '/task',
  'test-sim': '/test-sim',
  canal: '/canal',
  'canal-admin': '/canal-admin',
  log: '/log',
  system: '/system'
}
const pathPages = Object.fromEntries(Object.entries(pagePaths).map(([key, path]) => [path, key]))
const current = ref(pageFromLocation())
const selectedKeys = ref([current.value])
const titles = {
  dashboard: '实时任务状态与运行概览',
  datasource: 'MySQL 源库与 Canal 实例配置',
  task: '同步任务配置与生命周期管理',
  'test-sim': '本机目标端测试模拟',
  canal: 'Canal 运行时、配置、日志与能力扩展',
  'canal-admin': '嵌入式 Canal Admin 控制台',
  log: '任务执行日志与异常记录',
  system: '权限与运行参数'
}

function onMenu({ key }) {
  if (key === 'system' && !store.isAdmin) {
    setCurrent(defaultPage)
    return
  }
  setCurrent(key, true)
}

watch(() => store.isAdmin, isAdmin => {
  if (!isAdmin && current.value === 'system') {
    setCurrent(defaultPage, true)
  }
})

watch(() => store.token, token => {
  if (token && current.value === 'system' && !store.isAdmin) {
    setCurrent(defaultPage, true)
  }
})

function handleLoginExpired() {
  store.clearSession()
}

function pageFromLocation() {
  return pathPages[window.location.pathname] || defaultPage
}

function setCurrent(key, updateUrl = false) {
  const next = pagePaths[key] ? key : defaultPage
  current.value = next
  selectedKeys.value = [next]
  if (updateUrl) {
    const nextPath = pagePaths[next]
    if (window.location.pathname !== nextPath) {
      window.history.pushState({}, '', nextPath)
    }
  }
}

function handlePopstate() {
  const next = pageFromLocation()
  if (next === 'system' && !store.isAdmin) {
    setCurrent(defaultPage, true)
    return
  }
  setCurrent(next)
}

function refreshAll() {
  store.refreshAll().catch(() => {})
}

onMounted(() => {
  window.addEventListener('canal-web:login-expired', handleLoginExpired)
  window.addEventListener('popstate', handlePopstate)
  if (store.token) {
    refreshAll()
  }
})

onBeforeUnmount(() => {
  window.removeEventListener('canal-web:login-expired', handleLoginExpired)
  window.removeEventListener('popstate', handlePopstate)
})
</script>
