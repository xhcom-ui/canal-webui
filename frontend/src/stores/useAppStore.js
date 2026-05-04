import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { authApi, dashboardApi, datasourceApi, taskApi } from '../api'

export const useAppStore = defineStore('app', () => {
  const stats = ref({})
  const dashboardOverview = ref({})
  const datasources = ref([])
  const tasks = ref([])
  const user = ref(JSON.parse(localStorage.getItem('canal-web-user') || 'null'))
  const token = ref(localStorage.getItem('canal-web-token') || '')
  const roleCode = computed(() => user.value?.roleCode || 'VIEWER')
  const isAdmin = computed(() => roleCode.value === 'ADMIN')
  const isViewer = computed(() => roleCode.value === 'VIEWER')
  const canOperate = computed(() => roleCode.value === 'ADMIN' || roleCode.value === 'OPERATOR')

  async function login(form) {
    const result = await authApi.login(form)
    token.value = result.tokenValue
    user.value = result.user
    localStorage.setItem('canal-web-token', result.tokenValue)
    localStorage.setItem('canal-web-user', JSON.stringify(result.user))
    await refreshAll()
  }

  async function logout() {
    try {
      await authApi.logout()
    } finally {
      clearSession()
    }
  }

  function clearSession() {
    token.value = ''
    user.value = null
    localStorage.removeItem('canal-web-token')
    localStorage.removeItem('canal-web-user')
  }

  async function refreshStats() {
    stats.value = await dashboardApi.stats()
  }

  async function refreshDashboard() {
    dashboardOverview.value = await dashboardApi.overview()
    stats.value = dashboardOverview.value?.stats || {}
  }

  async function refreshDatasources() {
    datasources.value = await datasourceApi.list()
  }

  async function refreshTasks() {
    tasks.value = await taskApi.list()
  }

  async function refreshAll() {
    const results = await Promise.allSettled([refreshDashboard(), refreshDatasources(), refreshTasks()])
    const failed = results.find(result => result.status === 'rejected')
    if (failed?.reason?.__loginExpired) return
    if (failed) throw failed.reason
  }

  return {
    stats,
    dashboardOverview,
    datasources,
    tasks,
    user,
    token,
    roleCode,
    isAdmin,
    isViewer,
    canOperate,
    login,
    logout,
    clearSession,
    refreshStats,
    refreshDashboard,
    refreshDatasources,
    refreshTasks,
    refreshAll
  }
})
