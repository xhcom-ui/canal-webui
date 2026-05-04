import http from './http'

export const authApi = {
  login: data => http.post('/auth/login', data),
  userinfo: () => http.get('/auth/userinfo'),
  logout: () => http.post('/auth/logout')
}

export const dashboardApi = {
  stats: () => http.get('/dashboard/stats'),
  overview: () => http.get('/dashboard/overview')
}

export const datasourceApi = {
  list: () => http.get('/datasource/list'),
  save: data => http.post('/datasource/save', data),
  test: data => http.post('/datasource/test', data),
  binlogPosition: data => http.post('/datasource/binlog-position', data),
  clone: id => http.post(`/datasource/clone/${id}`),
  enable: data => http.post('/datasource/enable', data),
  tables: key => http.get(`/datasource/${key}/tables`),
  columns: (key, tableName) => http.get(`/datasource/${key}/columns`, { params: { tableName } }),
  diagnostics: key => http.get(`/datasource/${key}/diagnostics`)
}

export const taskApi = {
  list: status => http.get('/task/list', { params: status ? { status } : {} }),
  save: data => http.post('/task/save', data),
  targetTest: data => http.post('/task/target-test', data),
  sqlPreview: (data, limit = 20) => http.post('/task/sql-preview', data, { params: { limit } }),
  validateFieldMappings: data => http.post('/task/field-mapping/validate', data),
  clone: id => http.post(`/task/clone/${id}`),
  start: id => http.post(`/task/start/${id}`),
  stop: id => http.post(`/task/stop/${id}`),
  fullSync: id => http.post(`/task/full-sync/${id}`),
  fullSyncPreview: (id, limit = 20) => http.get(`/task/full-sync/preview/${id}`, { params: { limit } }),
  batchStart: ids => http.post('/task/batch/start', ids),
  batchStop: ids => http.post('/task/batch/stop', ids),
  remove: id => http.delete(`/task/${id}`),
  detail: id => http.get(`/task/detail/${id}`),
  preview: id => http.get(`/task/preview/${id}`),
  resourcePlan: id => http.get(`/task/resource-plan/${id}`),
  monitor: id => http.get(`/task/monitor/${id}`),
  runtime: id => http.get(`/task/runtime/${id}`),
  diagnostics: id => http.get(`/task/diagnostics/${id}`)
}

export const logApi = {
  task: (id, limit = 1000) => http.get(`/log/task/${id}`, { params: { limit } }),
  operation: params => http.get('/log/operation', { params }),
  alert: params => http.get('/log/alert', { params }),
  alertStats: () => http.get('/log/alert/stats'),
  ackAlert: id => http.post(`/log/alert/${id}/ack`),
  ackAlertBatch: ids => http.post('/log/alert/batch-ack', ids),
  ackAlertByFilter: params => http.post('/log/alert/ack-filter', params)
}

export const systemApi = {
  users: () => http.get('/system/user/list'),
  saveUser: data => http.post('/system/user/save', data),
  enableUser: data => http.post('/system/user/enable', data),
  resetPassword: data => http.post('/system/user/reset-password', data),
  configVersions: (params = {}) => http.get('/system/config/version/list', { params: { limit: 1000, ...params } }),
  rollbackConfigVersion: id => http.post(`/system/config/version/${id}/rollback`),
  exportConfigPackage: () => http.get('/system/config/package/export'),
  importConfigPackage: data => http.post('/system/config/package/import', data),
  localStackStatus: () => http.get('/system/local-stack/status', { timeout: 30000 }),
  verifyLocalStack: data => http.post('/system/local-stack/verify', data || {}, { timeout: 120000 }),
  startLocalStack: () => http.post('/system/local-stack/start', {}, { timeout: 60000 }),
  stopLocalStack: () => http.post('/system/local-stack/stop', {}, { timeout: 60000 }),
  restartLocalStack: () => http.post('/system/local-stack/restart', {}, { timeout: 90000 }),
  canalStatus: () => http.get('/system/canal/status'),
  canalConfig: () => http.get('/system/canal/config'),
  canalConfigConsistency: () => http.get('/system/canal/config/consistency', { timeout: 30000 }),
  canalStaleFiles: () => http.get('/system/canal/stale-files', { timeout: 30000 }),
  cleanCanalStaleFiles: () => http.post('/system/canal/stale-files/clean'),
  canalLogs: () => http.get('/system/canal/logs', { timeout: 30000 }),
  canalMetrics: () => http.get('/system/canal/metrics', { timeout: 30000 }),
  canalCapabilities: () => http.get('/system/canal/capabilities'),
  canalDiagnostics: () => http.get('/system/canal/diagnostics', { timeout: 30000 }),
  canalSetting: () => http.get('/system/canal/setting'),
  saveCanalSetting: data => http.post('/system/canal/setting', data),
  testCanalAdmin: data => http.post('/system/canal/admin/test', data),
  loginCanalAdmin: data => http.post('/system/canal/admin/login', data),
  updateCanalAdminPassword: data => http.post('/system/canal/admin/password', data),
  refreshCanal: () => http.post('/system/canal/refresh'),
  startCanal: () => http.post('/system/canal/start'),
  stopCanal: () => http.post('/system/canal/stop'),
  startCanalServer: () => http.post('/system/canal/server/start'),
  stopCanalServer: () => http.post('/system/canal/server/stop'),
  restartCanalServer: () => http.post('/system/canal/server/restart'),
  startCanalAdapter: () => http.post('/system/canal/adapter/start'),
  stopCanalAdapter: () => http.post('/system/canal/adapter/stop'),
  restartCanalAdapter: () => http.post('/system/canal/adapter/restart')
}
