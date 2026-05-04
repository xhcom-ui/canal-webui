<template>
  <div class="field-mapping">
    <div class="toolbar field-mapping-toolbar">
      <a-button @click="add"><Plus :size="14" />添加字段</a-button>
      <a-button @click="copySourceToTarget" :disabled="!modelValue.length"><Copy :size="14" />源字段复制到目标</a-button>
      <a-button @click="setAllEnabled(true)" :disabled="!modelValue.length"><Eye :size="14" />全部启用</a-button>
      <a-button @click="setAllEnabled(false)" :disabled="!modelValue.length"><EyeOff :size="14" />全部禁用</a-button>
      <a-button @click="applyTargetTemplate" :disabled="!modelValue.length"><Settings2 :size="14" />{{ targetLabel }}模板</a-button>
      <a-button @click="normalizeRows" :disabled="!modelValue.length"><Wand2 :size="14" />整理属性</a-button>
      <a-button @click="exportMappings" :disabled="!modelValue.length"><Download :size="14" />导出</a-button>
      <a-button @click="openImport"><Upload :size="14" />导入</a-button>
      <span class="muted-path">启用 {{ enabledCount }}/{{ modelValue.length }}，主键 {{ primaryKeyCount }}，异常 {{ validationIssues.length }}</span>
    </div>

    <a-alert
      v-if="validationIssues.length"
      type="warning"
      show-icon
      class="field-mapping-alert"
      :message="validationIssues.slice(0, 3).join('；')"
    />

    <a-table
      :data-source="tableRows"
      :columns="columns"
      :pagination="false"
      row-key="__rowKey"
      size="small"
      :scroll="{ x: 1280 }"
    >
      <template #bodyCell="{ column, record, index }">
        <a-switch
          v-if="column.key === 'enabled'"
          :checked="record.enabled !== false"
          size="small"
          @change="value => updateRow(index, 'enabled', value)"
        />
        <a-input
          v-else-if="column.key === 'sourceField'"
          :value="record.sourceField"
          placeholder="源字段"
          :status="!record.sourceField ? 'error' : ''"
          @input="updateRow(index, 'sourceField', $event.target.value)"
        />
        <a-input
          v-else-if="column.key === 'targetField'"
          :value="record.targetField"
          placeholder="目标字段"
          :status="!record.targetField ? 'error' : ''"
          @input="updateRow(index, 'targetField', $event.target.value)"
        />
        <a-select
          v-else-if="column.key === 'fieldType'"
          :value="record.fieldType"
          placeholder="类型"
          allow-clear
          style="width:100%"
          :options="fieldTypeOptions"
          @change="value => updateRow(index, 'fieldType', value || '')"
        />
        <a-checkbox
          v-else-if="column.key === 'primaryKey'"
          :checked="record.primaryKey === true"
          @change="event => updatePrimaryKey(index, event.target.checked)"
        />
        <a-checkbox
          v-else-if="column.key === 'nullableField'"
          :checked="record.nullableField !== false"
          @change="event => updateRow(index, 'nullableField', event.target.checked)"
        />
        <a-input
          v-else-if="column.key === 'defaultValue'"
          :value="record.defaultValue"
          placeholder="默认值"
          @input="updateRow(index, 'defaultValue', $event.target.value)"
        />
        <a-input
          v-else-if="column.key === 'transformExpr'"
          :value="record.transformExpr"
          placeholder="trim(name)"
          @input="updateRow(index, 'transformExpr', $event.target.value)"
        />
        <a-input
          v-else-if="column.key === 'formatPattern'"
          :value="record.formatPattern"
          placeholder="yyyy-MM-dd HH:mm:ss"
          @input="updateRow(index, 'formatPattern', $event.target.value)"
        />
        <div v-else-if="column.key === 'fieldOptions'" class="field-options-cell">
          <a-tag :color="fieldOptionsValid(record.fieldOptions) ? 'blue' : 'red'">
            {{ fieldOptionsSummary(record.fieldOptions) }}
          </a-tag>
          <a-button size="small" @click="openOptions(index)"><SlidersHorizontal :size="14" />参数</a-button>
        </div>
        <a-space v-else-if="column.key === 'actions'">
          <a-tooltip title="上移">
            <a-button size="small" class="icon-button" :disabled="index === 0" aria-label="上移" @click="move(index, -1)"><ArrowUp :size="14" /></a-button>
          </a-tooltip>
          <a-tooltip title="下移">
            <a-button size="small" class="icon-button" :disabled="index === modelValue.length - 1" aria-label="下移" @click="move(index, 1)"><ArrowDown :size="14" /></a-button>
          </a-tooltip>
          <a-tooltip title="复制一行">
            <a-button size="small" class="icon-button" aria-label="复制映射" @click="duplicate(index)"><Copy :size="14" /></a-button>
          </a-tooltip>
          <a-tooltip title="删除映射">
            <a-button size="small" class="icon-button" danger aria-label="删除映射" @click="remove(index)"><Trash2 :size="14" /></a-button>
          </a-tooltip>
        </a-space>
      </template>
    </a-table>

    <a-modal
      v-model:open="optionsOpen"
      title="字段属性参数"
      width="720px"
      ok-text="保存参数"
      cancel-text="取消"
      @ok="saveOptions"
    >
      <a-form layout="vertical" :model="optionsForm">
        <div class="target-grid">
          <a-form-item label="MQ Message Key">
            <a-switch v-model:checked="optionsForm.messageKey" />
          </a-form-item>
          <a-form-item label="Redis Value 字段">
            <a-switch v-model:checked="optionsForm.redisValueField" />
          </a-form-item>
          <a-form-item label="HBase Column Family">
            <a-input v-model:value="optionsForm.hbaseFamily" placeholder="CF" />
          </a-form-item>
          <a-form-item label="ES 字段类型">
            <a-select v-model:value="optionsForm.esType" allow-clear :options="esTypeOptions" />
          </a-form-item>
          <a-form-item label="RDB 列类型">
            <a-input v-model:value="optionsForm.rdbType" placeholder="varchar(255)" />
          </a-form-item>
          <a-form-item label="脱敏策略">
            <a-select v-model:value="optionsForm.mask" allow-clear :options="maskOptions" />
          </a-form-item>
        </div>
        <a-form-item label="自定义 JSON">
          <a-textarea
            v-model:value="optionsRaw"
            class="editor"
            :rows="7"
            :status="optionsRaw && !jsonValid(optionsRaw) ? 'error' : ''"
            placeholder='{"messageKey":true,"hbaseFamily":"CF","redisValueField":true}'
          />
        </a-form-item>
      </a-form>
    </a-modal>

    <a-modal
      v-model:open="importOpen"
      title="导入字段映射"
      width="760px"
      ok-text="导入"
      cancel-text="取消"
      @ok="importMappings"
    >
      <a-alert type="info" show-icon message="支持导入字段映射数组 JSON；会覆盖当前字段映射。" class="field-mapping-alert" />
      <a-textarea
        v-model:value="importText"
        class="editor"
        :rows="12"
        :status="importText && !importTextValid ? 'error' : ''"
        placeholder='[{"sourceField":"id","targetField":"id","fieldType":"LONG","primaryKey":true}]'
      />
    </a-modal>
  </div>
</template>

<script setup>
import { computed, reactive, ref, watch } from 'vue'
import { ArrowDown, ArrowUp, Copy, Download, Eye, EyeOff, Plus, Settings2, SlidersHorizontal, Trash2, Upload, Wand2 } from 'lucide-vue-next'
import { normalizeTargetType } from '../utils/targetType'

const props = defineProps({
  modelValue: { type: Array, required: true },
  targetType: { type: String, default: 'LOGGER' }
})
const emit = defineEmits(['update:modelValue'])

const optionsOpen = ref(false)
const importOpen = ref(false)
const editingIndex = ref(-1)
const optionsRaw = ref('')
const importText = ref('')
const optionsForm = reactive({
  messageKey: false,
  redisValueField: false,
  hbaseFamily: '',
  esType: undefined,
  rdbType: '',
  mask: undefined
})

const columns = [
  { title: '启用', key: 'enabled', width: 70, fixed: 'left' },
  { title: '源字段', key: 'sourceField', width: 150, fixed: 'left' },
  { title: '目标字段', key: 'targetField', width: 150 },
  { title: '类型', key: 'fieldType', width: 130 },
  { title: '主键', key: 'primaryKey', width: 70 },
  { title: '可空', key: 'nullableField', width: 70 },
  { title: '默认值', key: 'defaultValue', width: 130 },
  { title: '转换表达式', key: 'transformExpr', width: 170 },
  { title: '格式化', key: 'formatPattern', width: 160 },
  { title: '属性参数', key: 'fieldOptions', width: 180 },
  { title: '操作', key: 'actions', width: 100, fixed: 'right' }
]

const fieldTypeOptions = ['STRING', 'INT', 'LONG', 'DECIMAL', 'DATE', 'DATETIME', 'BOOLEAN', 'JSON', 'BINARY']
  .map(value => ({ label: value, value }))
const esTypeOptions = ['keyword', 'text', 'long', 'integer', 'double', 'boolean', 'date', 'object']
  .map(value => ({ label: value, value }))
const maskOptions = [
  { label: '不脱敏', value: '' },
  { label: '手机号', value: 'PHONE' },
  { label: '邮箱', value: 'EMAIL' },
  { label: '身份证', value: 'ID_CARD' },
  { label: '固定掩码', value: 'FIXED' }
]

const tableRows = computed(() => props.modelValue.map((row, index) => ({ __rowKey: index, ...defaultRow(), ...row })))
const normalizedTargetType = computed(() => normalizeTargetType(props.targetType))
const targetLabel = computed(() => {
  const labels = { MYSQL: 'RDB', PGSQL: 'RDB', ES: 'ES', REDIS: 'Redis', HBASE: 'HBase', KAFKA: 'MQ', ROCKETMQ: 'MQ', RABBITMQ: 'MQ', PULSAR: 'MQ' }
  return labels[normalizedTargetType.value] || '通用'
})
const enabledCount = computed(() => props.modelValue.filter(row => row.enabled !== false).length)
const primaryKeyCount = computed(() => props.modelValue.filter(row => row.primaryKey === true && row.enabled !== false).length)
const importTextValid = computed(() => {
  try {
    return Array.isArray(JSON.parse(importText.value || '[]'))
  } catch {
    return false
  }
})
const validationIssues = computed(() => {
  const issues = []
  const sourceSeen = new Set()
  const targetSeen = new Set()
  props.modelValue.forEach((row, index) => {
    if (row.enabled === false) return
    const label = `第 ${index + 1} 行`
    if (!row.sourceField) issues.push(`${label} 缺少源字段`)
    if (!row.targetField) issues.push(`${label} 缺少目标字段`)
    if (row.sourceField && sourceSeen.has(row.sourceField)) issues.push(`源字段重复：${row.sourceField}`)
    if (row.targetField && targetSeen.has(row.targetField)) issues.push(`目标字段重复：${row.targetField}`)
    if (row.sourceField) sourceSeen.add(row.sourceField)
    if (row.targetField) targetSeen.add(row.targetField)
    if (row.fieldOptions && !jsonValid(row.fieldOptions)) issues.push(`${label} 属性参数不是合法 JSON`)
  })
  return issues
})

watch(optionsForm, () => {
  const merged = { ...parseJson(optionsRaw.value), ...compactOptions(optionsForm) }
  optionsRaw.value = Object.keys(merged).length ? JSON.stringify(merged, null, 2) : ''
}, { deep: true })

function emitRows(rows) {
  emit('update:modelValue', rows.map(row => ({ ...defaultRow(), ...row })))
}

function updateRow(index, key, value) {
  const rows = [...props.modelValue]
  rows[index] = { ...defaultRow(), ...rows[index], [key]: value }
  emitRows(rows)
}

function updatePrimaryKey(index, checked) {
  const rows = [...props.modelValue]
  rows[index] = { ...defaultRow(), ...rows[index], primaryKey: checked, nullableField: checked ? false : rows[index]?.nullableField !== false }
  emitRows(rows)
}

function add() {
  emitRows([...props.modelValue, defaultRow()])
}

function duplicate(index) {
  const rows = [...props.modelValue]
  rows.splice(index + 1, 0, { ...defaultRow(), ...rows[index], targetField: `${rows[index]?.targetField || rows[index]?.sourceField || 'field'}_copy`, primaryKey: false })
  emitRows(rows)
}

function move(index, direction) {
  const nextIndex = index + direction
  if (nextIndex < 0 || nextIndex >= props.modelValue.length) return
  const rows = [...props.modelValue]
  const [row] = rows.splice(index, 1)
  rows.splice(nextIndex, 0, row)
  emitRows(rows)
}

function remove(index) {
  emitRows(props.modelValue.filter((_, i) => i !== index))
}

function copySourceToTarget() {
  emitRows(props.modelValue.map(row => ({ ...defaultRow(), ...row, targetField: row.targetField || row.sourceField || '' })))
}

function setAllEnabled(enabled) {
  emitRows(props.modelValue.map(row => ({ ...defaultRow(), ...row, enabled })))
}

function normalizeRows() {
  emitRows(props.modelValue.map(row => {
    const next = { ...defaultRow(), ...row }
    next.sourceField = (next.sourceField || '').trim()
    next.targetField = (next.targetField || next.sourceField || '').trim()
    next.fieldType = normalizeType(next.fieldType || inferType(next.sourceField || next.targetField))
    next.primaryKey = next.primaryKey === true || isPrimaryKeyName(next.sourceField || next.targetField)
    next.nullableField = next.primaryKey ? false : next.nullableField !== false
    next.enabled = next.enabled !== false
    next.fieldOptions = formatJson(next.fieldOptions)
    return next
  }))
}

function applyTargetTemplate() {
  emitRows(props.modelValue.map(row => {
    const next = { ...defaultRow(), ...row }
    next.fieldType = normalizeType(next.fieldType || inferType(next.sourceField || next.targetField))
    next.primaryKey = next.primaryKey === true || isPrimaryKeyName(next.sourceField || next.targetField)
    next.nullableField = next.primaryKey ? false : next.nullableField !== false
    const options = { ...parseJson(next.fieldOptions), ...targetOptions(next) }
    next.fieldOptions = Object.keys(options).length ? JSON.stringify(options) : ''
    return next
  }))
}

function exportMappings() {
  importText.value = JSON.stringify(props.modelValue.map(row => ({ ...defaultRow(), ...row })), null, 2)
  importOpen.value = true
}

function openImport() {
  importText.value = ''
  importOpen.value = true
}

function importMappings() {
  const parsed = JSON.parse(importText.value || '[]')
  emitRows(parsed.map(row => ({ ...defaultRow(), ...row, fieldOptions: formatJson(row.fieldOptions) })))
  importOpen.value = false
}

function openOptions(index) {
  editingIndex.value = index
  const row = props.modelValue[index] || {}
  const parsed = parseJson(row.fieldOptions)
  optionsForm.messageKey = parsed.messageKey === true
  optionsForm.redisValueField = parsed.redisValueField === true
  optionsForm.hbaseFamily = parsed.hbaseFamily || ''
  optionsForm.esType = parsed.esType || undefined
  optionsForm.rdbType = parsed.rdbType || ''
  optionsForm.mask = parsed.mask || undefined
  optionsRaw.value = row.fieldOptions ? formatJson(row.fieldOptions) : ''
  optionsOpen.value = true
}

function saveOptions() {
  if (editingIndex.value < 0) return
  const parsed = parseJson(optionsRaw.value)
  const merged = { ...parsed, ...compactOptions(optionsForm) }
  updateRow(editingIndex.value, 'fieldOptions', Object.keys(merged).length ? JSON.stringify(merged) : '')
  optionsOpen.value = false
}

function defaultRow() {
  return {
    sourceField: '',
    targetField: '',
    fieldType: '',
    primaryKey: false,
    nullableField: true,
    enabled: true,
    defaultValue: '',
    transformExpr: '',
    formatPattern: '',
    fieldOptions: ''
  }
}

function normalizeType(value) {
  const text = (value || '').trim().toUpperCase()
  if (!text) return ''
  if (['VARCHAR', 'CHAR', 'TEXT'].includes(text)) return 'STRING'
  if (['BIGINT'].includes(text)) return 'LONG'
  if (['INTEGER', 'TINYINT', 'SMALLINT'].includes(text)) return 'INT'
  if (['TIMESTAMP'].includes(text)) return 'DATETIME'
  return text
}

function inferType(field) {
  const name = (field || '').toLowerCase()
  if (!name) return ''
  if (name === 'id' || name.endsWith('_id')) return 'LONG'
  if (name.includes('time') || name.endsWith('_at') || name.includes('date')) return 'DATETIME'
  if (name.includes('amount') || name.includes('price') || name.includes('rate')) return 'DECIMAL'
  if (name.includes('count') || name.includes('num') || name.includes('status')) return 'INT'
  if (name.startsWith('is_') || name.startsWith('has_')) return 'BOOLEAN'
  return 'STRING'
}

function isPrimaryKeyName(field) {
  const name = (field || '').toLowerCase()
  return name === 'id' || name.endsWith('_id')
}

function targetOptions(row) {
  const field = row.targetField || row.sourceField || ''
  const primary = row.primaryKey === true || isPrimaryKeyName(field)
  switch (normalizedTargetType.value) {
    case 'MYSQL':
    case 'PGSQL':
    case 'CLICKHOUSE':
      return { rdbType: defaultRdbType(row.fieldType, primary) }
    case 'ES':
      return { esType: defaultEsType(row.fieldType, primary) }
    case 'REDIS':
      return { redisValueField: !primary }
    case 'HBASE':
      return { hbaseFamily: 'CF' }
    case 'KAFKA':
    case 'ROCKETMQ':
    case 'RABBITMQ':
    case 'PULSAR':
      return primary ? { messageKey: true } : {}
    default:
      return {}
  }
}

function defaultRdbType(fieldType, primary) {
  const type = normalizeType(fieldType)
  if (primary) return 'bigint'
  if (type === 'INT') return 'int'
  if (type === 'LONG') return 'bigint'
  if (type === 'DECIMAL') return 'decimal(20,6)'
  if (type === 'DATE') return 'date'
  if (type === 'DATETIME') return 'datetime'
  if (type === 'BOOLEAN') return 'tinyint(1)'
  if (type === 'JSON') return 'text'
  if (type === 'BINARY') return 'blob'
  return 'varchar(255)'
}

function defaultEsType(fieldType, primary) {
  const type = normalizeType(fieldType)
  if (primary) return 'keyword'
  if (type === 'INT') return 'integer'
  if (type === 'LONG') return 'long'
  if (type === 'DECIMAL') return 'double'
  if (type === 'DATE' || type === 'DATETIME') return 'date'
  if (type === 'BOOLEAN') return 'boolean'
  if (type === 'JSON') return 'object'
  return 'keyword'
}

function fieldOptionsValid(value) {
  return !value || jsonValid(value)
}

function fieldOptionsSummary(value) {
  if (!value) return '无参数'
  const parsed = parseJson(value)
  const keys = Object.keys(parsed)
  return keys.length ? `${keys.length} 项参数` : 'JSON 异常'
}

function jsonValid(value) {
  if (!value) return true
  try {
    JSON.parse(value)
    return true
  } catch {
    return false
  }
}

function parseJson(value) {
  if (!value) return {}
  try {
    const parsed = JSON.parse(value)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

function formatJson(value) {
  if (!value) return ''
  const parsed = parseJson(value)
  return Object.keys(parsed).length ? JSON.stringify(parsed) : value
}

function compactOptions(value) {
  return Object.fromEntries(Object.entries({
    messageKey: value.messageKey || undefined,
    redisValueField: value.redisValueField || undefined,
    hbaseFamily: value.hbaseFamily || undefined,
    esType: value.esType || undefined,
    rdbType: value.rdbType || undefined,
    mask: value.mask || undefined
  }).filter(([, item]) => item !== undefined && item !== ''))
}
</script>
