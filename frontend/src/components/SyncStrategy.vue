<template>
  <div>
    <a-form-item label="同步模式">
      <a-radio-group
        class="sync-mode-group"
        :value="modelValue.syncMode"
        button-style="solid"
        @change="event => update('syncMode', event.target.value)"
      >
        <a-radio-button
          v-for="item in syncModeOptions"
          :key="item.value"
          :value="item.value"
          @click="update('syncMode', item.value)"
        >
          <div class="sync-mode-option">
            <span class="sync-mode-title">{{ item.label }}</span>
            <span class="sync-mode-desc">{{ item.description }}</span>
          </div>
        </a-radio-button>
      </a-radio-group>
    </a-form-item>

    <a-row :gutter="12">
      <a-col :span="8">
        <a-form-item label="批大小">
          <a-input-number :value="modelValue.batchSize" :min="1" :max="100000" style="width:100%" @change="value => update('batchSize', value)" />
        </a-form-item>
      </a-col>
      <a-col :span="8">
        <a-form-item label="全量执行方式">
          <a-radio-group
            :value="scheduleMode"
            button-style="solid"
            :disabled="!requiresFullSync"
            @change="event => updateScheduleMode(event.target.value)"
          >
            <a-radio-button value="ONCE">只执行一次</a-radio-button>
            <a-radio-button value="CRON">Cron 调度</a-radio-button>
          </a-radio-group>
        </a-form-item>
      </a-col>
      <a-col :span="8">
        <a-form-item label="Cron 全量调度">
          <a-input
            :value="modelValue.cronExpression"
            :disabled="!requiresFullSync || scheduleMode === 'ONCE'"
            placeholder="Spring Cron，例如 0 */5 * * * *"
            @input="update('cronExpression', $event.target.value)"
          />
        </a-form-item>
      </a-col>
    </a-row>
    <a-alert class="sync-mode-alert" :type="syncModeHelp.type" :message="syncModeHelp.message" show-icon />
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue'

const props = defineProps({ modelValue: { type: Object, required: true } })
const emit = defineEmits(['update:modelValue'])

const syncModeOptions = [
  { label: '增量', value: 'INCREMENTAL', description: 'Canal 读取 binlog，Adapter 持续写入目标端' },
  { label: '全量', value: 'FULL', description: '按 SQL 生成 JSONL 快照，支持手动和 Cron' },
  { label: '全量+增量', value: 'FULL_INCREMENTAL', description: '启动时先跑全量快照，再加载增量配置' }
]

const scheduleMode = ref(props.modelValue.cronExpression ? 'CRON' : 'ONCE')
const requiresFullSync = computed(() => ['FULL', 'FULL_INCREMENTAL'].includes(props.modelValue.syncMode))

const syncModeHelp = computed(() => {
  if (props.modelValue.syncMode === 'FULL') {
    return { type: 'info', message: 'FULL 只执行源库 SQL 快照，不挂载 Canal Adapter 增量配置；适合一次性校验、离线快照或定时全量。' }
  }
  if (props.modelValue.syncMode === 'FULL_INCREMENTAL') {
    return { type: 'warning', message: 'FULL_INCREMENTAL 会先执行一次全量快照，再启动增量链路；目标端需要自行处理全量快照落地或后续导入。' }
  }
  return { type: 'success', message: 'INCREMENTAL 只启动 Canal 增量链路，依赖 MySQL binlog 和 Adapter mapping。' }
})

watch(() => props.modelValue.cronExpression, value => {
  if (value && value.trim()) {
    scheduleMode.value = 'CRON'
  }
})

watch(() => props.modelValue.syncMode, value => {
  if (value === 'INCREMENTAL') {
    scheduleMode.value = 'ONCE'
  }
})

function update(key, value) {
  emit('update:modelValue', { ...props.modelValue, [key]: value })
}

function updateScheduleMode(value) {
  scheduleMode.value = value
  if (value === 'ONCE') {
    update('cronExpression', '')
  }
}
</script>
