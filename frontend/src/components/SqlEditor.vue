<template>
  <a-textarea class="editor" :value="value" :rows="7" @input="$emit('update:value', $event.target.value)" />
</template>

<script setup>
defineProps({ value: { type: String, default: '' } })
defineEmits(['update:value'])
</script>
