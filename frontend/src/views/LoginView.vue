<template>
  <div class="login-page">
    <div class="login-panel">
      <div class="login-title">
        <Database :size="24" />
        <span>CanalSync Admin</span>
      </div>
      <a-form layout="vertical" :model="form" @finish="submit">
        <a-form-item label="用户名" name="username">
          <a-input v-model:value="form.username" autocomplete="username" />
        </a-form-item>
        <a-form-item label="密码" name="password">
          <a-input-password v-model:value="form.password" autocomplete="current-password" />
        </a-form-item>
        <a-button type="primary" html-type="submit" block :loading="loading">
          <LogIn :size="15" />登录
        </a-button>
      </a-form>
    </div>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue'
import { Database, LogIn } from 'lucide-vue-next'
import { useAppStore } from '../stores/useAppStore'

const store = useAppStore()
const loading = ref(false)
const form = reactive({ username: 'admin', password: 'admin123' })

async function submit() {
  loading.value = true
  try {
    await store.login(form)
  } finally {
    loading.value = false
  }
}
</script>
