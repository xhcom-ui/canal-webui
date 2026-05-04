import axios from 'axios'
import { message } from 'ant-design-vue'

const http = axios.create({
  baseURL: import.meta.env.VITE_API_BASE || '/api',
  timeout: 20000
})

let loginExpiredNotified = false

function notifyLoginExpired(messageText = '未登录或登录已过期') {
  localStorage.removeItem('canal-web-token')
  localStorage.removeItem('canal-web-user')
  window.dispatchEvent(new CustomEvent('canal-web:login-expired'))
  if (!loginExpiredNotified) {
    loginExpiredNotified = true
    message.warning(messageText)
    window.setTimeout(() => {
      loginExpiredNotified = false
    }, 1500)
  }
}

function markLoginExpired(error) {
  error.__loginExpired = true
  return error
}

http.interceptors.request.use(config => {
  const token = localStorage.getItem('canal-web-token')
  if (token) {
    config.headers.Authorization = token
  }
  return config
})

http.interceptors.response.use(
  response => {
    const body = response.data
    if (body && body.code !== 0) {
      if (body.message === '未登录或登录已过期') {
        notifyLoginExpired(body.message)
        return Promise.reject(markLoginExpired(new Error(body.message || '请求失败')))
      } else {
        message.error(body.message || '请求失败')
      }
      return Promise.reject(new Error(body.message || '请求失败'))
    }
    return body?.data
  },
  error => {
    const messageText = error.response?.data?.message || error.message || '网络异常'
    if (error.response?.status === 401 || messageText === '未登录或登录已过期') {
      notifyLoginExpired(messageText)
      return Promise.reject(markLoginExpired(error))
    } else {
      message.error(messageText)
    }
    return Promise.reject(error)
  }
)

export default http
