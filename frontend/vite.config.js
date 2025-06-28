import { fileURLToPath, URL } from 'node:url'

import { defineConfig, loadEnv } from 'vite'
import vue from '@vitejs/plugin-vue'
import vueDevTools from 'vite-plugin-vue-devtools'
import Components from 'unplugin-vue-components/vite'
import { PrimeVueResolver } from '@primevue/auto-import-resolver'

// https://vite.dev/config/
export default ({ mode }) => {
  process.env = { ...process.env, ...loadEnv(mode, process.cwd()) }
  return defineConfig({
    plugins: [
      vue(),
      vueDevTools(),
      Components({
        resolvers: [
          PrimeVueResolver()
        ]
      })
    ],
    server: {
      port: 3000,
      host: '0.0.0.0',
      watch: {
        usePolling: true
      },
      proxy: {
        '/app': {
          target: process.env.VITE_QP_BACKEND_URL,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/app/, '')
        }
      }
    },
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url))
      },
    },
  })
}