import { createRouter, createWebHistory } from 'vue-router'
import authLayout from '@/layouts/authLayout.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'auth',
      component: authLayout,
      meta: { requiresAuth: false, title: 'QuantPulse - Predictive Stock Analytics' }
    }
  ]
})

export default router
