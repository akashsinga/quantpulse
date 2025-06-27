import { createRouter, createWebHistory } from 'vue-router'
import authLayout from '@/layouts/authLayout.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'landing',
      component: () => import('@/pages/landingPage.vue')
    },
    {
      path: '/auth',
      name: 'auth',
      component: authLayout,
      children: [
        { path: '', redirect: '/login' },
        { path: 'login', name: 'login', component: () => import('@/pages/auth/loginPage.vue'), meta: { requiresAuth: false, title: 'Login | QuantPulse' } }
      ],
      meta: { requiresAuth: false, title: 'QuantPulse - Predictive Stock Analytics' }
    }
  ]
})

export default router