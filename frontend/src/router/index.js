// frontend/src/router/index.js

import { createRouter, createWebHistory } from 'vue-router'
import { setupNavigationGuards } from './guards'

import authLayout from '@/layouts/authLayout.vue'
import adminLayout from '@/layouts/adminLayout.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    // Public routes
    { path: '/', name: 'landing', component: () => import('@/pages/landingPage.vue'), meta: { requiresAuth: false, requiresGuest: false, title: 'QuantPulse | Predictive Stock Analytics' } },

    // Auth routes
    {
      path: '/auth',
      component: authLayout,
      children: [
        { path: '', name: 'redirect', redirect: '/auth/login' },
        { path: 'login', name: 'login', component: () => import('@/pages/auth/loginPage.vue'), meta: { requiresAuth: false, requiresGuest: true, title: 'Login | QuantPulse' } }
      ],
      meta: { requiresAuth: false, requiresGuest: true, title: 'QuantPulse | Authentication' }
    },

    // Admin routes
    {
      path: '/admin',
      component: adminLayout,
      children: [
        { path: '', name: 'adminRedirect', redirect: '/admin/dashboard' },
        { path: 'dashboard', name: 'adminDashboard', component: () => import('@/pages/admin/dashboardPage.vue'), meta: { title: 'QuantPulse | Dashboard' } },
        { path: 'securities', name: 'adminSecurities', component: () => import('@/pages/admin/securitiesPage.vue'), meta: { title: 'QuantPulse | Securities' } },
        { path: 'securities/:id', name: 'adminSecurityDetails', component: () => import('@/pages/admin/securityDetailsPage.vue'), meta: { title: 'QuantPulse | Security Details' } },
        { path: 'tasks', name: 'adminTasks', component: () => import('@/pages/admin/taskspage.vue'), meta: { title: 'QuantPulse | Background Tasks' } }
      ],
      meta: { requiresAuth: true, requiresSuperuser: true, title: 'QuantPulse | Admin' }
    },

    // Catch-all 404 route
    { path: '/:pathMatch(.*)*', name: 'notFound', component: () => import('@/pages/notFoundPage.vue'), meta: { requiresAuth: false, title: 'QuantPulse | 404' } }
  ]
})

setupNavigationGuards(router)

export default router