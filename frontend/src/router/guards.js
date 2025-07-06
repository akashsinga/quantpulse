// frontend/src/router/guards.js

import { useAuthStore } from "@/stores/auth"

/**
 * Navigation Guards for Quantpulse application.
 * Handles authentication, authorization and route protection.
 */

export function setupNavigationGuards(router) {
    router.beforeEach(async (to, from, next) => {
        const authStore = useAuthStore()

        // Check if user has valid token in localStorage
        const token = localStorage.getItem('access_token')
        const storedUser = localStorage.getItem('user')

        // Restore auth state if token exists but store is empty
        if (token && !authStore.token) {
            authStore.setAuthData({ token, tokenType: localStorage.getItem('token_type') || 'Bearer', expiresAt: localStorage.getItem('expires_at'), user: storedUser ? JSON.parse(storedUser) : {} })

            // Fetching user profile data
            await authStore.fetchCurrentUserProfile()
        }

        // Route Requirements
        const requiresAuth = to.matched.some(record => record.meta.requiresAuth)
        const requiresSuperUser = to.matched.some(record => record.meta.requiresSuperUser)
        const requiresGuest = to.matched.some(record => record.meta.requiresGuest)

        // Authentication status
        const isAuthenticated = !!(authStore.token && authStore.userProfile?.id)
        const isSuperuser = authStore.userProfile?.is_superuser || false

        // Handle route scenarios

        if (requiresGuest && isAuthenticated) {
            // Redirect authenticated users away from guest only pages (like login)
            return next('/admin/dashboard')
        }

        if (requiresAuth && !isAuthenticated) {
            // Redirect unauthenticated users to login
            return next({ path: '/auth/login', query: { redirect: to.fullPath } })
        }

        if (requiresSuperUser && !isSuperuser) {
            // Redirect non-super users away from admin pages
            return next('/dashboard')
        }

        // Check for token expiration
        if (isAuthenticated && authStore.expiresAt) {
            const expiryTime = new Date(authStore.expiresAt).getTime()
            const currentTiem = new Date().getTime()

            if (currentTiem >= expiryTime) {
                // Token expired, clear auth and redirect to login
                authStore.clearAuthData()
                return next({ path: '/auth/login', query: { expired: 'true', redirect: to.fullPath } })
            }
        }

        // All checks passed, proceed to route
        next()
    })

    // Global after guard - Runs after every successful navigation
    router.afterEach((to, from) => {
        // Update page title
        document.title = to.meta.title || 'QuantPulse - Predictive Stock Analytics'
    })

    router.onError((error) => {
        // Handle specific error types

        if (error.message.includes('ChunkLoadError')) {
            window.location.reload()
        }
    })
}