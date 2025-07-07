import { defineStore } from "pinia";

const state = () => ({
    user: {},
    userProfile: {},
    token: null,
    tokenType: "Bearer",
    isLoading: false,
    expiresAt: null,
    error: null
})

const actions = {
    /**
     * Handles full login flow.
     * @param {Object} credentials
     * @returns {Object} - Generic Object
     */
    login: async function (credentials) {
        this.isLoading = true
        this.error = null

        try {
            const formData = new FormData()
            formData.append('username', credentials.email)
            formData.append('password', credentials.password)

            const response = await this.$http.post('/api/v1/auth/login', formData, { headers: { 'Content-Type': 'multipart/form-data' } })

            await this.fetchCurrentUserProfile()

            this.setAuthData({ token: response.data.access_token, tokenType: response.data.tokenType, expiresAt: response.data.expires_at, user: response.data.user })

            return { error: false, data: response.data }
        } catch (error) {
            const errorMessage = error.response?.data?.detail || error.message || 'Login failed'
            this.error = errorMessage
        } finally {
            this.isLoading = false
        }
    },

    /**
     * Fetches current user profiled details and stores in state.
     */
    fetchCurrentUserProfile: async function () {
        try {
            const response = await this.$http.get(`/api/v1/auth/profile`)
            this.userProfile = response.data.data
            return { error: false, data: response.data }
        } catch (error) {
            this.userProfile = {}
            return { error: true, data: error }
        }
    },

    /**
     * Handles logout flow.
     */
    logout: function () {
        this.clearAuthData()
        return { error: false }
    },

    /**
     * Sets auth data information in localStorage and store state.
     * @param {Object} data - Login API response
     */
    setAuthData: function ({ token, tokenType, expiresAt, user }) {
        this.token = token
        this.tokenType = tokenType || 'Bearer'
        this.expiresAt = expiresAt
        this.user = user
        this.error = null

        if (token) {
            localStorage.setItem('access_token', token)
            localStorage.setItem('token_type', tokenType || 'bearer')
            localStorage.setItem('expires_at', expiresAt || '')
            localStorage.setItem('user', JSON.stringify(user || {}))
        }
    },

    /**
     * Clears all stored auth information.
     */
    clearAuthData: function () {
        this.token = null
        this.tokenType = 'bearer'
        this.userProfile = {}
        this.expiresAt = null
        this.user = null
        this.error = null

        // Clear from localStorage
        localStorage.removeItem('access_token')
        localStorage.removeItem('token_type')
        localStorage.removeItem('expires_at')
        localStorage.removeItem('user')
    },

    /**
     * Clears errors.
     */
    clearError() {
        this.error = null
    }
}

const getters = {}

export const useAuthStore = defineStore('auth', { state, actions, getters })