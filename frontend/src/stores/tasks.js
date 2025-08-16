import { defineStore } from "pinia"

const state = () => ({
    tasks: [],
    filters: {
        task_name: null,
        status: null,
        user_id: null,
        created_after: null,
        created_before: null,
        sort_by: null,
        sort_order: 'desc'
    },
    pagination: { skip: 0, limit: 25 },
    totalRecords: 0,
    isLoading: false,
    selectedTask: null,
    taskStats: null
})

const actions = {
    /**
     * Fetch paginated tasks with filters
     * @param {Object} params - Additional parameters
     */
    fetchTasks: async function (params = {}) {
        this.isLoading = true
        try {
            const queryParams = { skip: this.pagination.skip, limit: this.pagination.limit, ...this.getActiveFilters(), ...params }
            const response = await this.$http.get('/api/v1/tasks', { params: queryParams })
            this.tasks = response.data.data
            this.totalRecords = response.data.pagination.total
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        } finally {
            this.isLoading = false
        }
    },

    /**
     * Fetch task overview statistics.
     * @returns {Object} Generic Object
     */
    fetchTaskStatistics: async function () {
        try {
            const response = await this.$http.get('/api/v1/tasks/stats/overview')
            return { error: false, data: response }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Fetch detailed task information including steps and logs
     * @param {String} taskId 
     */
    fetchTaskDetails: async function (taskId) {
        try {
            const response = await this.$http.get(`/api/v1/tasks/${taskId}`)
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Fetch task logs with optional filtering
     * @param {String} taskId 
     * @param {Object} params - Pagination and filter params
     */
    fetchTaskLogs: async function (taskId, params = {}) {
        try {
            const queryParams = { skip: 0, limit: 100, ...params }
            const response = await this.$http.get(`/api/v1/tasks/${taskId}/logs`, { params: queryParams })
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Fetch task execution steps
     * @param {String} taskId 
     */
    fetchTaskSteps: async function (taskId) {
        try {
            const response = await this.$http.get(`/api/v1/tasks/${taskId}/steps`)
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Retry a failed task
     * @param {String} taskId 
     * @param {Object} retryData - Retry request data
     */
    retryTaskAction: async function (taskId, retryData = {}) {
        try {
            const response = await this.$http.post(`/api/v1/tasks/${taskId}/retry`, retryData)
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Cancel a running task
     * @param {String} taskId 
     * @param {Object} cancelData - Cancel request data
     */
    cancelTaskAction: async function (taskId, cancelData = {}) {
        try {
            const response = await this.$http.post(`/api/v1/tasks/${taskId}/cancel`, cancelData)
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Delete a task
     * @param {String} taskId 
     * @param {Boolean} force - Force delete even if running
     */
    deleteTaskAction: async function (taskId, force = false) {
        try {
            const params = force ? { force: true } : {}
            const response = await this.$http.delete(`/api/v1/tasks/${taskId}`, { params })
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Fetch task statistics overview
     */
    fetchTaskStats: async function () {
        try {
            const response = await this.$http.get('/api/v1/tasks/stats/overview')
            this.taskStats = response.data.data
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },
    /**
     * Bulk action on multiple tasks
     * @param {Object} bulkActionData - Bulk action request
     */
    bulkTaskAction: async function (bulkActionData) {
        try {
            const response = await this.$http.post('/api/v1/tasks/bulk-action', bulkActionData)
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Cleanup old completed tasks
     * @param {Number} days - Age threshold in days
     * @param {Boolean} dryRun - Preview mode
     */
    cleanupOldTasks: async function (days = 30, dryRun = true) {
        try {
            const response = await this.$http.post('/api/v1/tasks/cleanup', { days, dry_run: dryRun })
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Get active filters for API requests
     */
    getActiveFilters: function () {
        const activeFilters = {}

        if (this.filters.task_name) {
            activeFilters.task_name = this.filters.task_name
        }

        if (this.filters.status) {
            activeFilters.status = this.filters.status
        }

        if (this.filters.task_type) {
            activeFilters.task_type = this.filters.task_type
        }

        if (this.filters.user_id) {
            activeFilters.user_id = this.filters.user_id
        }

        if (this.filters.created_after) {
            activeFilters.created_after = this.filters.created_after.toISOString()
        }

        if (this.filters.created_before) {
            activeFilters.created_before = this.filters.created_before.toISOString()
        }

        if (this.filters.sort_by) {
            activeFilters.sort_by = this.filters.sort_by
            activeFilters.sort_order = this.filters.sort_order
        }

        return activeFilters
    },

    // <----- Mutations ----->

    /**
     * Set value for a property using path
     * @param {String} path 
     * @param {*} value 
     */
    setValue: function (path, value) {
        this.$lodash.set(this, path, value)
    },

    /**
     * Set pagination values
     * @param {Number} skip 
     * @param {Number} limit 
     */
    setPagination: function (skip, limit) {
        this.pagination = { skip, limit }
    },

    /**
     * Set filters
     * @param {Object} newFilters 
     */
    setFilters: function (newFilters) {
        this.filters = { ...this.filters, ...newFilters }
    },

    /**
     * Set sort parameters
     * @param {String} field 
     * @param {Number} order - 1 for asc, -1 for desc
     */
    setSort: function (field, order) {
        this.filters.sort_by = field
        this.filters.sort_order = order === 1 ? 'asc' : 'desc'
    },

    /**
     * Clear all filters
     */
    clearFilters: function () {
        this.filters = { task_name: null, status: null, task_type: null, user_id: null, created_after: null, created_before: null, sort_by: 'created_at', sort_order: 'desc' }
    },

    /**
     * Set selected task
     * @param {Object} task 
     */
    setSelectedTask: function (task) {
        this.selectedTask = task
    },

    /**
     * Update task in the tasks array
     * @param {String} taskId 
     * @param {Object} updatedData 
     */
    updateTask: function (taskId, updatedData) {
        const taskIndex = this.tasks.findIndex(task => task.id === taskId)
        if (taskIndex !== -1) {
            this.tasks[taskIndex] = { ...this.tasks[taskIndex], ...updatedData }
        }
    },

    /**
     * Remove task from the tasks array
     * @param {String} taskId 
     */
    removeTask: function (taskId) {
        this.tasks = this.tasks.filter(task => task.id !== taskId)
        this.totalRecords = Math.max(0, this.totalRecords - 1)
    },

    /**
     * Reset state to initial values
     */
    resetState: function () {
        Object.assign(this, state())
    }
}

const getters = {
    /**
     * Check if any filters are active
     */
    hasActiveFilters: (state) => {
        return !!(
            state.filters.task_name ||
            state.filters.status ||
            state.filters.task_type ||
            state.filters.user_id ||
            state.filters.created_after ||
            state.filters.created_before
        )
    },

    /**
     * Get running tasks from current tasks list
     */
    runningTasks: (state) => {
        return state.tasks.filter(task => ['PENDING', 'RECEIVED', 'STARTED', 'PROGRESS'].includes(task.status))
    },

    /**
     * Get completed tasks from current tasks list
     */
    completedTasks: (state) => {
        return state.tasks.filter(task => ['SUCCESS', 'FAILURE', 'CANCELLED', 'REVOKED'].includes(task.status))
    },

    /**
     * Get failed tasks from current tasks list
     */
    failedTasks: (state) => {
        return state.tasks.filter(task => task.status === 'FAILURE')
    },

    /**
     * Get tasks by type
     */
    tasksByType: (state) => {
        return (taskType) => { return state.tasks.filter(task => task.task_type === taskType) }
    },

    /**
     * Get task count by status
     */
    taskCountByStatus: (state) => {
        const counts = {}
        state.tasks.forEach(task => { counts[task.status] = (counts[task.status] || 0) + 1 })
        return counts
    },

    /**
     * Get recent tasks (last 24 hours)
     */
    recentTasks: (state) => {
        const yesterday = new Date()
        yesterday.setDate(yesterday.getDate() - 1)

        return state.tasks.filter(task => new Date(task.created_at) >= yesterday)
    },

    /**
     * Get average execution time for completed tasks
     */
    averageExecutionTime: (state) => {
        const completedTasks = state.tasks.filter(task => task.status === 'SUCCESS' && task.execution_time_seconds)

        if (completedTasks.length === 0) return 0

        const totalTime = completedTasks.reduce((sum, task) => sum + task.execution_time_seconds, 0)

        return Math.round(totalTime / completedTasks.length)
    },

    /**
     * Get success rate percentage
     */
    successRate: (state) => {
        const completedTasks = state.tasks.filter(task => ['SUCCESS', 'FAILURE'].includes(task.status))

        if (completedTasks.length === 0) return 0

        const successfulTasks = completedTasks.filter(task => task.status === 'SUCCESS')

        return Math.round((successfulTasks.length / completedTasks.length) * 100)
    }
}

export const useTasksStore = defineStore('tasks', { state, actions, getters })