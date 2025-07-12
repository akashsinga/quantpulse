import { defineStore } from "pinia";

const state = () => ({
    exchanges: [],
    filters: { search: null, exchange_id: null, security_type: null, segment: null, sector: null, is_active: null },
    pagination: { skip: 0, limit: 25 },
    securities: [],
    securityTypes: [
        { id: 'EQUITY' },
        { id: 'INDEX' },
        { id: 'FUTSTK' },
        { id: 'FUTIDX' },
        { id: 'FUTCOM' },
        { id: 'FUTCUR' },
        { id: 'OPTSTK' },
        { id: 'OPTIDX' },
        { id: 'OPTCOM' },
        { id: 'OPTCUR' }
    ],
    segments: [
        { id: 'EQUITY' },
        { id: 'DERIVATIVE' },
        { id: 'CURRENCY' },
        { id: 'COMMODITY' },
        { id: 'INDEX' }
    ],
    totalRecords: 0,
    sort: { field: null, order: null },
    stats: [
        { id: 'total', icon: 'ph ph-database', value: 0 },
        { id: 'active', icon: 'ph ph-check-circle', value: 0 },
        { id: 'futures', icon: 'ph ph-chart-line', value: 0 },
        { id: 'derivatives', icon: 'ph ph-lightning', value: 0 }
    ],
    isLoading: false,
    importStatus: {
        isImporting: false,
        taskId: null,
        progress: { percentage: 0, message: '', status: 'PENDING', result: null }
    }
})

const actions = {

    /**
     * Fetches all active exchanges.
     * @param {Object} params
     */
    fetchExchanges: async function (params) {
        try {
            const response = await this.$http.get(`/api/v1/exchanges`, { params: params })
            this.exchanges = response.data.data
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Fetch securities based on params.
     * @param {Object} params
     */
    fetchSecurities: async function (params = {}) {
        this.isLoading = true
        try {
            const queryParams = { skip: this.pagination.skip, limit: this.pagination.limit, ...this.getActiveFilters(), ...params }
            if (this.sort.field) {
                queryParams.sort_by = this.sort.field
                queryParams.sort_order = this.sort.order === 1 ? 'asc' : 'desc'
            }
            const response = await this.$http.get('/api/v1/securities', { params: queryParams })
            this.securities = response.data.data
            this.totalRecords = response.data.pagination.total
            return { error: false, data: response.data.data }
        } catch (error) {
            return { error: true, data: error }
        } finally {
            this.isLoading = false
        }
    },

    /**
     * Fetches the securities stats.
     * @returns {Object} Generic object
     */
    fetchSecuritiesStats: async function () {
        try {
            const response = await this.$http.get('/api/v1/securities/stats')
            this.$lodash.forEach(this.stats, (stat) => {
                stat.value = response.data.data[stat.id]
            })
            return { error: false, data: response.data }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    /**
     * Prepares active filters for API params.
     */
    getActiveFilters: function () {
        const activeFilters = {}

        if (this.filters.search) {
            activeFilters.q = this.filters.search
        }

        if (this.filters.exchange_id) {
            activeFilters.exchange_id = this.filters.exchange_id
        }

        if (this.filters.security_type) {
            activeFilters.security_type = this.filters.security_type
        }

        if (this.filters.segment) {
            activeFilters.segment = this.filters.segment
        }

        if (this.filters.sector) {
            activeFilters.sector = this.filters.sector
        }

        if (this.filters.is_active !== null) {
            activeFilters.is_active = this.filters.is_active
        }

        return activeFilters
    },

    /**
     * Clears filters.
     */
    clearFilters: function () {
        this.filters = { search: '', exchange_id: null, security_type: null, segment: null, sector: null, is_active: null }
    },

    /**
     * Starts Import.
     * @param {Object} options - Import Configuration
     * @returns {Object}
     */
    startImport: async function (options = {}) {
        this.importStatus.isImporting = true
        try {
            const response = await this.$http.post(`/api/v1/securities/import`, options)
            this.importStatus.taskId = response.data.data.task_id
            return { error: false, data: response.data.data }
        } catch (error) {
            this.importStatus.isImporting = false
            return { error: true, data: error }
        }
    },

    /**
     * Used by page polling to fetch import status for a taskId.
     * @param {String} taskId
     * @returns {Object}
     */
    getImportStatus: async function (taskId) {
        try {
            const response = await this.$http.get(`/api/v1/securities/import/status/${taskId}`)
            const status = response.data.data

            this.importStatus.progress = { percentage: status.progress_percentage || 0, message: status.message || '', status: status.status, result: status.result_data }

            if (status.status in ['SUCCESS', 'FAILURE']) {
                this.importStatus.isImporting = false
            }
            return { error: false, data: status }
        } catch (error) {
            return { error: true, data: error }
        }
    },

    // <----- Mutations ----->

    /**
     * Sets value for a property using path.
     * @param {String} path
     * @param {*} value
     */
    setValue: function (path, value) {
        this.$lodash.set(this, path, value)
    },

    /**
     * Sets pagination values.
     * @param {Number} skip
     * @param {Number} limit
     */
    setPagination: function (skip, limit) {
        this.pagination = { skip, limit }
    },

    /**
     * Sets Filters params.
     * @param {Object} newFilters
     */
    setFilters: function (newFilters) {
        this.filters = { ...this.filters, ...newFilters }
    },

    /**
     * Sets sort order
     * @param {String} field
     * @param {String} order
     */
    setSort: function (field, order) {
        this.sort.field = field
        this.sort.order = order
    },

    /**
     * Resets full state.
     */
    resetState: function () {
        Object.assign(this, state())
    }
}

const getters = {
    hasActiveFilters: (state) => {
        return !!(state.filters.search || state.filters.exchange_id || state.filters.security_type || state.filters.segment || state.filters.is_active !== null)
    }
}

export const useSecuritiesStore = defineStore('securities', { state, actions, getters })