<template>
    <div class="securities-page">
        <div class="page-header">
            <div class="header-content">
                <div class="header-left">
                    <div class="page-title">{{ securitiesI18n.securitiesManagement }}</div>
                    <div class="page-subtitle">{{ securitiesI18n.securitiesManagementSubtitle }}</div>
                </div>
                <div class="header-right">
                    <Button class="import-btn" icon="ph ph-download" :label="securitiesI18n.importSecurities" size="small" severity="secondary"></Button>
                    <Button icon="ph ph-arrow-clockwise" :label="$tm('common.refresh')" size="small"></Button>
                </div>
            </div>
        </div>

        <!-- Stat Cards -->
        <div class="stats-grid">
            <div class="stat-card" v-for="stat in stats" :key="stat.id">
                <div :class="['stat-icon', stat.id]">
                    <i :class="stat.icon"></i>
                </div>
                <div class="stat-content">
                    <div class="stat-value">{{ formatNumber(stat.value) }}</div>
                    <div class="stat-label">{{ stat.title }}</div>
                </div>
            </div>
        </div>

        <!-- Filters and Search -->
        <div class="filters-card">
            <div class="filters-content">
                <div class="search-section">
                    <IconField iconPosition="left" class="search-field">
                        <InputIcon class="ph ph-magnifying-glass"></InputIcon>
                        <InputText class="search-input" v-model="filters.search" :placeholder="securitiesI18n.searchPlaceholder" size="small"></InputText>
                    </IconField>
                    <Button v-if="hasActiveFilters" text :label="$tm('common.clear')" size="small" icon="ph ph-x"></Button>
                </div>

                <!-- Filter Groups -->
                <div class="filter-controls">
                    <div class="filter-group">
                        <label>{{ securitiesI18n.filters.exchange }}</label>
                        <Select class="filter-dropdown" v-model="filters.exchange_id" :placeholder="securitiesI18n.filters.placeholders.exchange" :options="exchanges" optionLabel="name" optionValue="id" size="small"></Select>
                    </div>
                    <div class="filter-group">
                        <label>{{ securitiesI18n.filters.securityType }}</label>
                        <Select class="filter-dropdown" v-model="filters.security_type" :placeholder="securitiesI18n.filters.placeholders.securityType" optionLabel="name" optionValue="id" size="small"></Select>
                    </div>
                    <div class="filter-group">
                        <label>{{ securitiesI18n.filters.segment }}</label>
                        <Select class="filter-dropdown" v-model="filters.segment" :placeholder="securitiesI18n.filters.placeholders.segment" optionLabel="name" optionValue="id" size="small"></Select>
                    </div>
                    <div class="filter-group">
                        <label>{{ securitiesI18n.filters.status }}</label>
                        <Select class="filter-dropdown" v-model="filters.is_active" :placeholder="securitiesI18n.filters.placeholders.status" optionLabel="name" optionValue="id" size="small"></Select>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>
<script>
import { mapState, mapActions } from 'pinia'

import { useSecuritiesStore } from '@/stores/securities'
export default {
    data() {
        return {
            securitiesI18n: this.$tm('pages.securities')
        }
    },
    computed: {
        ...mapState(useSecuritiesStore, ['exchanges', 'filters', 'stats', 'hasActiveFilters']),
    },
    methods: {
        ...mapActions(useSecuritiesStore, ['fetchExchanges', 'fetchSecurities', 'fetchSecuritiesStats', 'setValue']),

        /**
         * Utility function to format number.
         * @param {Number} value
         */
        formatNumber: function (value) {
            if (!value && value !== 0) return ''
            return new Intl.NumberFormat().format(value)
        },

        /**
         *  Loads the exchanges data in the state.
         */
        getExchanges: async function () {
            const response = await this.fetchExchanges()
            if (response.error) {
                this.$toast.add({ severity: 'error', summary: this.$tm('common.failed'), detail: this.securitiesI18n.messages.errorWhileFetchingExchanges, life: 3000 })
            }
        },

        /**
         * Loads the securities data in the state.
         */
        getSecurities: async function () {
            const response = await this.fetchSecurities()
            if (response.error) {
                this.$toast.add({ severity: 'error', summary: this.$tm('common.failed'), detail: this.securitiesI18n.messages.errorWhileFetchingSecurities, life: 3000 })
            }
        },

        /**
         * Loads the securities cache in state.
         */
        getSecuritiesStats: async function () {
            const response = await this.fetchSecuritiesStats()
            if (response.error) {
                this.$toast.add({ severity: 'error', summary: this.$tm('common.failed'), detail: this.securitiesI18n.messages.errorWhileFetchingSecurityStats, life: 3000 })
            }
        },

        /**
         * Fetches all the API's needed for the screen.
         */
        loadData: async function () {
            await this.getSecuritiesStats()
            await this.getExchanges()
            await this.getSecurities()
        },

        /**
         * Initializes the metadata needed for this page.
         */
        init: async function () {
            this.setValue('stats', this.$lodash.map(this.stats, (stat) => ({ ...stat, title: this.securitiesI18n.stats[stat.id] })))
            await this.loadData()
        }
    },
    mounted() {
        this.init()
    }
}
</script>
<style lang="postcss" scoped>
.securities-page {
    @apply qp-space-y-3 qp-w-full;

    .page-header {
        @apply qp-bg-white qp-rounded-md qp-border qp-border-primary-200 qp-p-3 qp-w-full;

        .header-content {
            @apply qp-flex qp-justify-between qp-items-center;

            .header-left {
                @apply qp-flex qp-flex-col qp-space-y-1;

                .page-title {
                    @apply qp-text-lg qp-font-bold qp-text-primary-900;
                }

                .page-subtitle {
                    @apply qp-text-sm qp-text-primary-600;
                }
            }

            .header-right {
                @apply qp-flex qp-gap-3;

                .import-btn {
                    @apply qp-bg-secondary-50 qp-text-secondary-700 qp-border-secondary-200 hover:qp-bg-secondary-100;
                }
            }
        }
    }

    /* Stats Grid */
    .stats-grid {
        @apply qp-grid qp-grid-cols-1 md:qp-grid-cols-2 lg:qp-grid-cols-4 qp-gap-4;

        .stat-card {
            @apply qp-bg-white qp-rounded-md qp-border qp-border-primary-200 qp-p-4 qp-flex qp-items-center qp-space-x-4;

            .stat-icon {
                @apply qp-w-12 qp-h-12 qp-rounded-lg qp-bg-primary-100 qp-flex qp-items-center qp-justify-center qp-text-primary-600 qp-text-2xl qp-ring-1 qp-ring-primary-200;

                &.active {
                    @apply qp-bg-green-100 qp-text-green-600 qp-ring-green-200;
                }

                &.futures {
                    @apply qp-bg-secondary-100 qp-text-secondary-600 qp-ring-secondary-200;
                }

                &.derivatives {
                    @apply qp-bg-purple-100 qp-text-purple-600 qp-ring-purple-200;
                }
            }

            .stat-content {
                @apply qp-flex qp-flex-col;

                .stat-value {
                    @apply qp-text-2xl qp-font-bold qp-text-primary-900;
                }

                .stat-label {
                    @apply qp-text-sm qp-text-primary-600;
                }
            }
        }
    }

    /* Filters */
    .filters-card {
        @apply qp-bg-white qp-rounded-md qp-ring-1 qp-ring-primary-200 qp-p-3;

        .filters-content {
            @apply qp-space-y-4;

            .search-section {
                @apply qp-flex qp-items-center qp-gap-3;

                .search-field {
                    @apply qp-w-full;

                    .search-input {
                        @apply qp-w-full;
                    }
                }
            }

            .filter-controls {
                @apply qp-grid qp-grid-cols-1 md:qp-grid-cols-2 lg:qp-grid-cols-4 qp-gap-4;

                .filter-group {
                    @apply qp-flex qp-flex-col qp-space-y-2;

                    label {
                        @apply qp-text-sm qp-font-medium qp-text-primary-700;
                    }

                    .filter-dropdown {
                        @apply qp-w-full;
                    }
                }
            }
        }
    }
}
</style>