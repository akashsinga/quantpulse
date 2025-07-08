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
                        <Select class="filter-dropdown" v-model="filters.exchange_id" :placeholder="securitiesI18n.filters.placeholders.exchange" optionLabel="name" optionValue="id" size="small"></Select>
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
export default {
    data() {
        return {
            filters: { search: '', exchange_id: null, security_type: null, segment: null, sector: null, is_active: null },
            securitiesI18n: this.$tm('pages.securities'),
            stats: [
                { id: 'total', icon: 'ph ph-database', value: 0 },
                { id: 'active', icon: 'ph ph-check-circle', value: 0 },
                { id: 'futures', icon: 'ph ph-chart-line', value: 0 },
                { id: 'derivatives', icon: 'ph ph-lightning', value: 0 }
            ]
        }
    },
    computed: {
        hasActiveFilters() {
            return !!(this.filters.search || this.filters.exchange_id || this.filters.security_type || this.filters.segment || this.filters.is_active !== null)
        }
    },
    methods: {
        /**
         * Utility function to format number.
         * @param {Number} value
         */
        formatNumber: function (value) {
            if (!value && value !== 0) return ''
            return new Intl.NumberFormat().format(value)
        },

        /**
         * Initializes the metadata needed for this page.
         */
        init: function () {
            this.stats = this.$lodash.map(this.stats, (stat) => ({ ...stat, title: this.securitiesI18n.stats[stat.id] }))
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
        @apply qp-bg-white qp-rounded-md qp-border qp-border-slate-200 qp-p-3 qp-w-full;

        .header-content {
            @apply qp-flex qp-justify-between qp-items-center;

            .header-left {
                @apply qp-flex qp-flex-col qp-space-y-1;

                .page-title {
                    @apply qp-text-lg qp-font-bold qp-text-slate-900;
                }

                .page-subtitle {
                    @apply qp-text-sm qp-text-slate-600;
                }
            }

            .header-right {
                @apply qp-flex qp-gap-3;

                .import-btn {
                    @apply qp-bg-blue-50 qp-text-blue-700 qp-border-blue-200 hover:qp-bg-blue-100;
                }
            }
        }
    }

    /* Stats Grid */
    .stats-grid {
        @apply qp-grid qp-grid-cols-1 md:qp-grid-cols-2 lg:qp-grid-cols-4 qp-gap-4;

        .stat-card {
            @apply qp-bg-white qp-rounded-md qp-border qp-border-slate-200 qp-p-4 qp-flex qp-items-center qp-space-x-4;

            .stat-icon {
                @apply qp-w-12 qp-h-12 qp-rounded-lg qp-bg-slate-100 qp-flex qp-items-center qp-justify-center qp-text-slate-600 qp-text-2xl qp-ring-1 qp-ring-slate-200;

                &.active {
                    @apply qp-bg-green-100 qp-text-green-600 qp-ring-green-200;
                }

                &.futures {
                    @apply qp-bg-blue-100 qp-text-blue-600 qp-ring-blue-200;
                }

                &.derivatives {
                    @apply qp-bg-purple-100 qp-text-purple-600 qp-ring-purple-200;
                }
            }

            .stat-content {
                @apply qp-flex qp-flex-col;

                .stat-value {
                    @apply qp-text-2xl qp-font-bold qp-text-slate-900;
                }

                .stat-label {
                    @apply qp-text-sm qp-text-slate-600;
                }
            }
        }
    }

    /* Filters */
    .filters-card {
        @apply qp-bg-white qp-rounded-md qp-ring-1 qp-ring-slate-200 qp-p-3;

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
                        @apply qp-text-sm qp-font-medium qp-text-slate-700;
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