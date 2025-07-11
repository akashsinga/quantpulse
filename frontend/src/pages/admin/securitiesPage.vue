<template>
    <div class="securities-page">
        <div class="page-header">
            <div class="header-content">
                <div class="header-left">
                    <div class="page-title">{{ securitiesI18n.securitiesManagement }}</div>
                    <div class="page-subtitle">{{ securitiesI18n.securitiesManagementSubtitle }}</div>
                </div>
                <div class="header-right">
                    <Button class="import-btn" icon="ph ph-download" :label="securitiesI18n.importSecurities" size="small" severity="secondary" @click="showImportDialog = true"></Button>
                    <Button icon="ph ph-arrow-clockwise" :label="$tm('common.refresh')" size="small" @click="refreshSecurities"></Button>
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
                        <InputText class="search-input" v-model="filters.search" :placeholder="securitiesI18n.searchPlaceholder" size="small" @input="debouncedSearch"></InputText>
                    </IconField>
                    <Button v-if="hasActiveFilters" text :label="$tm('common.clear')" size="small" icon="ph ph-x" @click="clearAllFilters"></Button>
                </div>

                <!-- Filter Groups -->
                <div class="filter-controls">
                    <div class="filter-group">
                        <label>{{ securitiesI18n.filters.exchange }}</label>
                        <Select class="filter-dropdown" v-model="filters.exchange_id" :placeholder="securitiesI18n.filters.placeholders.exchange" :options="exchanges" optionLabel="name" optionValue="id" size="small" @change="applyFilters"></Select>
                    </div>
                    <div class="filter-group">
                        <label>{{ securitiesI18n.filters.securityType }}</label>
                        <Select class="filter-dropdown" v-model="filters.security_type" :placeholder="securitiesI18n.filters.placeholders.securityType" :options="securityTypes" optionLabel="title" optionValue="id" size="small" @change="applyFilters"></Select>
                    </div>
                    <div class="filter-group">
                        <label>{{ securitiesI18n.filters.segment }}</label>
                        <Select class="filter-dropdown" v-model="filters.segment" :placeholder="securitiesI18n.filters.placeholders.segment" :options="segments" optionLabel="title" optionValue="id" size="small" @change="applyFilters"></Select>
                    </div>
                    <div class="filter-group">
                        <label>{{ securitiesI18n.filters.status }}</label>
                        <Select class="filter-dropdown" v-model="filters.is_active" :placeholder="securitiesI18n.filters.placeholders.status" :options="statusOptions" optionLabel="title" optionValue="value" size="small" @change="applyFilters"></Select>
                    </div>
                </div>
            </div>
        </div>

        <!-- Securities Table -->
        <div class="table-card">
            <DataTable class="securities-table" :value="securities" :loading="isLoading" :totalRecords="totalRecords" :rows="pagination.limit" :first="pagination.skip" paginator lazy showGridlines sortMode="single" :rowsPerPageOptions="[25, 50, 100]" paginatorTemplate="PrevPageLink CurrentPageReport NextPageLink RowsPerPageDropdown" currentPageReportTemplate="{first} to {last} of {totalRecords}" dataKey="id" :rowHover="true" @page="onPageChange" @sort="onSort">
                <template #loading>
                    <div class="loading-state">
                        <ProgressSpinner size="50" stroke-width="2"></ProgressSpinner>
                        <p>{{ $tm('common.loading') }}</p>
                    </div>
                </template>
                <template #empty>
                    <div class="empty-state">
                        <i class="ph ph-database"></i>
                        <h3>{{ securitiesI18n.noSecuritiesFound }}</h3>
                        <p>{{ securitiesI18n.tryAdjustingYourSearchCriteria }}</p>
                    </div>
                </template>
                <Column v-for="column in columns" class="table-column" :key="column.id" :field="column.id" :header="column.title" :sortable="column.sortable">
                    <template #body="slotProps">
                        <div v-if="column.id === 'symbol'" class="qp-flex qp-space-x-2">
                            <span>{{ slotProps.data.symbol }}</span>
                            <Badge :value="slotProps.data.exchange?.code" severity="secondary" size="small"></Badge>
                        </div>
                        <div v-else-if="column.id === 'segment'" class="qp-flex qp-items-center qp-space-x-2">
                            <span>{{ slotProps.data.segment }}</span>
                            <Badge v-if="!(slotProps.data.segment === slotProps.data.security_type)" severity="secondary" :value="slotProps.data.security_type" size="small"></Badge>
                        </div>
                        <div v-else-if="column.id === 'actions'" class="qp-flex qp-items-center qp-space-x-1.5">
                            <Button icon="ph ph-eye" size="small" severity="primary" v-tooltip="securitiesI18n.tooltips.viewSecurity"></Button>
                            <Button icon="ph ph-pencil" size="small" severity="info" v-tooltip="securitiesI18n.tooltips.editSecurity"></Button>
                            <Button :icon="slotProps.data.is_active ? 'ph ph-pause' : 'ph ph-play'" size="small" :severity="slotProps.data.is_active ? 'danger' : 'success'" v-tooltip="slotProps.data.is_active ? $tm('common.deactivate') : $tm('common.activate')"></Button>
                        </div>
                        <div v-else>
                            {{ slotProps.data[column.id] || 'N/A' }}
                        </div>
                    </template>
                </Column>
            </DataTable>
        </div>

        <!-- Import Dialog -->
        <Dialog class="import-dialog" v-model:visible="showImportDialog" modal :style="{ width: '500px' }">
            <div class="import-content">
                <div class="import-info">
                    <i class="ph ph-info-circle"></i>
                    <div>
                        <h4>{{ securitiesI18n.importDialog.importTitle }}</h4>
                        <p>{{ securitiesI18n.importDialog.importSubtitle }}</p>
                    </div>
                </div>
                <div class="import-options">
                    <div class="option-item">
                        <Checkbox id="force_refresh" v-model="importOptions.force_refresh"></Checkbox>
                        <label for="force_refresh">{{ securitiesI18n.importDialog.forceRefresh }}</label>
                    </div>
                </div>
            </div>
            <template #footer>
                <Button :label="$tm('common.cancel')" text @click="showImportDialog = false"></Button>
                <Button :label="securitiesI18n.importDialog.startImport" icon="ph ph-download"></Button>
            </template>
        </Dialog>
    </div>
</template>
<script>
import { mapState, mapActions } from 'pinia'

import { useSecuritiesStore } from '@/stores/securities'
import { debounce } from 'lodash'
export default {
    data() {
        return {
            securitiesI18n: this.$tm('pages.securities'),
            isRefreshing: false,
            columns: [
                { id: 'symbol', sortable: true },
                { id: 'name', sortable: true },
                { id: 'segment' },
                { id: 'sector', sortable: true },
                { id: 'industry', sortable: true },
                { id: 'actions' }
            ],
            statusOptions: [
                { title: this.$tm('common.active'), value: true },
                { title: this.$tm('common.inactive'), value: false }
            ],
            showImportDialog: false,
            importOptions: { force_refresh: false }
        }
    },
    computed: {
        ...mapState(useSecuritiesStore, ['securities', 'exchanges', 'filters', 'stats', 'securityTypes', 'segments', 'isLoading', 'pagination', 'totalRecords', 'hasActiveFilters']),
    },
    methods: {
        ...mapActions(useSecuritiesStore, ['fetchExchanges', 'fetchSecurities', 'fetchSecuritiesStats', 'setValue', 'setPagination', 'setSort', 'clearFilters']),

        /**
         * Applies filters
         */
        applyFilters: async function () {
            this.setPagination(0, this.pagination.limit)
            await this.getSecurities()
        },

        /**
         * Resets data to initial state.
         */
        clearAllFilters: function () {
            this.clearFilters()
            this.applyFilters()
        },

        /**
         * Input Handler for Searching Security.
         */
        debouncedSearch: debounce(function () {
            this.applyFilters()
        }, 500),

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
            this.columns = this.$lodash.map(this.columns, (column) => ({ ...column, title: this.securitiesI18n.tableHeaders[column.id] }))
            this.setValue('stats', this.$lodash.map(this.stats, (stat) => ({ ...stat, title: this.securitiesI18n.stats[stat.id] })))
            this.setValue('securityTypes', this.$lodash.map(this.securityTypes, (securityType) => ({ ...securityType, title: this.securitiesI18n.securityTypes[securityType.id] })))
            this.setValue('segments', this.$lodash.map(this.segments, (segment) => ({ ...segment, title: this.securitiesI18n.segments[segment.id] })))
            await this.loadData()
        },

        /**
         * Executed on Page Change
         * @param {Object} event
         */
        onPageChange: async function (event) {
            this.setPagination(event.first, event.rows)
            await this.getSecurities()
        },

        /**
         * Executed on sort order change.
         * @param {Object} event
         */
        onSort: async function (event) {
            this.setSort(event.sortField, event.sortOrder)
            await this.getSecurities()
        },

        /**
         * Handles full page refresh flow.
         */
        refreshSecurities: async function () {
            this.isRefreshing = true
            await this.loadData()
            this.isRefreshing = false
            this.$toast.add({ severity: 'success', summary: this.$tm('common.success'), detail: this.securitiesI18n.refreshSuccessful, life: 3000 })
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

    /* Securities Table */
    .table-card {
        @apply qp-bg-white qp-rounded-lg qp-border qp-border-primary-200 qp-p-1;

        .securities-table {
            @apply qp-w-full;
        }

        .empty-state {
            @apply qp-text-center qp-py-12;

            i {
                @apply qp-text-4xl qp-text-primary-300 qp-mb-4;
            }

            h3 {
                @apply qp-text-lg qp-font-semibold qp-text-primary-900 qp-mb-2;
            }

            p {
                @apply qp-text-primary-600;
            }
        }

        .loading-state {
            @apply qp-text-center qp-py-12;

            p {
                @apply qp-mt-4 qp-text-primary-600;
            }
        }
    }
}

/* Import Dialog */
.import-dialog {
    @apply qp-max-w-lg;

    .import-content {
        @apply qp-space-y-6;

        .import-info {
            @apply qp-flex qp-gap-3 qp-p-4 qp-bg-secondary-50 qp-rounded-md qp-border qp-border-secondary-200;

            i {
                @appyl qp-text-secondary-600 qp-text-xl qp-flex-shrink-0 qp-mt-1;
            }

            h4 {
                @apply qp-font-semibold qp-text-primary-900 qp-mb-1;
            }

            p {
                @apply qp-text-sm qp-text-primary-600;
            }
        }

        .import-options {
            @apply qp-space-y-3;

            .option-item {
                @apply qp-flex qp-items-center qp-gap-2;

                label {
                    @apply qp-text-sm qp-text-primary-700;
                }
            }
        }
    }
}
</style>