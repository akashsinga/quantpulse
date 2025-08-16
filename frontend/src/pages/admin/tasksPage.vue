<template>
    <div class="tasks-page">
        <!-- Page Header -->
        <div class="page-header">
            <div class="header-content">
                <div class="header-left">
                    <div class="page-title">{{ tasksI18n.title }}</div>
                    <div class="page-subtitle">{{ tasksI18n.subtitle }}</div>
                </div>
                <div class="header-right">
                    <Button icon="ph ph-chart-bar" :label="tasksI18n.viewStats" size="small" severity="secondary" @click="showStatsDialog = true"></Button>
                    <Button icon="ph ph-broom" :label="tasksI18n.cleanup" size="small" severity="warn" @click="showCleanupDialog = true"></Button>
                    <Button icon="ph ph-arrow-clockwise" :label="$tm('common.refresh')" size="small" @click="refreshTasks"></Button>
                </div>
            </div>
        </div>

        <!-- Quick Stats Cards -->
        <div class="stats-grid">
            <div class="stat-card" v-for="stat in quickStats" :key="stat.id">
                <div :class="['stat-icon', stat.id]">
                    <i :class="stat.icon"></i>
                </div>
                <div class="stat-content">
                    <div class="stat-value">{{ getFormattedNumber(stat.value) }}</div>
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
                        <InputText class="search-input" v-model="filters.task_name" :placeholder="tasksI18n.searchPlaceholder" size="small" @input="debouncedSearch"></InputText>
                    </IconField>
                    <Button v-if="hasActiveFilters" :label="$tm('common.clear')" size="small" severity="danger" icon="ph ph-x" @click="clearAllFilters"></Button>
                </div>

                <!-- Filter Controls -->
                <div class="filter-controls">
                    <div class="filter-group">
                        <label>{{ tasksI18n.filters.status }}</label>
                        <Select class="filter-dropdown" v-model="filters.status" :placeholder="tasksI18n.filters.placeholders.status" :options="statusOptions" optionLabel="title" optionValue="value" size="small" @change="applyFilters"></Select>
                    </div>
                    <div class="filter-group">
                        <label>{{ tasksI18n.filters.taskType }}</label>
                        <Select class="filter-dropdown" v-model="filters.task_type" :placeholder="tasksI18n.filters.placeholders.taskType" :options="taskTypeOptions" optionLabel="title" optionValue="value" size="small" @change="applyFilters"></Select>
                    </div>
                    <div class="filter-group">
                        <label>{{ tasksI18n.filters.dateRange }}</label>
                        <Calendar class="filter-dropdown" v-model="filters.created_after" :placeholder="tasksI18n.filters.placeholders.createdAfter" size="small" showIcon @date-select="applyFilters"></Calendar>
                    </div>
                    <div class="filter-group">
                        <label>{{ tasksI18n.filters.sortBy }}</label>
                        <Select class="filter-dropdown" v-model="filters.sort_by" :placeholder="tasksI18n.filters.placeholders.createdDate" :options="sortOptions" optionLabel="title" optionValue="value" size="small" @change="applyFilters"></Select>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tasks Table -->
        <div class="table-card">
            <!-- Bulk Actions Toolbar -->
            <!-- <div v-if="selectedTasks.length > 0" class="bulk-actions-toolbar">
                <div class="bulk-info">
                    <span>{{ selectedTasks.length }} tasks selected</span>
                </div>
                <div class="bulk-actions">
                    <Button icon="ph ph-arrow-clockwise" label="Retry Selected" size="small" severity="info" @click="bulkRetryTasks"></Button>
                    <Button icon="ph ph-pause" label="Cancel Selected" size="small" severity="warn" @click="bulkCancelTasks"></Button>
                    <Button icon="ph ph-trash" label="Delete Selected" size="small" severity="danger" @click="bulkDeleteTasks"></Button>
                </div>
            </div> -->

            <DataTable class="tasks-table" v-model:selection="selectedTasks" :value="tasks" :loading="isLoading" :totalRecords="totalRecords" :rows="pagination.limit" :first="pagination.skip" paginator lazy showGridlines sortMode="single" :rowsPerPageOptions="[25, 50, 100]" paginatorTemplate="PrevPageLink CurrentPageReport NextPageLink RowsPerPageDropdown" currentPageReportTemplate="{first} to {last} of {totalRecords}" dataKey="id" :rowHover="true" @page="onPageChange" @sort="onSort">
                <template #loading>
                    <div class="loading-state">
                        <ProgressSpinner size="50" stroke-width="2"></ProgressSpinner>
                        <p>{{ $tm('common.loading') }}</p>
                    </div>
                </template>
                <template #empty>
                    <div class="empty-state">
                        <i class="ph ph-list-checks"></i>
                        <h3>{{ tasksI18n.noTasksFound }}</h3>
                        <p>{{ tasksI18n.tryAdjustingFilters }}</p>
                    </div>
                </template>

                <!-- Selection column -->
                <Column selectionMode="multiple" headerStyle="width: 3rem"></Column>
                <Column v-for="column in columns" class="table-column" :key="column.id" :field="column.id" :header="column.title" :sortable="column.sortable">
                    <template #body="slotProps">
                        <div v-if="column.id === 'title'" class="task-title-cell">
                            <div class="task-title">{{ slotProps.data.title }}</div>
                            <div class="task-name">{{ slotProps.data.task_name }}</div>
                        </div>
                        <div v-else-if="column.id === 'status'" class="status-cell">
                            <span>{{ $tm('common.jobStatus')[slotProps.data.status] }}</span>
                        </div>
                        <div v-else-if="column.id === 'task_type'" class="type-cell">
                            <span>{{ slotProps.data.task_type }}</span>
                        </div>
                        <div v-else-if="column.id === 'created_at'" class="date-cell">
                            {{ getFormattedDate(slotProps.data.created_at) }}
                            <small class="time-ago">({{ getElapsedTime(slotProps.data.created_at) }})</small>
                        </div>
                        <div v-else-if="column.id === 'execution_time'" class="duration-cell">
                            {{ getFormattedDuration(slotProps.data.execution_time_seconds) }}
                        </div>
                        <div v-else-if="column.id === 'actions'" class="actions-cell">
                            <Button icon="ph ph-eye" size="small" severity="primary" v-tooltip="tasksI18n.tooltips.viewDetails" @click="viewTaskDetails(slotProps.data)"></Button>
                            <Button v-if="canRetryTask(slotProps.data)" icon="ph ph-arrow-clockwise" size="small" severity="info" v-tooltip="tasksI18n.tooltips.retryTask" @click="retryTask(slotProps.data)"></Button>
                            <Button v-if="canCancelTask(slotProps.data)" icon="ph ph-pause" size="small" severity="warn" v-tooltip="tasksI18n.tooltips.cancelTask" @click="cancelTask(slotProps.data)"></Button>
                            <Button v-if="canDeleteTask(slotProps.data)" icon="ph ph-trash" size="small" severity="danger" v-tooltip="tasksI18n.tooltips.deleteTask" @click="deleteTask(slotProps.data)"></Button>
                        </div>
                        <div v-else>
                            {{ slotProps.data[column.id] || 'N/A' }}
                        </div>
                    </template>
                </Column>
            </DataTable>
        </div>

        <Dialog class="task-details-dialog" v-model:visible="showDetailsDialog" modal :style="{ width: '90vw', maxWidth: '1200px' }">
            <template #header>
                <div class="dialog-header">
                    <h3>{{ tasksI18n.taskDetails }}</h3>
                </div>
            </template>
            <TaskDetailsComponent v-if="selectedTask" :task="selectedTask" @refresh="refreshTaskDetails" @retry="retryTask" @cancel="cancelTask" @close="showDetailsDialog = false"></TaskDetailsComponent>
        </Dialog>
    </div>
</template>
<script>
import { mapState, mapActions } from 'pinia'
import { debounce } from 'lodash'

import { useTasksStore } from '@/stores/tasks'
import { useGlobalStore } from '@/stores/global'
export default {
    name: 'BackgroundTasks',
    data() {
        return {
            columns: [
                { id: 'title', sortable: true },
                { id: 'status', sortable: true },
                { id: 'task_type', sortable: true },
                { id: 'created_at', sortable: true },
                { id: 'execution_time', sortable: false },
                { id: 'actions', sortable: false }
            ],
            statusOptions: [
                { value: 'PENDING' },
                { value: 'STARTED' },
                { value: 'PROGRESS' },
                { value: 'SUCCESS' },
                { value: 'FAILURE' },
                { value: 'CANCELLED' }
            ],
            taskTypeOptions: [
                { value: 'SECURITIES_IMPORT' },
                { value: 'DATA_ENRICHMENT' },
                { value: 'SYSTEM_MAINTENANCE' },
                { value: 'CSV_PROCESSING' },
                { value: 'API_ENRICHMENT' }
            ],
            sortOptions: [
                { value: 'created_at' },
                { value: 'status' },
                { value: 'task_type' },
                { value: 'progress_percentage' }
            ],
            quickStats: [
                { id: 'total', icon: 'ph ph-list-checks', value: 0 },
                { id: 'running', icon: 'ph ph-play', value: 0 },
                { id: 'success', icon: 'ph ph-check-circle', value: 0 },
                { id: 'failed', icon: 'ph ph-x-circle', value: 0 }
            ],
            selectedTask: null,
            showDetailsDialog: false,
            selectedTasks: [],
            tasksI18n: this.$tm('pages.tasks')
        }
    },
    computed: {
        ...mapState(useTasksStore, ['tasks', 'filters', 'pagination', 'totalRecords', 'isLoading', 'hasActiveFilters'])
    },
    methods: {
        ...mapActions(useTasksStore, ['fetchTasks', 'fetchTaskDetails', 'retryTaskAction', 'cancelTaskAction', 'deleteTaskAction', 'clearFilters', 'setPagination', 'setSort', 'setValue']),
        ...mapActions(useGlobalStore, ['getFormattedDateTime', 'getFormattedNumber', 'getElapsedTime', 'getFormattedDuration']),

        /**
         * Apply filters and refresh data.
         */
        applyFilters: async function () {
            this.setPagination(0, this.pagination.limit)
            await this.getTasks()
        },

        /**
         * Clear all filters
         */
        clearAllFilters: function () {
            this.clearFilters()
            this.applyFilters()
        },

        /**
         * Check if task can be retried
         */
        canRetryTask: function (task) {
            return ['FAILURE', 'CANCELLED', 'REVOKED'].includes(task.status)
        },

        /**
         * Check if task can be cancelled
         */
        canCancelTask: function (task) {
            return ['PENDING', 'RECEIVED', 'STARTED', 'PROGRESS'].includes(task.status)
        },

        /**
         * Check if task can be deleted
         */
        canDeleteTask: function (task) {
            return ['SUCCESS', 'FAILURE', 'CANCELLED', 'REVOKED'].includes(task.status)
        },

        /**
         * Debounced search handler
         */
        debouncedSearch: debounce(function () {
            this.applyFilters()
        }, 500),

        /**
         * Handle page change
         */
        onPageChange: async function (event) {
            this.setPagination(event.first, event.rows)
            await this.getTasks()
        },

        /**
         * Handle sort change
         */
        onSort: async function (event) {
            this.setSort(event.sortField, event.sortOrder)
            await this.getTasks()
        },

        /**
         * Load tasks data.
         */
        getTasks: async function () {
            const response = await this.fetchTasks()
            if (response.error) {
                this.$toast.add({ severity: 'error', summary: this.$tm('common.error'), detail: this.tasksI18n.messages.errorFetchingTasks, life: 3000 })
            }
        },

        /**
         * Refresh tasks data
         */
        refreshTasks: async function () {
            await this.getTasks()
            this.$toast.add({ severity: 'success', summary: this.$tm('common.success'), detail: this.tasksI18n.messages.refreshSuccessful, life: 2000 })
        },

        /**
         * Retry a failed task
         */
        retryTask: function (task) {
            this.$confirm.require({
                message: this.tasksI18n.confirmations.retry,
                header: this.tasksI18n.actions.retryTask,
                icon: 'ph ph-arrow-clockwise',
                accept: async () => {
                    const response = await this.retryTaskAction(task.id, { reason: 'Manual retry from UI' })
                    if (response.error) {
                        this.$toast.add({ severity: 'error', summary: this.$tm('common.error'), detail: this.tasksI18n.errorRetryingTask, life: 3000 })
                    } else {
                        this.$toast.add({ severity: 'success', summary: this.$tm('common.success'), detail: this.tasksI18n.taskRetried, life: 3000 })
                        await this.getTasks()
                    }
                }
            })
        },


        /**
         * Cancel a running task
         */
        cancelTask: function (task) {
            this.$confirm.require({
                message: this.tasksI18n.confirmations.cancel,
                header: this.tasksI18n.actions.cancelTask,
                icon: 'ph ph-pause',
                accept: async () => {
                    const response = await this.cancelTaskAction(task.id, { reason: 'Cancelled from UI' })
                    if (response.error) {
                        this.$toast.add({ severity: 'error', summary: this.$tm('common.error'), detail: this.tasksI18n.errorCancellingTask, life: 3000 })
                    } else {
                        this.$toast.add({ severity: 'success', summary: this.$tm('common.success'), detail: this.tasksI18n.taskCancelled, life: 3000 })
                        await this.getTasks()
                    }
                }
            })
        },

        /**
         * Delete a task
         */
        deleteTask: function (task) {
            this.$confirm.require({
                message: this.tasksI18n.confirmations.delete,
                header: this.tasksI18n.actions.delete,
                icon: 'ph ph-trash',
                accept: async () => {
                    const response = await this.deleteTaskAction(task.id, false)
                    if (response.error) {
                        this.$toast.add({ severity: 'error', summary: this.$tm('common.error'), detail: this.tasksI18n.errorDeletingTask, life: 3000 })
                    } else {
                        this.$toast.add({ severity: 'success', summary: this.$tm('common.success'), detail: this.tasksI18n.taskDeleted, life: 3000 })
                        await this.getTasks()
                    }
                }
            })
        },

        /**
         * View task details
         */
        viewTaskDetails: async function (task) {
            const response = await this.fetchTaskDetails(task.id)
            if (response.error) {
                this.$toast.add({ severity: 'error', summary: this.$tm('common.error'), detail: this.tasksI18n.messages.errorFetchingDetails, life: 3000 })
                return
            }
            this.selectedTask = response.data
            this.showDetailsDialog = true
        },

        /**
         * Initialize page
         */
        init: async function () {
            this.sortOptions = this.$lodash.map(this.sortOptions, (option) => ({ ...option, title: this.tasksI18n.sortOptions[option.value] }))
            this.columns = this.$lodash.map(this.columns, (column) => ({ ...column, title: this.tasksI18n.columns[column.id] }))
            this.taskTypeOptions = this.$lodash.map(this.taskTypeOptions, (option) => ({ ...option, title: this.tasksI18n.taskTypes[option.value] }))
            this.statusOptions = this.$lodash.map(this.statusOptions, (option) => ({ ...option, title: this.$tm('common.jobStatus')[option.value] }))
            this.quickStats = this.$lodash.map(this.quickStats, (stat) => ({ ...stat, title: this.tasksI18n.quickStats[stat.id] }))
            await this.getTasks()
        }
    },
    mounted() {
        this.init()
    }
}
</script>
<style lang="postcss" scoped>
.tasks-page {
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

                &.running {
                    @apply qp-bg-blue-100 qp-text-blue-600 qp-ring-blue-200;
                }

                &.success {
                    @apply qp-bg-green-100 qp-text-green-600 qp-ring-green-200;
                }

                &.failed {
                    @apply qp-bg-red-100 qp-text-red-600 qp-ring-red-200;
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

    /* Tasks Table */
    .table-card {
        @apply qp-bg-white qp-rounded-lg qp-border qp-border-primary-200 qp-p-1;

        .tasks-table {
            @apply qp-w-full;

            .task-title-cell {
                @apply qp-space-y-1;

                .task-title {
                    @apply qp-font-medium qp-text-primary-900;
                }

                .task-name {
                    @apply qp-text-sm qp-text-primary-500 qp-font-mono;
                }
            }

            .status-cell {
                @apply qp-space-y-2;

                .progress-mini {
                    @apply qp-w-full;
                }
            }

            .date-cell {
                @apply qp-space-y-1;

                .time-ago {
                    @apply qp-text-primary-400 qp-block;
                }
            }

            .actions-cell {
                @apply qp-flex qp-items-center qp-space-x-1;
            }
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

    .bulk-actions-toolbar {
        @apply qp-flex qp-justify-between qp-items-center qp-p-3 qp-bg-blue-50 qp-border-t qp-border-blue-200;

        .bulk-info {
            @apply qp-text-sm qp-font-medium qp-text-blue-700;
        }

        .bulk-actions {
            @apply qp-flex qp-gap-2;
        }
    }
}

/* Dialog Styles */
.task-details-dialog,
.stats-dialog {
    .dialog-header {
        @apply qp-flex qp-items-center qp-justify-between qp-w-full;

        h3 {
            @apply qp-text-lg qp-font-semibold qp-text-primary-900;
        }
    }
}
</style>