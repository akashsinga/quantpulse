<template>
    <div class="task-stats">
        <!-- Overview Metrics -->
        <div class="overview-section">
            <h4>{{ taskStatsI18n.overviewTitle }}</h4>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-icon total">
                        <i class="ph ph-list-checks"></i>
                    </div>
                    <div class="stat-content">
                        <div class="stat-value">{{ stats.total_tasks || 0 }}</div>
                        <div class="stat-label">{{ taskStatsI18n.overview.totalTasks }}</div>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon running">
                        <i class="ph ph-play"></i>
                    </div>
                    <div class="stat-content">
                        <div class="stat-value">{{ stats.running_tasks || 0 }}</div>
                        <div class="stat-label">{{ taskStatsI18n.overview.currentlyRunning }}</div>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon recent">
                        <i class="ph ph-clock"></i>
                    </div>
                    <div class="stat-content">
                        <div class="stat-value">{{ stats.recent_tasks_24h || 0 }}</div>
                        <div class="stat-label">{{ taskStatsI18n.overview.last24Hours }}</div>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon failures">
                        <i class="ph ph-warning"></i>
                    </div>
                    <div class="stat-content">
                        <div class="stat-value">{{ stats.recent_failures_7d || 0 }}</div>
                        <div class="stat-label">{{ taskStatsI18n.overview.failures }}</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Success Rate -->
        <div class="success-rate-section">
            <h4>{{ taskStatsI18n.successRateTitle }}</h4>
            <div class="success-rate-card">
                <div class="rate-circle">
                    <div class="rate-value">{{ stats.success_rate_percentage || 0 }}%</div>
                    <div class="rate-label">{{ taskStatsI18n.successRateTitle }}</div>
                </div>
                <div class="rate-details">
                    <div class="rate-item">
                        <div class="rate-indicator success"></div>
                        <span>{{ taskStatsI18n.successRate.successfulTasks }}</span>
                    </div>
                    <div class="rate-item">
                        <div class="rate-indicator failed"></div>
                        <span>{{ taskStatsI18n.successRate.failedTasks }}</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- Status Breakdown -->
        <div class="breakdown-section">
            <h4>{{ taskStatsI18n.statusBreakdown }}</h4>
            <div class="breakdown-chart">
                <div v-for="(count, status) in stats.status_breakdown" :key="status" class="breakdown-item">
                    <div class="breakdown-bar">
                        <div class="breakdown-fill" :style="{ width: getPercentage(count, getTotalStatusCount()) + '%' }" :class="getStatusClass(status)"></div>
                    </div>
                    <div class="breakdown-info">
                        <span class="breakdown-label">{{ $tm('common.jobStatus')[status] }}</span>
                        <span class="breakdown-count">{{ count }}</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- Task Types -->
        <div class="types-section">
            <h4>{{ taskStatsI18n.taskTypes }}</h4>
            <div class="types-list">
                <div v-for="(count, type) in stats.type_breakdown" :key="type" class="type-item">
                    <div class="type-icon">
                        <i :class="getTypeIcon(type)"></i>
                    </div>
                    <div class="type-content">
                        <div class="type-name">{{ $tm('pages.tasks.taskTypes')[type] }}</div>
                        <div class="type-count">{{ count }} tasks</div>
                    </div>
                    <div class="type-percentage">{{ getPercentage(count, getTotalTypeCount()) }}%</div>
                </div>
            </div>
        </div>

        <!-- Running Tasks Details -->
        <div v-if="stats.running_task_details && stats.running_task_details.length > 0" class="running-tasks-section">
            <h4>Currently Running Tasks</h4>
            <div class="running-tasks-list">
                <div v-for="task in stats.running_task_details" :key="task.id" class="running-task-item">
                    <div class="task-header">
                        <div class="task-title">{{ task.title }}</div>
                        <Badge :value="$tm('common.jobStatus')[task.status]" :severity="getStatusSeverity(task.status)" size="small"></Badge>
                    </div>
                    <div class="task-progress">
                        <div class="progress-info">
                            <span class="progress-label">{{ task.current_message || $tm('common.processing') }}</span>
                            <span class="progress-percentage">{{ task.progress_percentage || 0 }}%</span>
                        </div>
                        <ProgressBar :value="task.progress_percentage || 0" style="height: 6px;"></ProgressBar>
                    </div>
                    <div class="task-meta">
                        <span class="task-started">Started: {{ getFormattedTime(task.started_at) }}</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- Refresh Button -->
        <div class="stats-actions">
            <Button icon="ph ph-arrow-clockwise" :label="taskStatsI18n.refreshStatistics" size="small" @click="refreshStats"></Button>
        </div>
    </div>
</template>
<script>
import { mapActions } from 'pinia'
import { useGlobalStore } from '@/stores/global'
import { useTasksStore } from '@/stores/tasks'
export default {
    name: 'TaskStats',
    emits: ['close'],
    data() {
        return {
            stats: {},
            taskStatsI18n: this.$tm('components.tasksStats'),
            isLoading: false
        }
    },
    methods: {
        ...mapActions(useTasksStore, ['fetchTaskStatistics']),
        ...mapActions(useGlobalStore, ['getPercentage', 'getFormattedTime']),
        /**
         * Fetches tasks overview statistics.
         */
        fetchStats: async function () {
            this.isLoading = true
            const response = await this.fetchTaskStatistics()
            if (response.error) {
                this.$toast.add({ severity: 'error', summary: this.$tm('common.error'), detail: this.$tm('pages.tasks.messages.errorFetchingStats'), life: 3000 })
                this.isLoading = false
                return
            }
            this.stats = response.data.data.data
            this.isLoading = false
        },

        /**
         * Refreshes task stats
         */
        refreshStats: async function () {
            await this.fetchStats()
            this.$toast.add({ severity: 'success', summary: this.$tm('common.success'), detail: this.$tm('pages.tasks.messages.refreshSuccessful'), life: 2000 })
        },

        /**
         * Returns status wise count
         * @returns {Object}
         */
        getTotalStatusCount: function () {
            if (!this.stats.status_breakdown) return 0
            return Object.values(this.stats.status_breakdown).reduce((sum, count) => sum + count, 0)
        },

        /**
         * Returns total type wise count.
         * @returns {Object}
         */
        getTotalTypeCount: function () {
            if (!this.stats.type_breakdown) return 0
            return Object.values(this.stats.type_breakdown).reduce((sum, count) => sum + count, 0)
        },

        /**
         * Returns color based on status
         * @param {String} status
         * @returns {String}
         */
        getStatusSeverity: function (status) {
            const severityMap = { 'PENDING': 'secondary', 'RECEIVED': 'info', 'STARTED': 'info', 'PROGRESS': 'info', 'SUCCESS': 'success', 'FAILURE': 'danger', 'CANCELLED': 'warn', 'REVOKED': 'warn' }
            return severityMap[status] || 'secondary'
        },

        /**
         * Returns class based on status
         * @param {String} status
         * @returns {String}
         */
        getStatusClass(status) {
            const classMap = { 'SUCCESS': 'status-success', 'FAILURE': 'status-error', 'PENDING': 'status-pending', 'STARTED': 'status-running', 'PROGRESS': 'status-running', 'CANCELLED': 'status-cancelled', 'REVOKED': 'status-cancelled' }
            return classMap[status] || 'status-pending'
        },

        /**
         * Returns icon based on task type.
         * @param {String} type
         * @returns {String}
         */
        getTypeIcon: function (type) {
            const iconMap = { 'SECURITIES_IMPORT': 'ph ph-database', 'DATA_ENRICHMENT': 'ph ph-magic-wand', 'SYSTEM_MAINTENANCE': 'ph ph-gear', 'CSV_PROCESSING': 'ph ph-file-csv', 'API_ENRICHMENT': 'ph ph-globe' }
            return iconMap[type] || 'ph ph-gear'
        },

        /**
         * Initializes data for the component.
         */
        init: async function () {
            await this.fetchStats()
        }
    },
    mounted() {
        this.init()
    }
}
</script>
<style lang="postcss" scoped>
.task-stats {
    @apply qp-space-y-6;

    h4 {
        @apply qp-text-lg qp-font-semibold qp-text-primary-900 qp-mb-4;
    }

    .overview-section {
        .stats-grid {
            @apply qp-grid qp-grid-cols-2 md:qp-grid-cols-4 qp-gap-4;

            .stat-card {
                @apply qp-bg-primary-50 qp-rounded-lg qp-p-4 qp-flex qp-items-center qp-space-x-3 qp-border qp-border-primary-200;

                .stat-icon {
                    @apply qp-w-12 qp-h-12 qp-rounded-lg qp-flex qp-items-center qp-justify-center qp-text-white qp-text-xl;

                    &.total {
                        @apply qp-bg-primary-600;
                    }

                    &.running {
                        @apply qp-bg-blue-600;
                    }

                    &.recent {
                        @apply qp-bg-green-600;
                    }

                    &.failures {
                        @apply qp-bg-red-600;
                    }
                }

                .stat-content {
                    .stat-value {
                        @apply qp-text-2xl qp-font-bold qp-text-primary-900;
                    }

                    .stat-label {
                        @apply qp-text-sm qp-text-primary-600;
                    }
                }
            }
        }
    }

    .success-rate-section {
        .success-rate-card {
            @apply qp-bg-primary-50 qp-rounded-lg qp-p-6 qp-flex qp-items-center qp-justify-between qp-border qp-border-primary-200;

            .rate-circle {
                @apply qp-text-center;

                .rate-value {
                    @apply qp-text-4xl qp-font-bold qp-text-green-600;
                }

                .rate-label {
                    @apply qp-text-sm qp-text-primary-600;
                }
            }

            .rate-details {
                @apply qp-space-y-2;

                .rate-item {
                    @apply qp-flex qp-items-center qp-gap-2;

                    .rate-indicator {
                        @apply qp-w-3 qp-h-3 qp-rounded-full;

                        &.success {
                            @apply qp-bg-green-500;
                        }

                        &.failed {
                            @apply qp-bg-red-500;
                        }
                    }

                    span {
                        @apply qp-text-sm qp-text-primary-700;
                    }
                }
            }
        }
    }

    .breakdown-section {
        .breakdown-chart {
            @apply qp-space-y-3;

            .breakdown-item {
                @apply qp-space-y-2;

                .breakdown-bar {
                    @apply qp-w-full qp-h-3 qp-bg-primary-200 qp-rounded-full qp-overflow-hidden;

                    .breakdown-fill {
                        @apply qp-h-full qp-transition-all qp-duration-300;

                        &.status-success {
                            @apply qp-bg-green-500;
                        }

                        &.status-error {
                            @apply qp-bg-red-500;
                        }

                        &.status-running {
                            @apply qp-bg-blue-500;
                        }

                        &.status-pending {
                            @apply qp-bg-gray-400;
                        }

                        &.status-cancelled {
                            @apply qp-bg-yellow-500;
                        }
                    }
                }

                .breakdown-info {
                    @apply qp-flex qp-justify-between qp-items-center;

                    .breakdown-label {
                        @apply qp-text-sm qp-font-medium qp-text-primary-700;
                    }

                    .breakdown-count {
                        @apply qp-text-sm qp-font-bold qp-text-primary-900;
                    }
                }
            }
        }
    }

    .types-section {
        .types-list {
            @apply qp-space-y-3;

            .type-item {
                @apply qp-flex qp-items-center qp-gap-4 qp-p-3 qp-bg-primary-50 qp-rounded-lg qp-border qp-border-primary-200;

                .type-icon {
                    @apply qp-w-10 qp-h-10 qp-bg-secondary-100 qp-rounded-lg qp-flex qp-items-center qp-justify-center qp-text-secondary-600;
                }

                .type-content {
                    @apply qp-flex-1;

                    .type-name {
                        @apply qp-font-medium qp-text-primary-900;
                    }

                    .type-count {
                        @apply qp-text-sm qp-text-primary-600;
                    }
                }

                .type-percentage {
                    @apply qp-text-sm qp-font-bold qp-text-primary-700;
                }
            }
        }
    }

    .running-tasks-section {
        .running-tasks-list {
            @apply qp-space-y-4;

            .running-task-item {
                @apply qp-border qp-border-primary-200 qp-rounded-lg qp-p-4 qp-bg-blue-50;

                .task-header {
                    @apply qp-flex qp-justify-between qp-items-center qp-mb-3;

                    .task-title {
                        @apply qp-font-medium qp-text-primary-900;
                    }
                }

                .task-progress {
                    @apply qp-space-y-2 qp-mb-3;

                    .progress-info {
                        @apply qp-flex qp-justify-between qp-items-center;

                        .progress-label {
                            @apply qp-text-sm qp-text-primary-700;
                        }

                        .progress-percentage {
                            @apply qp-text-sm qp-font-bold qp-text-primary-900;
                        }
                    }
                }

                .task-meta {
                    .task-started {
                        @apply qp-text-xs qp-text-primary-500;
                    }
                }
            }
        }
    }

    .stats-actions {
        @apply qp-text-center qp-pt-4 qp-border-t qp-border-primary-200;
    }
}
</style>