<template>
    <div class="task-details">
        <!-- Task Overview -->
        <div class="task-overview">
            <div class="task-header">
                <div class="task-info">
                    <div class="task-title">{{ task.title }}</div>
                    <div class="task-meta">
                        <Badge :value="task.task_type" severity="info"></Badge>
                        <Badge :value="$tm('common.jobStatus')[task.status]"></Badge>
                        <span class="task-id">ID: {{ task.id }}</span>
                    </div>
                </div>
                <div class="task-actions">
                    <Button v-if="canRetryTask" icon="ph ph-arrow-clockwise" :label="$tm('common.retry')" size="small" severity="info" @click="$emit('retry', task)"></Button>
                    <Button v-if="canCancelTask" icon="ph ph-pause" :label="$tm('common.cancel')" size="small" severity="warn" @click="$emit('cancel', task)"></Button>
                    <Button icon="ph ph-arrow-clockwise" :label="$tm('common.refresh')" size="small" @click="$emit('refresh')"></Button>
                </div>
            </div>

            <!-- Progress Bar -->
            <div v-if="task.progress_percentage > 0" class="progress-section">
                <div class="progress-header">
                    <span class="progress-label">{{ $tm('common.progress') }}</span>
                    <span class="progress-percentage">{{ task.progress_percentage }}%</span>
                </div>
                <ProgressBar :value="task.progress_percentage"></ProgressBar>
                <div v-if="task.current_message" class="progress-message">{{ task.current_message }}</div>
            </div>
        </div>

        <!-- Task Statistics -->
        <div class="task-stats-grid">
            <div class="stat-item">
                <div class="stat-label">{{ taskDetailsI18n.statistics.created }}</div>
                <div class="stat-value">{{ getFormattedDateTime(task.created_at) }}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">{{ taskDetailsI18n.statistics.started }}</div>
                <div class="stat-value">{{ task.started_at ? getFormattedDateTime(task.started_at) : taskDetailsI18n.notStarted }}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">{{ taskDetailsI18n.statistics.completed }}</div>
                <div class="stat-value">{{ task.completed_at ? getFormattedDateTime(task.completed_at) : taskDetailsI18n.notCompleted }}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">{{ taskDetailsI18n.statistics.duration }}</div>
                <div class="stat-value">{{ getFormattedDuration(task.execution_time_seconds) }}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">{{ taskDetailsI18n.statistics.retries }}</div>
                <div class="stat-value">{{ task.retry_count || 0 }}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">{{ taskDetailsI18n.statistics.user }}</div>
                <div class="stat-value">{{ task.user_id ? taskDetailsI18n.superAdmin : taskDetailsI18n.system }}</div>
            </div>
        </div>

        <!-- Tabbed Content -->
        <div class="task-tabs">
            <Tabs :value="selectedTab" scrollable>
                <TabList>
                    <Tab value="details">
                        <div class="tab-header">
                            <i class="ph ph-info"></i>
                            <span>{{ taskDetailsI18n.tabs.details }}</span>
                        </div>
                    </Tab>
                    <Tab value="steps" v-if="task.steps?.length">
                        <div class="tab-header">
                            <i class="ph ph-list-checks"></i>
                            <span>{{ taskDetailsI18n.tabs.steps }} ({{ task.steps.length }})</span>
                        </div>
                    </Tab>
                    <Tab value="logs" v-if="task.logs?.length">
                        <div class="tab-header">
                            <i class="ph ph-file-text"></i>
                            <span>{{ taskDetailsI18n.tabs.logs }} ({{ task.logs.length }})</span>
                        </div>
                    </Tab>
                    <Tab value="result" v-if="task.result_data">
                        <div class="tab-header">
                            <i class="ph ph-chart-bar"></i>
                            <span>{{ taskDetailsI18n.tabs.results }}</span>
                        </div>
                    </Tab>
                    <Tab value="error" v-if="task.error_message">
                        <div class="tab-header">
                            <i class="ph ph-warning"></i>
                            <span>{{ taskDetailsI18n.tabs.error }}</span>
                        </div>
                    </Tab>
                </TabList>
                <TabPanels>
                    <!-- Details Tab -->
                    <TabPanel value="details">
                        <div class="details-content">
                            <div class="detail-section">
                                <h4>{{ taskDetailsI18n.tabDetails.details.basicInformation }}</h4>
                                <div class="detail-grid">
                                    <div class="detail-item">
                                        <label>{{ taskDetailsI18n.tabDetails.details.taskName }}</label>
                                        <span>{{ task.task_name }}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>{{ taskDetailsI18n.tabDetails.details.celeryTaskID }}</label>
                                        <span class="code">{{ task.celery_task_id }}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>{{ taskDetailsI18n.tabDetails.details.description }}</label>
                                        <span>{{ task.description || 'N/A' }}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>{{ taskDetailsI18n.tabDetails.details.currentStep }}</label>
                                        <span>{{ task.current_step }} / {{ task.total_steps }}</span>
                                    </div>
                                </div>
                            </div>
                            <div v-if="task.input_parameters" class="detail-section">
                                <h4>{{ taskDetailsI18n.tabDetails.details.inputParameters }}</h4>
                                <pre class="json-display">{{ JSON.stringify(task.input_parameters, null, 2) }}</pre>
                            </div>
                        </div>
                    </TabPanel>

                    <!-- Steps Tab -->
                    <TabPanel value="steps" v-if="task.steps?.length">
                        <div class="steps-content">
                            <div class="steps-list">
                                <div v-for="step in task.steps" :key="step.id" class="step-item">
                                    <div class="step-header">
                                        <div class="step-indicator" :class="getStepStatusClass(step.status)">
                                            <i :class="getStepStatusIcon(step.status)"></i>
                                        </div>
                                        <div class="step-info">
                                            <div class="step-title">{{ step.title }}</div>
                                            <div class="step-meta">
                                                <span class="step-order">Step {{ step.step_order }}</span>
                                                <Badge :value="$tm('common.jobStatus')[step.status]" :severity="getStatusSeverity(step.status)" size="small"></Badge>
                                            </div>
                                        </div>
                                    </div>
                                    <div v-if="step.result_data" class="step-result">
                                        <pre class="json-display">{{ JSON.stringify(step.result_data, null, 2) }}</pre>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </TabPanel>

                    <!-- Logs Tab -->
                    <TabPanel value="logs" v-if="task.logs?.length">
                        <div class="logs-content">
                            <div class="logs-toolbar">
                                <div class="log-filters">
                                    <Select v-model="logLevelFilter" :options="logLevelOptions" optionLabel="label" optionValue="value" :placeholder="taskDetailsI18n.tabDetails.logs.logLevelOptions.allLevels" size="small" @change="filterLogs"></Select>
                                </div>
                                <div class="log-actions">
                                    <Button icon="ph ph-download" :label="$tm('common.export')" size="small" severity="secondary" @click="exportLogs"></Button>
                                </div>
                            </div>
                            <div class="logs-list">
                                <div v-for="log in filteredLogs" :key="log.id" class="log-entry" :class="`log-${log.level.toLowerCase()}`">
                                    <div class="log-header">
                                        <Badge :value="log.level" :severity="getLogSeverity(log.level)" size="small"></Badge>
                                        <span class="log-timestamp">{{ getFormattedDateTime(log.created_at) }}</span>
                                    </div>
                                    <div class="log-message">{{ log.message }}</div>
                                    <div v-if="log.extra_data" class="log-extra">
                                        <pre class="json-display">{{ JSON.stringify(log.extra_data, null, 2) }}</pre>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </TabPanel>

                    <!-- Results Tab -->
                    <TabPanel value="result" v-if="task.result_data">
                        <div class="result-content">
                            <div class="result-header">
                                <h4>{{ taskDetailsI18n.tabDetails.details.taskResults }}</h4>
                                <Button icon="ph ph-copy" :label="$tm('common.copy')" size="small" @click="copyResults"></Button>
                            </div>
                            <pre class="json-display result-data">{{ JSON.stringify(task.result_data, null, 2) }}</pre>
                        </div>
                    </TabPanel>

                    <!-- Error Tab -->
                    <TabPanel value="error" v-if="task.error_message">
                        <div class="error-content">
                            <div class="error-message">
                                <h4>{{ taskDetailsI18n.tabDetails.error.errorMessage }}</h4>
                                <div class="error-text">{{ task.error_message }}</div>
                            </div>
                            <div v-if="task.error_traceback" class="error-traceback">
                                <h4>{{ taskDetailsI18n.tabDetails.error.traceback }}</h4>
                                <pre class="traceback-text">{{ task.error_traceback }}</pre>
                            </div>
                        </div>
                    </TabPanel>
                </TabPanels>
            </Tabs>
        </div>
    </div>
</template>
<script>
import { mapActions } from 'pinia'
import { useGlobalStore } from '@/stores/global'
export default {
    props: {
        task: { type: Object, required: true }
    },
    emits: ['refresh', 'retry', 'cancel', 'close'],
    data() {
        return {
            selectedTab: 'details',
            taskDetailsI18n: this.$tm('components.tasksDetails'),
            logLevelFilter: null,
            logLevelOptions: [
                { value: 'DEBUG' },
                { value: 'INFO' },
                { value: 'WARNING' },
                { value: 'ERROR' },
                { value: 'CRITICAL' }
            ],
            filteredLogs: []
        }
    },
    computed: {
        canRetryTask() {
            return ['FAILURE', 'CANCELLED', 'REVOKED'].includes(this.task.status)
        },
        canCancelTask() {
            return ['PENDING', 'RECEIVED', 'STARTED', 'PROGRESS'].includes(this.task.status)
        }
    },
    methods: {
        ...mapActions(useGlobalStore, ['getFormattedDateTime', 'getFormattedDuration']),
        /**
         * Copies result JSON to clipboard.
         */
        copyResults: function () {
            navigator.clipboard.writeText(JSON.stringify(this.task.result_data, null, 2)).then(() => { this.$toast.add({ severity: 'success', summary: this.$tm('common.copied'), detail: this.$tm('common.textCopiedToClipboard'), life: 2000 }) })
        },

        /**
         * Filters Logs
         */
        filterLogs: function () {
            this.filteredLogs = this.logLevelFilter ? this.$lodash.filter(this.task.logs, { level: this.logLevelFilter }) : (this.task.logs || [])
        },

        /**
         * Exports Logs
         */
        exportLogs: function () {
            const logs = this.$lodash.map(this.filteredLogs, (log) => ({ timestamp: log.created_at, level: log.level, message: log.message, extra: log.extra_data }))
            const dataStr = JSON.stringify(logs, nulll, 2)
            const dataBlob = new Blob([dataStr], { type: 'application/json' })
            const url = URL.createObjectURL(dataBlob)
            const link = document.createElement('a')
            link.href = url
            link.download = `task-${this.task.id.slice(0, 8)}-logs.json`
            link.click()
            URL.revokeObjectURL(url)
        },

        /**
         * Returns Step Status Class.
         * @param {String} status
         * @returns {String}
         */
        getStepStatusClass: function (status) {
            const classMap = { 'SUCCESS': 'step-success', 'FAILURE': 'step-error', 'PROGRESS': 'step-running', 'PENDING': 'step-pending' }
            return classMap[status] || 'step-pending'
        },

        /**
         * Returns Step Icon based on status.
         * @param {String} status
         * @returns {String}
         */
        getStepStatusIcon: function (status) {
            const iconMap = { 'SUCCESS': 'ph ph-check', 'FAILURE': 'ph ph-x', 'PROGRESS': 'ph ph-spinner', 'PENDING': 'ph ph-clock' }
            return iconMap[status] || 'ph ph-clock'
        },

        /**
         * Returns class for log severity.
         * @param {String} level
         * @returns {String}
         */
        getLogSeverity: function (level) {
            const severityMap = { 'DEBUG': 'secondary', 'INFO': 'info', 'WARNING': 'warn', 'ERROR': 'danger', 'CRITICAL': 'danger' }
            return severityMap[level] || 'secondary'
        },

        /**
         * Initializes data needed for the component.
         */
        init: function () {
            this.logLevelOptions = this.$lodash.map(this.logLevelOptions, (option) => ({ ...option, label: this.taskDetailsI18n.tabDetails.logs.logLevelOptions[option.value] }))
        }
    },
    mounted() {
        this.init()
    }
}
</script>
<style lang="postcss" scoped>
.task-details {
    @apply qp-space-y-6;

    .task-overview {
        @apply qp-space-y-4;

        .task-header {
            @apply qp-flex qp-justify-between qp-items-start;

            .task-info {
                @apply qp-space-y-2;

                .task-title {
                    @apply qp-text-xl qp-font-bold qp-text-primary-900;
                }

                .task-meta {
                    @apply qp-flex qp-items-center qp-gap-3;

                    .task-id {
                        @apply qp-text-sm qp-text-primary-500 qp-font-mono;
                    }
                }
            }

            .task-actions {
                @apply qp-flex qp-gap-2;
            }
        }

        .progress-section {
            @apply qp-space-y-2;

            .progress-header {
                @apply qp-flex qp-justify-between qp-items-center;

                .progress-label {
                    @apply qp-text-sm qp-font-medium qp-text-primary-700;
                }

                .progress-percentage {
                    @apply qp-text-sm qp-font-bold qp-text-primary-900;
                }
            }

            .progress-message {
                @apply qp-text-sm qp-text-primary-600 qp-italic;
            }
        }
    }

    .task-stats-grid {
        @apply qp-grid qp-grid-cols-2 md:qp-grid-cols-3 lg:qp-grid-cols-6 qp-gap-4 qp-p-4 qp-bg-primary-50 qp-rounded-lg qp-border;

        .stat-item {
            @apply qp-text-center;

            .stat-label {
                @apply qp-text-xs qp-text-primary-500 qp-font-medium qp-uppercase qp-tracking-wide qp-mb-1;
            }

            .stat-value {
                @apply qp-text-sm qp-font-semibold qp-text-primary-900;
            }
        }
    }

    .task-tabs {
        @apply qp-border qp-border-primary-200 qp-rounded-lg;

        .tab-header {
            @apply qp-flex qp-items-center qp-gap-2;

            i {
                @apply qp-text-primary-500;
            }
        }

        .details-content {
            @apply qp-space-y-6 qp-p-4;

            .detail-section {
                @apply qp-space-y-3;

                h4 {
                    @apply qp-text-lg qp-font-semibold qp-text-primary-900 qp-border-b qp-border-primary-200 qp-pb-2;
                }

                .detail-grid {
                    @apply qp-grid qp-grid-cols-1 md:qp-grid-cols-2 qp-gap-4;

                    .detail-item {
                        @apply qp-space-y-1;

                        label {
                            @apply qp-text-sm qp-font-medium qp-text-primary-600;
                        }

                        span {
                            @apply qp-block qp-text-sm qp-text-primary-900;

                            &.code {
                                @apply qp-font-mono qp-bg-primary-100 qp-px-2 qp-py-1 qp-rounded;
                            }
                        }
                    }
                }
            }
        }

        .steps-content {
            @apply qp-p-4;

            .steps-list {
                @apply qp-space-y-4;

                .step-item {
                    @apply qp-border qp-border-primary-200 qp-rounded-lg qp-p-4;

                    .step-header {
                        @apply qp-flex qp-items-center qp-gap-3;

                        .step-indicator {
                            @apply qp-w-8 qp-h-8 qp-rounded-full qp-flex qp-items-center qp-justify-center qp-text-white;

                            &.step-success {
                                @apply qp-bg-green-500;
                            }

                            &.step-error {
                                @apply qp-bg-red-500;
                            }

                            &.step-running {
                                @apply qp-bg-blue-500;
                            }

                            &.step-pending {
                                @apply qp-bg-gray-400;
                            }
                        }

                        .step-info {
                            @apply qp-flex-1;

                            .step-title {
                                @apply qp-font-medium qp-text-primary-900;
                            }

                            .step-meta {
                                @apply qp-flex qp-items-center qp-gap-2 qp-mt-1;

                                .step-order {
                                    @apply qp-text-sm qp-text-primary-500;
                                }
                            }
                        }
                    }

                    .step-result {
                        @apply qp-mt-3 qp-pt-3 qp-border-t qp-border-primary-200;
                    }
                }
            }
        }

        .logs-content {
            @apply qp-p-4 qp-space-y-4;

            .logs-toolbar {
                @apply qp-flex qp-justify-between qp-items-center qp-pb-3 qp-border-b qp-border-primary-200;

                .log-filters {
                    @apply qp-flex qp-gap-2;
                }

                .log-actions {
                    @apply qp-flex qp-gap-2;
                }
            }

            .logs-list {
                @apply qp-space-y-3 qp-max-h-96 qp-overflow-y-auto;

                .log-entry {
                    @apply qp-border qp-border-primary-200 qp-rounded qp-p-3;

                    .log-header {
                        @apply qp-flex qp-justify-between qp-items-center qp-mb-2;

                        .log-timestamp {
                            @apply qp-text-sm qp-text-primary-500 qp-font-mono;
                        }
                    }

                    .log-message {
                        @apply qp-text-sm qp-text-primary-900 qp-mb-2;
                    }

                    .log-extra {
                        @apply qp-mt-2 qp-pt-2 qp-border-t qp-border-primary-100;
                    }

                    &.log-error {
                        @apply qp-border-red-200 qp-bg-red-50;
                    }

                    &.log-warning {
                        @apply qp-border-yellow-200 qp-bg-yellow-50;
                    }
                }
            }
        }

        .result-content {
            @apply qp-p-4 qp-space-y-4;

            .result-header {
                @apply qp-flex qp-justify-between qp-items-center qp-pb-3 qp-border-b qp-border-primary-200;

                h4 {
                    @apply qp-text-lg qp-font-semibold qp-text-primary-900;
                }
            }

            .result-data {
                @apply qp-max-h-96 qp-overflow-y-auto;
            }
        }

        .error-content {
            @apply qp-p-4 qp-space-y-6;

            .error-message {
                @apply qp-space-y-3;

                h4 {
                    @apply qp-text-lg qp-font-semibold qp-text-red-700;
                }

                .error-text {
                    @apply qp-p-3 qp-bg-red-50 qp-border qp-border-red-200 qp-rounded qp-text-red-800;
                }
            }

            .error-traceback {
                @apply qp-space-y-3;

                h4 {
                    @apply qp-text-lg qp-font-semibold qp-text-red-700;
                }

                .traceback-text {
                    @apply qp-p-3 qp-bg-red-50 qp-border qp-border-red-200 qp-rounded qp-text-red-800 qp-text-sm qp-max-h-64 qp-overflow-y-auto;
                }
            }
        }

        .json-display {
            @apply qp-bg-primary-100 qp-p-3 qp-rounded qp-text-sm qp-font-mono qp-text-primary-800 qp-overflow-x-auto;
        }
    }
}
</style>