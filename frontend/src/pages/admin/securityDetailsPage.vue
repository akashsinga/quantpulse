<template>
    <div class="security-details-page">
        <div v-if="isLoading" class="qp-text-center">
            <ProgressSpinner style="width: 40px; height: 40px;" stroke-width="2"></ProgressSpinner>
        </div>
        <div v-else-if="!security && !isLoading" class="not-found">
            <i class="ph ph-warning-circle"></i>
            <h3>{{ securitiesI18n.securityNotFound }}</h3>
            <p>{{ securitiesI18n.requestedSecurityNotFound }}</p>
            <Button :label="securitiesI18n.backToSecurities" icon="ph ph-arrow-left" @click="$router.push('/admin/securities')"></Button>
        </div>
        <div v-else-if="security" class="security-content">
            <div class="security-header">
                <div class="header-left">
                    <div class="security-identity">
                        <div class="symbol-section">
                            <h2 class="security-symbol">{{ security.symbol }}</h2>
                            <div class="badges-row">
                                <Badge :value="security.exchange?.code" severity="info" size="small"></Badge>
                                <Badge :value="security.security_type" :severity="securityTypeSeverity" size="small"></Badge>
                                <Badge :value="security.is_active ? $tm('common.active') : $tm('common.inactive')" :severity="security.is_active ? 'success' : 'danger'" size="small"></Badge>
                            </div>
                        </div>
                        <div class="security-details">
                            <div class="security-name">{{ security.name }}</div>
                            <div class="meta-info">
                                <div class="meta-item">
                                    <i class="ph ph-hash"></i>
                                    <strong>{{ securitiesI18n.labels.externalID }}: </strong> {{ security.external_id }}
                                </div>
                                <div v-if="security.isin" class="meta-item">
                                    <i class="ph ph-barcode"></i>
                                    <strong>{{ securitiesI18n.labels.isin }}: </strong> {{ security.isin }}
                                </div>
                                <div class="meta-item">
                                    <i class="ph ph-calendar-plus"></i>
                                    <strong>{{ securitiesI18n.labels.added }}: </strong> {{ getFormattedDate(security.created_at) }}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="header-right">
                    <div class="quick-stats">
                        <div class="stat-item">
                            <div class="stat-label">{{ securitiesI18n.labels.lotSize }}</div>
                            <div class="stat-value">{{ getFormattedNumber(security.lot_size) }}</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-label">{{ securitiesI18n.labels.tickSize }}</div>
                            <div class="stat-value">₹{{ security.tick_size }}</div>
                        </div>
                        <div v-if="isDerivative && security.expiration_date" class="stat-item">
                            <div class="stat-label">{{ securitiesI18n.labels.daysToExpiry }}</div>
                            <div class="stat-value" :class="{ 'expiry-warning': daysToExpiry <= 7 }">{{ `${daysToExpiry} ${$tm('common.days')}` }}</div>
                        </div>
                    </div>
                    <div class="admin-actions">
                        <SplitButton :icon="security.is_active ? 'ph ph-pause' : 'ph ph-play'" :severity="security.is_active ? 'danger' : 'success'" size="small" :model="splitButtonActions" :label="security.is_active ? $tm('common.deactivate') : $tm('common.activate')" @click="toggleSecurityStatus"></SplitButton>
                    </div>
                </div>
            </div>

            <!-- Key Metrics Cards -->
            <div class="metrics-row">
                <div class="metric-card" :class="{ 'metric-success': security.is_active }">
                    <div class="metric-icon">
                        <i class="ph ph-pulse"></i>
                    </div>
                    <div class="metric-content">
                        <h4>{{ securitiesI18n.metrics.tradingStatus }}</h4>
                        <p class="metric-value">{{ security.is_tradeable ? 'Tradeable' : 'Non-Tradeable' }}</p>
                        <small>{{ security.is_active ? securitiesI18n.metrics.activeInMarket : securitiesI18n.metrics.currentlyInactive }}</small>
                    </div>
                </div>
                <div class="metric-card" :class="{ 'metric-info': security.is_derivatives_eligible }">
                    <div class="metric-icon">
                        <i class="ph ph-lightning"></i>
                    </div>
                    <div class="metric-content">
                        <h4>{{ securitiesI18n.metrics.derivatives }}</h4>
                        <p class="metric-value">{{ security.is_derivatives_eligible ? securitiesI18n.metrics.eligible : securitiesI18n.metrics.notEligible }}</p>
                        <small>{{ derivativeTypes }}</small>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-icon">
                        <i class="ph ph-calculator"></i>
                    </div>
                    <div class="metric-content">
                        <h4>{{ securitiesI18n.metrics.minTradeValue }}</h4>
                        <p class="metric-value">₹{{ minTradeValue }}</p>
                        <small>{{ securitiesI18n.lotSizexTickSize }}</small>
                    </div>
                </div>
                <div v-if="security.sector" class="metric-card">
                    <div class="metric-icon">
                        <i class="ph ph-buildings"></i>
                    </div>
                    <div class="metric-content">
                        <h4>{{ securitiesI18n.metrics.classification }}</h4>
                        <p class="metric-value">{{ security.sector }}</p>
                        <small>{{ security.industry || securitiesI18n.metrics.industryNotClassified }}</small>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>
<script>
import { defineAsyncComponent } from 'vue';
import { mapActions } from 'pinia';

import { useSecuritiesStore } from '@/stores/securities';
export default {
    components: {
        securityOverview: defineAsyncComponent(() => import('@/components/securities/securityOverview.vue'))
    },
    data() {
        return {
            isLoading: false,
            securitiesI18n: this.$tm('pages.securityDetails'),
            selectedTab: 0,
            security: null,
            splitButtonActions: [
                { id: 'editDetails', icon: 'ph ph-pencil', command: () => this.editSecurity() },
                { id: 'viewMarketData', icon: 'ph ph-chart-line', command: () => this.viewMarketData() },
                { id: 'exportData', icon: 'ph ph-export', command: () => this.exportData() },
                { separator: true },
                { id: 'archiveSecurity', icon: 'ph ph-archive', command: () => this.archiveSecurity() }
            ]
        }
    },
    computed: {
        daysToExpiry() {
            if (!this.security?.expiration_date) return 0
            return Math.ceil((new Date(this.security.expiration_date) - new Date()) / (1000 * 60 * 60 * 24))
        },
        derivativeTypes() {
            let count = 0, types = []
            if (this.security?.has_futures) { count++; types.push(this.$tm('common.futures')) }
            if (this.security?.has_options) { count++; types.push(this.$tm('common.options')) }
            if (count === 0) return this.securitiesI18n.noDerivativesAvailable
            return `${types.join(' & ')} ${this.$tm('common.available')}`
        },
        isDerivative() {
            if (!this.security) return false;
            return ['FUTSTK', 'FUTIDX', 'FUTCOM', 'FUTCUR', 'OPTSTK', 'OPTIDX', 'OPTFUT', 'OPTCUR'].includes(this.security.security_type)
        },
        minTradeValue() {
            if (!this.security) return 'N/A'
            const lotSize = this.security.lot_size || 1
            const tickSize = parseFloat(this.security.tick_size) || 0.01
            return this.getFormattedNumber(lotSize * tickSize)
        },
        securityTypeSeverity() {
            return { 'EQUITY': 'success', 'INDEX': 'info', 'FUTSTK': 'warning', 'FUTIDX': 'warning', 'OPTSTK': 'danger', 'OPTIDX': 'danger' }[this.security.security_type] || 'info'
        }
    },
    methods: {
        ...mapActions(useSecuritiesStore, ['fetchSecurity']),

        /**
         * Initializes and fetches data needed for the page.
         */
        init: async function () {
            this.splitButtonActions = this.$lodash.map(this.splitButtonActions, (action) => ({ ...action, label: this.securitiesI18n.buttonActions[action.id] }))
            await this.getSecurityDetails()
        },

        /**
         * Formats date to readable format
         * @param {String} date
         * @returns {String}
         */
        getFormattedDate: function (date) {
            if (!date) return 'N/A'
            return new Date(date).toLocaleDateString('en-IN', { year: 'numeric', month: 'short', day: 'numeric' })
        },

        /**
         * Formats number to readable format.
         * @param {Number} number
         * @returns {*}
         */
        getFormattedNumber: function (number) {
            if (!number && number !== 0) return 'N/A';
            return new Intl.NumberFormat('en-IN').format(number)
        },

        /**
         * Fetches security details.
         */
        getSecurityDetails: async function () {
            this.isLoading = true

            const response = await this.fetchSecurity(this.$route.params.id)

            if (response.error) {
                this.$toast.add({ severity: 'error', summary: this.$tm('common.failed'), detail: this.securitiesI18n.messages.errorWhileFetchingSecurityDetails, life: 3000 })
                this.isLoading = false
                return
            }

            this.security = response.data

            this.$emit('page-info-update', { title: this.security.name, breadcrumb: this.security.symbol })

            this.isLoading = false
        },

        /**
         * Performs API call to toggle security status and displays appropriate message.
         */
        toggleSecurityStatus: function () {
            // TODO: Implement API call to toggle security active status.
            console.log('TODO: Toggle Security Status')
        }
    },
    mounted() {
        this.init()
    },
    watch: {
        '$route.params.id': {
            handler() {
                this.getSecurityDetails()
            }
        }
    }
}
</script>
<style lang="postcss" scoped>
.security-details-page {
    @apply qp-w-full qp-space-y-3;

    .not-found {
        @apply qp-flex qp-flex-col qp-items-center qp-justify-center qp-py-20 qp-space-y-4;

        i {
            @apply qp-text-6xl qp-text-secondary-300;
        }

        h3 {
            @apply qp-text-xl qp-font-semibold;
        }
    }

    .security-header {
        @apply qp-bg-white qp-rounded-md qp-border qp-border-primary-200 qp-p-4 qp-flex qp-justify-between qp-items-start;

        .header-left {
            @apply qp-flex-1;

            .security-identity {
                @apply qp-space-y-1.5;

                .symbol-section {
                    @apply qp-flex qp-items-center qp-gap-4;

                    .security-symbol {
                        @apply qp-text-2xl qp-font-bold qp-text-primary-900 qp-tracking-tight;
                    }

                    .badges-row {
                        @apply qp-flex qp-gap-2;
                    }
                }

                .security-details {
                    @apply qp-space-y-2;

                    .security-name {
                        @apply qp-text-lg qp-font-semibold qp-text-primary-700 qp-leading-tight;
                    }

                    .meta-info {
                        @apply qp-flex qp-flex-wrap qp-gap-4;

                        .meta-item {
                            @apply qp-flex qp-items-center qp-gap-2 qp-text-sm qp-text-primary-600;

                            i {
                                @apply qp-text-primary-400;
                            }

                            strong {
                                @apply qp-font-medium qp-text-primary-700;
                            }
                        }
                    }
                }
            }
        }

        .header-right {
            @apply qp-flex qp-flex-col qp-items-end qp-gap-4;

            .quick-stats {
                @apply qp-flex qp-gap-3;

                .stat-item {
                    @apply qp-text-center;

                    .stat-label {
                        @apply qp-block qp-text-xs qp-text-primary-500 qp-font-medium qp-uppercase qp-tracking-wide;
                    }

                    .stat-value {
                        @apply qp-block qp-text-lg qp-font-bold qp-text-primary-900;

                        &.expiry-warning {
                            @apply qp-text-red-600;
                        }
                    }
                }
            }

            .admin-actions {
                @apply qp-flex qp-gap-3;
            }
        }
    }

    .metrics-row {
        @apply qp-grid qp-grid-cols-1 md:qp-grid-cols-2 lg:qp-grid-cols-4 qp-gap-3 qp-mt-3;

        .metric-card {
            @apply qp-bg-white qp-rounded-md qp-border qp-border-primary-200 qp-p-4 qp-py-3 qp-flex qp-items-center qp-gap-4 qp-transition-all qp-duration-200;

            .metric-icon {
                @apply qp-w-12 qp-h-12 qp-rounded-md qp-bg-primary-100 qp-flex qp-items-center qp-justify-center qp-text-primary-600 qp-text-2xl qp-flex-shrink-0 qp-ring-1 qp-ring-primary-200;
            }

            .metric-content {
                @apply qp-min-w-0;

                h4 {
                    @apply qp-text-base qp-font-semibold qp-text-primary-900 qp-mb-1;
                }

                .metric-value {
                    @apply qp-text-lg qp-font-bold qp-text-primary-900;
                }

                small {
                    @apply qp-text-sm qp-text-primary-500;
                }
            }

            &.metric-success {
                @apply qp-border-green-200 qp-bg-green-50;

                .metric-icon {
                    @apply qp-bg-green-100 qp-text-green-600 qp-ring-green-200;
                }
            }

            &.metric-info {
                @apply qp-border-secondary-200 qp-bg-secondary-50;

                .metric-icon {
                    @apply qp-bg-secondary-100 qp-text-secondary-600 qp-ring-secondary-200;
                }
            }
        }
    }
}
</style>