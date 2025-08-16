<template>
    <div class="admin-grid trading-grid">
        <div class="admin-card">
            <div class="card-title">
                <i class="ph ph-calculator"></i>
                {{ tradingConfigurationI18n.tradingParameters }}
            </div>
            <div class="card-content">
                <div class="trading-params-grid">
                    <div class="param-group">
                        <div class="param-item">
                            <label>{{ tradingConfigurationI18n.labels.lotSize }}</label>
                            <div class="param-value">
                                <div class="value-main">{{ getFormattedNumber(security.lot_size) }}</div>
                                <div class="value-unit">{{ tradingConfigurationI18n.shares }}</div>
                            </div>
                            <div class="param-description">{{ tradingConfigurationI18n.minimumQuantity }}</div>
                        </div>
                        <div class="param-item">
                            <label>{{ tradingConfigurationI18n.labels.tickSize }}</label>
                            <div class="param-value">
                                <div class="value-main">₹{{ security.tick_size }}</div>
                                <div class="value-unit">{{ tradingConfigurationI18n.minimumPriceMovement }}</div>
                            </div>
                            <div class="param-description">{{ tradingConfigurationI18n.smallestPriceIncrementAllowed }}</div>
                        </div>
                        <div class="param-item">
                            <label>{{ tradingConfigurationI18n.labels.minTradeValue }}</label>
                            <div class="param-value">
                                <div class="value-main">₹{{ getMinTradeValue() }}</div>
                                <div class="value-unit">{{ tradingConfigurationI18n.minimumInvestment }}</div>
                            </div>
                            <div class="param-description">{{ tradingConfigurationI18n.lotSizexTickSize }}</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="admin-card">
            <div class="card-title">
                <i class="ph ph-shield-check"></i>
                {{ tradingConfigurationI18n.tradingStatusPermissions }}
            </div>
            <div class="card-content">
                <div class="status-list">
                    <div class="status-item" :class="{ 'status-active': security.is_active }">
                        <div class="status-indicator"></div>
                        <div class="status-content">
                            <div class="status-label">{{ tradingConfigurationI18n.labels.activeStatus }}</div>
                            <div class="status-value">{{ security.is_active ? $tm('common.active') : $tm('common.inactive') }}</div>
                        </div>
                        <Button :icon="security.is_active ? 'ph ph-pause' : 'ph ph-play'" :severity="security.is_active ? 'danger' : 'success'" size="small" outlined></Button>
                    </div>
                    <div class="status-item" :class="{ 'status-active': security.is_tradeable }">
                        <div class="status-indicator"></div>
                        <div class="status-content">
                            <div class="status-label">{{ tradingConfigurationI18n.labels.tradingPermission }}</div>
                            <div class="status-value">{{ security.is_tradeable ? $tm('common.allowed') : $tm('common.restricted') }}</div>
                        </div>
                        <Button icon="ph ph-gear" severity="secondary" size="small" outlined></Button>
                    </div>
                    <div class="status-item" :class="{ 'status-active': security.is_derivatives_eligible }">
                        <div class="status-indicator"></div>
                        <div class="status-content">
                            <span class="status-label">{{ tradingConfigurationI18n.labels.derivativesEligible }}</span>
                            <span class="status-value">{{ security.is_derivatives_eligible ? $tm('common.yes') : $tm('common.no') }}</span>
                        </div>
                        <Button icon="ph ph-info" severity="info" size="small" outlined></Button>
                    </div>
                </div>
            </div>
        </div>

        <div class="admin-card">
            <div class="card-title">
                <i class="ph ph-storefront"></i>
                {{ tradingConfigurationI18n.marketAvailability }}
            </div>
            <div class="card-content">
                <div class="availability-grid">
                    <div class="availability-item">
                        <div class="availability-header">
                            <i class="ph ph-chart-line"></i>
                            <span>{{ tradingConfigurationI18n.labels.futuresMarket }}</span>
                        </div>
                        <div class="availability-status" :class="{ 'available': security.has_futures }">
                            {{ security.has_futures ? $t('common.available') : $t('common.unavailable') }}
                        </div>
                    </div>

                    <div class="availability-item">
                        <div class="availability-header">
                            <i class="ph ph-chart-line"></i>
                            <span>{{ tradingConfigurationI18n.labels.optionsMarket }}</span>
                        </div>
                        <div class="availability-status" :class="{ 'available': security.has_options }">
                            {{ security.has_options ? $t('common.available') : $t('common.unavailable') }}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { mapActions } from 'pinia';
import { useGlobalStore } from '@/stores/global'
export default {
    props: {
        security: { type: Object, required: true },
        step: { type: String, required: true }
    },
    data() {
        return {
            tradingConfigurationI18n: this.$tm('components.securitiesTradingConfiguration')
        }
    },
    methods: {
        ...mapActions(useGlobalStore, ['getFormattedNumber']),
        /**
        * Calculates minimum trade value
        */
        getMinTradeValue: function () {
            if (!this.security) return 'N/A';
            const lotSize = this.security.lot_size || 1;
            const tickSize = parseFloat(this.security.tick_size) || 0.01;
            return this.getFormattedNumber(lotSize * tickSize);
        }
    }
}
</script>

<style lang="postcss" scoped>
.admin-grid {
    &.trading-grid {
        @apply qp-grid-cols-1;
    }

    .trading-params-grid {
        @apply qp-space-y-6;

        .param-group {
            @apply qp-grid qp-grid-cols-1 md:qp-grid-cols-3 qp-gap-4 qp-p-5;

            .param-item {
                @apply qp-text-center qp-p-4 qp-bg-primary-50 qp-rounded-md qp-border qp-border-primary-200;

                label {
                    @apply qp-block qp-text-sm qp-font-medium qp-text-primary-600 qp-mb-2;
                }

                .param-value {
                    @apply qp-mb-2;

                    .value-main {
                        @apply qp-block qp-text-2xl qp-font-bold qp-text-primary-900
                    }

                    .value-unit {
                        @apply qp-text-xs qp-text-primary-500;
                    }
                }

                .param-description {
                    @apply qp-text-xs qp-text-primary-700;
                }
            }
        }
    }

    .status-list {
        @apply qp-space-y-4 qp-p-5;

        .status-item {
            @apply qp-flex qp-items-center qp-justify-between qp-p-4 qp-bg-primary-50 qp-rounded-md qp-border qp-border-primary-200;

            .status-indicator {
                @apply qp-w-3 qp-h-3 qp-rounded-full qp-bg-primary-300 qp-flex-shrink-0;
            }

            .status-content {
                @apply qp-flex-1 qp-ml-3;

                .status-label {
                    @apply qp-block qp-text-sm qp-font-medium qp-text-primary-700;
                }

                .status-value {
                    @apply qp-text-xs qp-text-primary-500;
                }
            }

            &.status-active {
                @apply qp-bg-green-50 qp-border-green-200;

                .status-indicator {
                    @apply qp-bg-green-500;
                }
            }
        }
    }

    .availability-grid {
        @apply qp-space-y-4 qp-p-5;

        .availability-item {
            @apply qp-flex qp-items-center qp-justify-between qp-p-4 qp-bg-primary-50 qp-rounded-lg;

            .availability-header {
                @apply qp-flex qp-items-center qp-gap-3;

                i {
                    @apply qp-text-primary-500;
                }

                span {
                    @apply qp-font-medium qp-text-primary-700;
                }
            }

            .availability-status {
                @apply qp-px-3 qp-py-1 qp-rounded-full qp-text-sm qp-font-medium qp-bg-primary-200 qp-text-primary-700 qp-capitalize;

                &.available {
                    @apply qp-bg-green-200 qp-text-green-800;
                }
            }
        }
    }
}
</style>