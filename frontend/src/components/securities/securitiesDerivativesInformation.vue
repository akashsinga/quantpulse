<template>
    <div>
        <!-- If Security is a derivative contract -->
        <div v-if="isDerivative" class="admin-card">
            <div class="card-title">
                <i class="ph ph-file-text"></i>
                {{ derivativesInformationI18n.contractSpecifications }}
            </div>
            <div class="card-content">
                <div class="contract-specs">
                    <div class="spec-row">
                        <div class="spec-group">
                            <label>{{ derivativesInformationI18n.labels.contractType }}</label>
                            <div class="spec-value">
                                <Badge :value="contractType"></Badge>
                            </div>
                        </div>

                        <div v-if="security.underlying_symbol" class="spec-group">
                            <label>{{ derivativesInformationI18n.labels.underlyingAsset }}</label>
                            <div class="spec-value">
                                <code>{{ security.underlying_symbol }}</code>
                                <Button icon="ph ph-arrow-square-out" size="small" text></Button>
                            </div>
                        </div>

                        <div v-if="security.expiration_date" class="spec-group">
                            <label>{{ derivativesInformationI18n.labels.expiration_date }}</label>
                            <span class="spec-value">
                                {{ getFormattedDate(security.expiration_date) }}
                                <Badge :value="`${daysToExpiry} days`" :severity="daysToExpiry <= 7 ? 'danger' : 'info'"></Badge>
                            </span>
                        </div>
                    </div>
                    <div class="spec-row">
                        <div v-if="security.contract_month" class="spec-group">
                            <label>{{ derivativesInformationI18n.labels.contractMonth }}</label>
                            <span class="spec-value">{{ security.contract_month }}</span>
                        </div>

                        <div v-if="security.settlement_type" class="spec-group">
                            <label>{{ derivativesInformationI18n.labels.settlementType }}</label>
                            <span class="spec-value">
                                <Badge :value="security.settlement_type" severity="secondary"></Badge>
                            </span>
                        </div>

                        <div v-if="security.contract_size" class="spec-group">
                            <label>{{ derivativesInformationI18n.labels.contractSize }}</label>
                            <span class="spec-value">{{ getFormattedNumber(security.contract_size) }}</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <!-- If security HAS derivatives -->
        <div v-else class="admin-grid">
            <div class="admin-card">
                <div class="card-title">
                    <i class="ph ph-chart-line"></i>
                    {{ derivativesInformationI18n.availableDerivativeInstruments }}
                </div>
                <div class="card-content">
                    <div class="instruments-list">
                        <div class="instrument-item" :class="{ 'available': security.has_futures }">
                            <div class="instrument-icon">
                                <i class="ph ph-trend-up"></i>
                            </div>
                            <div class="instrument-info">
                                <h4>{{ derivativesInformationI18n.labels.futuresContracts }}</h4>
                                <p>{{ security.has_futures ? $tm('common.availableForTrading') : $tm('common.notAvailable') }}</p>
                            </div>
                            <div class="instrument-status">
                                <Badge :value="security.has_futures ? $tm('common.available') : 'N/A'" :severity="security.has_futures ? 'success' : 'secondary'"></Badge>
                            </div>
                        </div>

                        <div class="instrument-item" :class="{ 'available': security.has_options }">
                            <div class="instrument-icon">
                                <i class="ph ph-target"></i>
                            </div>
                            <div class="instrument-info">
                                <h4>{{ derivativesInformationI18n.labels.optionsContracts }}</h4>
                                <p>{{ security.has_options ? $tm('common.availableForTrading') : $tm('common.notAvailable') }}</p>
                            </div>
                            <div class="instrument-status">
                                <Badge :value="security.has_options ? $tm('common.available') : 'N/A'" :severity="security.has_options ? 'success' : 'secondary'"></Badge>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="admin-card">
                <div class="card-title">
                    <i class="ph ph-list"></i>
                    {{ derivativesInformationI18n.contractManagement }}
                </div>
                <div class="card-content">
                    <div class="contract-actions">
                        <div class="action-item">
                            <Button icon="ph ph-list" :label="derivativesInformationI18n.contractActions.viewAllContracts" severity="info" size="small" outlined></Button>
                            <small>{{ derivativesInformationI18n.contractActions.viewAllContractsSubtitle }}</small>
                        </div>
                        <div class="action-item">
                            <Button icon="ph ph-calendar" :label="derivativesInformationI18n.contractActions.expiryManagement" severity="warn" size="small" outlined></Button>
                            <small>{{ derivativesInformationI18n.contractActions.expiryManagementSubtitle }}</small>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
export default {
    props: {
        security: { type: Object, required: true },
        step: { type: String, required: true }
    },
    computed: {
        contractType() {
            if (!this.security) return 'N/A'
            return this.security.security_type.startsWith('FUT') ? this.$tm('common.futures') : (this.security.security_type.startsWith('OPT') ? this.$tm('common.options') : this.security.security_type)
        },
        daysToExpiry() {
            if (!this.security?.expiration_date) return 0
            return Math.ceil((new Date(this.security.expiration_date) - new Date()) / (1000 * 60 * 60 * 24))
        },
        isDerivative() {
            if (!this.security) return false
            return ['FUTSTK', 'FUTIDX', 'FUTCOM', 'FUTCUR', 'OPTSTK', 'OPTIDX', 'OPTFUT', 'OPTCUR'].includes(this.security.security_type);
        }
    },
    data() {
        return {
            derivativesInformationI18n: this.$tm('components.securitiesDerivativesInformation')
        }
    },
    methods: {
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
        }
    }
}
</script>
<style lang="postcss" scoped>
.card-content {
    @apply qp-p-5;
}

.contract-specs {
    @apply qp-space-y-6;

    .spec-row {
        @apply qp-grid qp-grid-cols-1 md:qp-grid-cols-3 qp-gap-6;
    }

    .spec-group {
        @apply qp-text-center qp-p-4 qp-bg-primary-50 qp-rounded-lg;

        label {
            @apply qp-block qp-text-sm qp-font-medium qp-text-primary-600 qp-mb-2;
        }

        .spec-value {
            @apply qp-flex qp-items-center qp-justify-center qp-gap-2 qp-text-sm qp-font-medium qp-text-primary-900;

            code {
                @apply qp-px-2 qp-py-1 qp-bg-white qp-rounded;
            }
        }
    }
}

.instruments-list {
    @apply qp-space-y-4;

    .instrument-item {
        @apply qp-flex qp-items-center qp-gap-4 qp-p-4 qp-bg-primary-50 qp-rounded-lg qp-border qp-border-primary-200;

        &.available {
            @apply qp-bg-green-50 qp-border-green-200;

            .instrument-icon {
                @apply qp-bg-green-100 qp-text-green-600;
            }
        }

        .instrument-icon {
            @apply qp-w-12 qp-h-12 qp-rounded-lg qp-bg-primary-100 qp-flex qp-items-center qp-justify-center qp-text-primary-600 qp-text-xl;
        }

        .instrument-info {
            @apply qp-flex-1;

            h4 {
                @apply qp-text-sm qp-font-semibold qp-text-primary-900 qp-mb-1;
            }

            p {
                @apply qp-text-xs qp-text-primary-600;
            }
        }

        .instrument-status {
            @apply qp-flex-shrink-0;
        }
    }
}

/* Contract Actions */
.contract-actions {
    @apply qp-space-y-4;

    .action-item {
        @apply qp-flex qp-flex-col qp-gap-2;

        small {
            @apply qp-text-xs qp-text-primary-500;
        }
    }
}
</style>