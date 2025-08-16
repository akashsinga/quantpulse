<template>
    <div class="admin-grid">
        <div class="admin-card">
            <div class="card-title">
                <i class="ph ph-identification-card"></i>
                {{ coreInformationI18n.primaryDetails }}
            </div>
            <div class="card-content">
                <div class="detail-table">
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.securitySymbol }}</div>
                        <div class="value">
                            <code>{{ security.symbol }}</code>
                            <Button icon="ph ph-copy" size="small" text @click="copyToClipboard(security.symbol)"></Button>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.fullName }}</div>
                        <div class="value">{{ security.name }}</div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.isinCode }}</div>
                        <div class="value">
                            <code v-if="security.isin">{{ security.isin }}</code>
                            <span v-else class="not-available">N/A</span>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.externalID }}</div>
                        <div class="value">
                            <code>{{ security.external_id }}</code>
                            <Button icon="ph ph-copy" size="small" text @click="copyToClipboard(security.external_id)"></Button>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.internalID }}</div>
                        <div class="value">
                            <code>{{ security.id }}</code>
                            <Button icon="ph ph-copy" size="small" text @click="copyToClipboard(security.id)"></Button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <div class="admin-card">
            <div class="card-title">
                <i class="ph ph-bank"></i>
                {{ coreInformationI18n.exchangeInformation }}
            </div>
            <div class="card-content">
                <div class="detail-table">
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.exchange }}</div>
                        <div class="value">
                            {{ security.exchange?.name }}
                            <Badge :value="security.exchange?.code" severity="info"></Badge>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.country }}</div>
                        <div class="value">{{ security.exchange?.country }}</div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.currency }}</div>
                        <div class="value">{{ security.exchange?.currency }}</div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.tradingHours }}</div>
                        <div class="value">{{ security.exchange?.trading_hours_start || 'N/A' }} - {{ security.exchange?.trading_hours_end || 'N/A' }}</div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.timezone }}</div>
                        <div class="value">{{ security.exchange?.timezone || 'Asia/Kolkata' }}</div>
                    </div>
                </div>
            </div>
        </div>
        <div class="admin-card">
            <div class="card-title">
                <i class="ph ph-tree-structure"></i>
                {{ coreInformationI18n.classification }}
            </div>
            <div class="card-content">
                <div class="detail-table">
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.securityType }}</div>
                        <div class="value">
                            <Badge :value="security.security_type"></Badge>
                            <span class="qp-text-sm qp-text-primary-500">{{ securityTypesI18n[security.security_type] }}</span>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.marketSegment }}</div>
                        <div class="value">
                            <Badge :value="security.segment" severity="secondary"></Badge>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.sector }}</div>
                        <div class="value">{{ security.sector || 'N/A' }}</div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.industry }}</div>
                        <div class="value">{{ security.industry || 'N/A' }}</div>
                    </div>
                </div>
            </div>
        </div>
        <div class="admin-card">
            <div class="card-title">
                <i class="ph ph-gear"></i>
                {{ coreInformationI18n.systemInformation }}
            </div>
            <div class="card-content">
                <div class="detail-table">
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.createdAt }}</div>
                        <div class="value">
                            {{ getFormattedDateTime(security.created_at) }}
                            <small class="time-ago">({{ getElapsedTime(security.created_at) }})</small>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.lastUpdated }}</div>
                        <div class="value">
                            {{ getFormattedDateTime(security.updated_at) }}
                            <small class="time-ago">({{ getElapsedTime(security.updated_at) }})</small>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.dataSource }}</div>
                        <div class="value">
                            <Badge value="Dhan HQ API" severity="info"></Badge>
                        </div>
                    </div>
                    <div class="detail-row">
                        <div class="label">{{ coreInformationI18n.labels.lastSync }}</div>
                        <div class="value">
                            {{ getFormattedDateTime(security.updated_at) }}
                            <Button icon="ph ph-arrow-clockwise" :label="coreInformationI18n.syncNow" size="small" text></Button>
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
            coreInformationI18n: this.$tm('components.securitiesCoreInformation'),
            securityTypesI18n: this.$tm('pages.securities.securityTypes')
        }
    },
    methods: {
        ...mapActions(useGlobalStore, ['getFormattedDateTime', 'getElapsedTime']),
        /**
         * Copy text to clipboard.
         * @param {String} text
         */
        copyToClipboard: function (text) {
            navigator.clipboard.writeText(text).then(() => {
                this.$toast.add({ severity: 'success', summary: this.$tm('common.copied'), detail: this.$tm('common.textCopiedToClipboard'), life: 2000 });
            })
        }
    }
}
</script>
