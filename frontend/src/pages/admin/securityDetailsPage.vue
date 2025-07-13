<template>
    <div class="security-details-page">
        <!-- Loading State -->
        <div v-if="isLoading" class="qp-text-center">
            <ProgressSpinner style="width: 40px; height: 40px;" stroke-width="2"></ProgressSpinner>
        </div>
    </div>
</template>
<script>
import { mapActions } from 'pinia';

import { useSecuritiesStore } from '@/stores/securities';
export default {
    data() {
        return {
            isLoading: false,
            securitiesI18n: this.$tm('pages.securityDetails'),
            security: null
        }
    },
    methods: {
        ...mapActions(useSecuritiesStore, ['fetchSecurity']),

        /**
         * Initializes and fetches data needed for the page.
         */
        init: async function () {
            await this.getSecurityDetails()
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

            this.$emit('page-info-update', { title: this.security.symbol, breadcrumb: this.security.symbol })

            this.isLoading = false
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
}
</style>