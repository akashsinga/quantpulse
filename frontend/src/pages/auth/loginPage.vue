<template>
    <div class="qp-w-full">
        <div class="qp-text-center qp-mb-8">
            <h1 class="qp-text-3xl qp-font-bold qp-text-slate-900 qp-mb-3 qp-tracking-tight qp-leading-tight">{{ loginPageI18n.welcomeBack }}</h1>
            <p class="qp-text-slate-600 qp-text-base qp-leading-relaxed">{{ loginPageI18n.signInToContinue }}</p>
        </div>

        <div v-if="error" class="qp-bg-red-50 qp-border qp-border-red-200 qp-rounded-xl qp-p-4 qp-mb-4 qp-shadow-sm qp-backdrop-blur-sm">
            <div class="qp-flex qp-items-center">
                <div class="qp-flex-shrink-0">
                    <i class="ph ph-warning-circle qp-text-red-500 qp-text-lg"></i>
                </div>
                <div class="qp-ml-3">
                    <span class="qp-text-sm qp-text-red-800 qp-font-semibold">{{ error }}</span>
                </div>
            </div>
        </div>

        <form @submit.prevent="handleLogin" class="qp-space-y-6">
            <div class="qp-space-y-2">
                <label for="email" class="qp-block qp-text-sm qp-font-semibold qp-text-slate-800 qp-tracking-wide">{{ loginPageI18n.fields.emailAddress }}</label>
                <IconField>
                    <InputIcon class="ph ph-envelope"></InputIcon>
                    <InputText v-model="loginForm.email" class="qp-w-full qp-transition-all qp-duration-200 qp-ease-in-out" id="email" type="email" size="small" :placeholder="loginPageI18n.enterYourEmailAddress" :invalid="!!errors.email" @input="clearErrors" />
                </IconField>
                <small v-if="errors.email" class="qp-text-red-600 qp-font-semibold qp-text-xs qp-block qp-mt-1">{{ errors.email }}</small>
            </div>

            <div class="qp-space-y-2">
                <label for="password" class="qp-block qp-text-sm qp-font-semibold qp-text-slate-800 qp-tracking-wide">{{ loginPageI18n.fields.password }}</label>
                <IconField>
                    <InputIcon class="ph ph-lock"></InputIcon>
                    <InputText v-model="loginForm.password" class="qp-w-full qp-transition-all qp-duration-200 qp-ease-in-out" id="password" size="small" type="password" :placeholder="loginPageI18n.enterYourPassword" :invalid="!!errors.password" :feedback="false" toggleMask @input="clearErrors" />
                </IconField>
                <small v-if="errors.password" class="qp-text-red-600 qp-font-semibold qp-text-xs qp-block qp-mt-1">{{ errors.password }}</small>
            </div>

            <div class="qp-pt-4">
                <Button type="submit" :loading="isLoading" :disabled="!isFormValid" class="qp-w-full qp-transform qp-transition-all qp-duration-200 qp-ease-in-out hover:qp-scale-[1.02] hover:qp-shadow-lg active:qp-scale-[0.98]" size="small">
                    <template #default>
                        <span v-if="!isLoading" class="qp-flex qp-items-center qp-justify-center qp-font-semibold qp-tracking-wide">{{ loginPageI18n.signIn }}</span>
                        <span v-else class="qp-flex qp-items-center qp-justify-center qp-font-semibold qp-tracking-wide">
                            <i class="ph ph-spinner animate-spin qp-mr-2"></i>
                            {{ loginPageI18n.signingIn }}
                        </span>
                    </template>
                </Button>
            </div>
        </form>

        <div class="qp-mt-8 qp-pt-6 qp-border-t qp-border-slate-200/60">
            <div class="qp-flex qp-justify-between qp-items-center">
                <a href="#" class="qp-text-sm qp-text-blue-600 hover:qp-text-blue-700 qp-font-semibold qp-transition-all qp-duration-300 qp-ease-out qp-transform hover:qp--translate-y-0.5 hover:qp-shadow-sm qp-rounded qp-px-2 qp-py-1 hover:qp-bg-blue-50/50">{{ loginPageI18n.forgotPassword }}</a>
                <a href="#" class="qp-text-sm qp-text-slate-700 hover:qp-text-slate-900 qp-font-semibold qp-transition-all qp-duration-300 qp-ease-out qp-transform hover:qp--translate-y-0.5 hover:qp-shadow-sm qp-rounded qp-px-2 qp-py-1 hover:qp-bg-slate-50/50">{{ loginPageI18n.createAccount }}</a>
            </div>
        </div>
    </div>
</template>

<script>
import { mapActions, mapState } from 'pinia'
import { useAuthStore } from '@/stores/auth'

export default {
    name: 'LoginPage',
    data() {
        return {
            loginForm: { email: '', password: '' },
            errors: { email: '', password: '' },
            loginPageI18n: this.$tm('pages.login')
        }
    },

    computed: {
        ...mapState(useAuthStore, ['error', 'isLoading']),
        isFormValid() {
            return this.loginForm.email && this.loginForm.password && this.isValidEmail(this.loginForm.email)
        }
    },

    watch: {
        // Clear auth store error when user starts typing
        'loginForm.email'() {
            this.clearError()
        },
        'loginForm.password'() {
            this.clearError()
        }
    },

    methods: {
        ...mapActions(useAuthStore, ['clearError', 'login']),

        isValidEmail: function (email) {
            return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
        },

        clearErrors: function () {
            this.errors = { email: '', password: '' }
            this.clearError()
        },

        validateForm: function () {
            this.clearErrors()
            let isValid = true

            if (!this.loginForm.email) {
                this.errors.email = 'Email is required'
                isValid = false
            } else if (!this.isValidEmail(this.loginForm.email)) {
                this.errors.email = 'Please enter a valid email'
                isValid = false
            }

            if (!this.loginForm.password) {
                this.errors.password = 'Password is required'
                isValid = false
            }

            return isValid
        },

        handleLogin: async function () {
            if (!this.validateForm()) return

            const result = await this.login({ email: this.loginForm.email, password: this.loginForm.password })

            if (!result.error) {
                // Redirect to dashboard or intended page
                const redirectTo = this.$route.query.redirect || '/dashboard'
                await this.$router.push(redirectTo)
            }
        }
    }
}
</script>

<style lang="postcss" scoped>
@keyframes spin {
    from {
        transform: rotate(0deg);
    }

    to {
        transform: rotate(360deg);
    }
}

.animate-spin {
    animation: spin 1s linear infinite;
}

*:focus {
    outline: none;
}
</style>