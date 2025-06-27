<template>
  <div class="w-full">
    <div class="text-center mb-8">
      <h1 class="text-3xl font-bold text-slate-900 mb-3 tracking-tight leading-tight">Welcome back</h1>
      <p class="text-slate-600 text-base leading-relaxed">Sign in to your account to continue</p>
    </div>

    <div v-if="errors.general" class="bg-red-50 border border-red-200 rounded-xl p-4 mb-4 shadow-sm backdrop-blur-sm">
      <div class="flex items-center">
        <div class="flex-shrink-0">
          <i class="ph ph-warning-circle text-red-500 text-lg"></i>
        </div>
        <div class="ml-3">
          <span class="text-sm text-red-800 font-semibold">{{ errors.general }}</span>
        </div>
      </div>
    </div>

    <form @submit.prevent="handleLogin" class="space-y-6">
      <div class="space-y-2">
        <label for="email" class="block text-sm font-semibold text-slate-800 tracking-wide">Email Address</label>
        <IconField>
          <InputIcon class="ph ph-envelope"></InputIcon>
          <InputText v-model="loginForm.email" class="w-full transition-all duration-200 ease-in-out" id="email" type="email" size="small" placeholder="Enter your email address" :invalid="!!errors.email" />
        </IconField>
        <small v-if="errors.email" class="text-red-600 font-semibold text-xs block mt-1">{{ errors.email }}</small>
      </div>

      <div class="space-y-2">
        <label for="password" class="block text-sm font-semibold text-slate-800 tracking-wide">Password</label>
        <IconField>
          <InputIcon class="ph ph-lock"></InputIcon>
          <InputText v-model="loginForm.password" class="w-full transition-all duration-200 ease-in-out" id="password" size="small" type="password" placeholder="Enter your password" :invalid="!!errors.password" :feedback="false" toggleMask />
        </IconField>
        <small v-if="errors.password" class="text-red-600 font-semibold text-xs block mt-1">{{ errors.password }}</small>
      </div>

      <div class="pt-4">
        <Button type="submit" :loading="isLoading" :disabled="!isFormValid" class="w-full transform transition-all duration-200 ease-in-out hover:scale-[1.02] hover:shadow-lg active:scale-[0.98]" size="small">
          <template #default>
            <span v-if="!isLoading" class="flex items-center justify-center font-semibold tracking-wide">Sign in</span>
            <span v-else class="flex items-center justify-center font-semibold tracking-wide">
              <i class="ph ph-spinner animate-spin mr-2"></i>
              Signing in...
            </span>
          </template>
        </Button>
      </div>
    </form>

    <div class="mt-8 pt-6 border-t border-slate-200/60">
      <div class="flex justify-between items-center">
        <a href="#" class="text-sm text-blue-600 hover:text-blue-700 font-semibold transition-all duration-300 ease-out transform hover:-translate-y-0.5 hover:shadow-sm rounded px-2 py-1 hover:bg-blue-50/50">
          Forgot password?
        </a>
        <a href="#" class="text-sm text-slate-700 hover:text-slate-900 font-semibold transition-all duration-300 ease-out transform hover:-translate-y-0.5 hover:shadow-sm rounded px-2 py-1 hover:bg-slate-50/50">
          Create account
        </a>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'LoginPage',
  data() {
    return {
      isLoading: false,
      loginForm: {
        email: '',
        password: ''
      },
      errors: {
        email: '',
        password: '',
        general: ''
      }
    }
  },

  computed: {
    /**
     * Check if form is valid for submission
     * @returns {boolean} True if form is valid
     */
    isFormValid: function () {
      return this.loginForm.email && this.loginForm.password && this.isValidEmail(this.loginForm.email)
    }
  },

  watch: {
    /**
     * Clear errors when user types
     */
    'loginForm.email': function () {
      this.clearErrors()
    },
    'loginForm.password': function () {
      this.clearErrors()
    }
  },

  methods: {
    /**
     * Validate email format
     * @param {string} email - Email to validate
     * @returns {boolean} True if valid
     */
    isValidEmail: function (email) {
      return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
    },

    /**
     * Clear all form errors
     */
    clearErrors: function () {
      this.errors = {
        email: '',
        password: '',
        general: ''
      }
    },

    /**
     * Validate form fields
     * @returns {boolean} True if form is valid
     */
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

    /**
     * Handle login form submission
     */
    handleLogin: async function () {
      if (!this.validateForm()) return

      this.isLoading = true

      try {
        const formData = new FormData()
        formData.append('username', this.loginForm.email)
        formData.append('password', this.loginForm.password)

        const response = await fetch('/api/v1/auth/login', {
          method: 'POST',
          body: formData
        })

        const data = await response.json()

        if (!response.ok) {
          throw new Error(data.detail || 'Login failed')
        }

        // Store auth data
        localStorage.setItem('access_token', data.access_token)
        localStorage.setItem('token_type', data.token_type)
        localStorage.setItem('expires_at', data.expires_at)
        localStorage.setItem('user', JSON.stringify(data.user))

        // Redirect to dashboard
        const redirectTo = this.$route.query.redirect || '/dashboard'
        await this.$router.push(redirectTo)

      } catch (error) {
        console.error('Login error:', error)
        this.errors.general = 'Invalid email or password'
      } finally {
        this.isLoading = false
      }
    }
  }
}
</script>

<style lang="postcss" scoped>
/* Custom animations for elements not covered by Tailwind */
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

/* Focus states */
*:focus {
  outline: none;
}
</style>