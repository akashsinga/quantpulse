import './assets/styles/main.scss'

import { createApp } from 'vue'
import { createPinia } from 'pinia'
import PrimeVue from 'primevue/config'
import QuantPulse from '@/assets/styles/themes/index'

import App from './App.vue'
import router from './router'
import plugins from '@/plugins'
import { $http, $lodash } from '@/plugins'

const app = createApp(App)
const pinia = createPinia()

pinia.use(({ store }) => {
    store.$http = $http
    store.$lodash = $lodash
})

app.use(createPinia())
app.use(router)
app.use(plugins)
app.use(PrimeVue, {
    theme: {
        preset: QuantPulse,
        options: {
            prefix: 'p',
            darkModeSelector: '.dark',
            cssLayer: false
        }
    }
})

app.mount('#app')