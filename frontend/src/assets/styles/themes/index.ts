// src/themes/quantpulse.js
import { definePreset } from '@primevue/themes'
import Aura from '@primevue/themes/aura'

const QuantPulse = definePreset(Aura, {
    semantic: {
        primary: {
            50: '#f8fafc',   // slate-50
            100: '#f1f5f9',  // slate-100  
            200: '#e2e8f0',  // slate-200
            300: '#cbd5e1',  // slate-300
            400: '#94a3b8',  // slate-400
            500: '#64748b',  // slate-500
            600: '#475569',  // slate-600
            700: '#334155',  // slate-700
            800: '#1e293b',  // slate-800
            900: '#0f172a',  // slate-900
            950: '#020617'   // slate-950
        }
    }
})

export default QuantPulse