// tailwind.config.js
export default {
    prefix: 'qp-',
    content: ["./index.html", "./src/**/*.{vue,js,ts,jsx,tsx,scss}"],
    darkMode: 'selector',
    theme: {
        extend: {
            colors: {
                primary: {
                    50: 'var(--p-primary-50)',
                    100: 'var(--p-primary-100)',
                    200: 'var(--p-primary-200)',
                    300: 'var(--p-primary-300)',
                    400: 'var(--p-primary-400)',
                    500: 'var(--p-primary-500)',
                    600: 'var(--p-primary-600)',
                    700: 'var(--p-primary-700)',
                    800: 'var(--p-primary-800)',
                    900: 'var(--p-primary-900)',
                    950: 'var(--p-primary-950)'
                },
                secondary: {
                    50: 'var(--p-secondary-50)',
                    100: 'var(--p-secondary-100)',
                    200: 'var(--p-secondary-200)',
                    300: 'var(--p-secondary-300)',
                    400: 'var(--p-secondary-400)',
                    500: 'var(--p-secondary-500)',
                    600: 'var(--p-secondary-600)',
                    700: 'var(--p-secondary-700)',
                    800: 'var(--p-secondary-800)',
                    900: 'var(--p-secondary-900)',
                    950: 'var(--p-secondary-950)',
                }
            }
        }
    },
    plugins: [],
    safelist: {}
}