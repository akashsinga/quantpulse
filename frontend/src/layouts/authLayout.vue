<template>
    <div class="auth-layout">
        <!-- Background elements -->
        <div class="auth-background">
            <div class="bg-gradient"></div>
            <div class="bg-grid"></div>
            <div class="bg-orbs">
                <div class="orb orb-1"></div>
                <div class="orb orb-2"></div>
            </div>
        </div>

        <!-- Main container -->
        <div class="auth-container">
            <!-- Left panel - Branding -->
            <div class="auth-brand">
                <div class="brand-content">
                    <!-- Logo -->
                    <div class="logo-section">
                        <div class="logo-container">
                            <img src="@/assets/images/favicon.svg" alt="QuantPulse" class="logo" />
                        </div>
                        <div class="brand-text">
                            <h1 class="brand-name">QuantPulse</h1>
                            <div class="brand-badge">AI-Powered</div>
                        </div>
                    </div>

                    <!-- Tagline -->
                    <div class="brand-description">
                        <h2 class="tagline">Where Data Becomes Alpha</h2>
                        <p class="subtitle">
                            Sophisticated quantitative strategies meet cutting-edge machine learning.
                            Backtest with precision, execute with confidence, and outperform with intelligence.
                        </p>
                    </div>

                    <!-- Features list -->
                    <div class="features-list">
                        <div class="feature-item">
                            <div class="feature-icon">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                                    stroke-width="2">
                                    <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
                                </svg>
                            </div>
                            <span>Real-time signal generation</span>
                        </div>
                        <div class="feature-item">
                            <div class="feature-icon">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                                    stroke-width="2">
                                    <path d="M3 3v18h18" />
                                    <path d="M7 12l3-3 3 3 5-5" />
                                </svg>
                            </div>
                            <span>Historical strategy backtesting</span>
                        </div>
                        <div class="feature-item">
                            <div class="feature-icon">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                                    stroke-width="2">
                                    <path d="M12 2L2 7l10 5 10-5-10-5z" />
                                    <path d="M2 17l10 5 10-5" />
                                    <path d="M2 12l10 5 10-5" />
                                </svg>
                            </div>
                            <span>Portfolio risk management</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Right panel - Auth form -->
            <div class="auth-form-panel">
                <div class="form-container">
                    <div class="form-card">
                        <div class="form-content">
                            <!-- Dynamic content slot -->
                            <slot />
                        </div>

                        <div class="form-footer">
                            <slot name="footer">
                                <p class="footer-text">
                                    By continuing, you agree to our
                                    <a href="#" class="footer-link">Terms of Service</a>
                                    and
                                    <a href="#" class="footer-link">Privacy Policy</a>
                                </p>
                            </slot>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<style lang="postcss" scoped>
.auth-layout {
    @apply min-h-screen relative flex items-center justify-center p-4;
    background: #fafafa;
}

/* Background Elements */
.auth-background {
    @apply absolute inset-0 overflow-hidden;
}

.bg-gradient {
    @apply absolute inset-0;
    background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
}

.bg-grid {
    @apply absolute inset-0 opacity-30;
    background-image:
        linear-gradient(rgba(148, 163, 184, 0.1) 1px, transparent 1px),
        linear-gradient(90deg, rgba(148, 163, 184, 0.1) 1px, transparent 1px);
    background-size: 32px 32px;
}

.bg-orbs {
    @apply absolute inset-0;
}

.orb {
    @apply absolute rounded-full blur-3xl opacity-20;
}

.orb-1 {
    @apply w-96 h-96 bg-blue-400 -top-48 -left-48;
    animation: float 20s ease-in-out infinite;
}

.orb-2 {
    @apply w-80 h-80 bg-purple-400 -bottom-40 -right-40;
    animation: float 25s ease-in-out infinite reverse;
}

/* Main Container */
.auth-container {
    @apply relative w-full max-w-7xl mx-auto grid lg:grid-cols-2 gap-8 lg:gap-16 items-center;
}

/* Brand Panel */
.auth-brand {
    @apply space-y-8 text-center lg:text-left;
}

.brand-content {
    @apply space-y-8;
}

.logo-section {
    @apply flex items-center lg:items-start space-x-4;
}

.logo-container {
    @apply w-16 h-16 rounded-2xl bg-white shadow-lg flex items-center justify-center flex-shrink-0;
    box-shadow:
        0 10px 25px -5px rgba(0, 0, 0, 0.1),
        0 4px 6px -2px rgba(0, 0, 0, 0.05);
}

.logo {
    @apply w-10 h-10;
}

.brand-text {
    @apply flex flex-col;
}

.brand-name {
    @apply text-3xl lg:text-4xl font-bold text-gray-900 leading-tight;
}

.brand-badge {
    @apply inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 w-fit mt-1;
}

.brand-description {
    @apply space-y-4 max-w-md mx-auto lg:mx-0;
}

.tagline {
    @apply text-xl lg:text-2xl font-semibold text-gray-800;
}

.subtitle {
    @apply text-base text-gray-600 leading-relaxed;
}

.features-list {
    @apply space-y-3 max-w-sm mx-auto lg:mx-0;
}

.feature-item {
    @apply flex items-center space-x-3 text-sm text-gray-700;
}

.feature-icon {
    @apply w-8 h-8 rounded-lg bg-gray-100 flex items-center justify-center text-gray-600 flex-shrink-0;
}

/* Form Panel */
.auth-form-panel {
    @apply flex justify-center lg:justify-end;
}

.form-container {
    @apply w-full max-w-md;
}

.form-card {
    @apply bg-white rounded-2xl shadow-xl border border-gray-100 overflow-hidden;
    box-shadow:
        0 25px 50px -12px rgba(0, 0, 0, 0.08),
        0 10px 25px -5px rgba(0, 0, 0, 0.1);
}

.form-content {
    @apply p-8 pb-6;
}

.form-footer {
    @apply px-8 pb-8 pt-4 border-t border-gray-50;
}

.footer-text {
    @apply text-xs text-gray-500 text-center leading-relaxed;
}

.footer-link {
    @apply text-blue-600 hover:text-blue-700 transition-colors duration-200;
}

/* Animations */
@keyframes float {

    0%,
    100% {
        transform: translate(0, 0) scale(1);
    }

    33% {
        transform: translate(20px, -20px) scale(1.1);
    }

    66% {
        transform: translate(-20px, 20px) scale(0.9);
    }
}

/* Responsive Design */
@media (max-width: 1024px) {
    .auth-container {
        @apply grid-cols-1 gap-12 text-center;
    }

    .auth-brand {
        @apply order-2;
    }

    .auth-form-panel {
        @apply order-1 justify-center;
    }

    .brand-content {
        @apply space-y-6;
    }

    .brand-name {
        @apply text-3xl;
    }

    .tagline {
        @apply text-xl;
    }
}

@media (max-width: 640px) {
    .auth-layout {
        @apply p-4;
    }

    .auth-container {
        @apply gap-8;
    }

    .brand-name {
        @apply text-2xl;
    }

    .tagline {
        @apply text-lg;
    }

    .subtitle {
        @apply text-sm;
    }

    .orb-1,
    .orb-2 {
        @apply w-64 h-64;
    }
}

/* Dark mode support */
@media (prefers-color-scheme: dark) {
    .auth-layout {
        background: #0f172a;
    }

    .bg-gradient {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    }

    .bg-grid {
        background-image:
            linear-gradient(rgba(148, 163, 184, 0.05) 1px, transparent 1px),
            linear-gradient(90deg, rgba(148, 163, 184, 0.05) 1px, transparent 1px);
    }

    .logo-container {
        @apply bg-slate-800 shadow-2xl;
    }

    .brand-badge {
        @apply bg-blue-900 text-blue-200;
    }

    .brand-name {
        @apply text-white;
    }

    .tagline {
        @apply text-slate-200;
    }

    .subtitle {
        @apply text-slate-400;
    }

    .feature-item {
        @apply text-slate-300;
    }

    .feature-icon {
        @apply bg-slate-800 text-slate-400;
    }

    .form-card {
        @apply bg-slate-800 border-slate-700;
    }

    .form-footer {
        @apply border-slate-700;
    }

    .footer-text {
        @apply text-slate-500;
    }

    .footer-link {
        @apply text-blue-400 hover:text-blue-300;
    }
}
</style>