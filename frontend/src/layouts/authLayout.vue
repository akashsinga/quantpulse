<template>
    <div class="auth-layout">
        <!-- Enhanced background elements -->
        <div class="auth-background">
            <div class="bg-gradient"></div>
            <div class="bg-mesh"></div>
            <div class="bg-orbs">
                <div class="orb orb-1"></div>
                <div class="orb orb-2"></div>
                <div class="orb orb-3"></div>
            </div>
        </div>

        <!-- Main container -->
        <div class="auth-container">
            <!-- Left panel - Enhanced Branding -->
            <div class="auth-brand">
                <div class="brand-content">
                    <!-- Logo Section -->
                    <div class="logo-section">
                        <div class="logo-container">
                            <img src="@/assets/images/favicon.svg" alt="QuantPulse" class="logo" />
                            <div class="logo-glow"></div>
                        </div>
                        <div class="brand-text">
                            <h1 class="brand-name">QuantPulse</h1>
                            <div class="brand-badge">
                                <i class="ph ph-brain text-xs"></i>
                                <span>AI-Powered</span>
                            </div>
                        </div>
                    </div>

                    <!-- Enhanced Tagline -->
                    <div class="brand-description">
                        <h2 class="tagline">Where Data Becomes Alpha</h2>
                        <p class="subtitle">
                            Sophisticated quantitative strategies meet cutting-edge machine learning.
                            Backtest with precision, execute with confidence, and outperform with intelligence.
                        </p>
                    </div>

                    <!-- Enhanced Features with better icons -->
                    <div class="features-list">
                        <div class="feature-item">
                            <div class="feature-icon">
                                <i class="ph ph-pulse"></i>
                            </div>
                            <div class="feature-content">
                                <span class="feature-title">Real-time signal generation</span>
                                <span class="feature-desc">ML-driven insights updated every second</span>
                            </div>
                        </div>
                        <div class="feature-item">
                            <div class="feature-icon">
                                <i class="ph ph-chart-line-up"></i>
                            </div>
                            <div class="feature-content">
                                <span class="feature-title">Historical strategy backtesting</span>
                                <span class="feature-desc">Test strategies against 20+ years of data</span>
                            </div>
                        </div>
                        <div class="feature-item">
                            <div class="feature-icon">
                                <i class="ph ph-shield-check"></i>
                            </div>
                            <div class="feature-content">
                                <span class="feature-title">Portfolio risk management</span>
                                <span class="feature-desc">Advanced risk metrics and protection</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Right panel - Enhanced Auth form -->
            <div class="auth-form-panel">
                <div class="form-container">
                    <div class="form-card">
                        <router-view></router-view>
                        <div class="form-footer">
                            <slot name="footer">
                                <div class="footer-content">
                                    <p class="footer-text">
                                        By continuing, you agree to our
                                        <a href="#" class="footer-link">Terms of Service</a>
                                        and
                                        <a href="#" class="footer-link">Privacy Policy</a>
                                    </p>
                                </div>
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
    background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 50%, #e2e8f0 100%);
}

.bg-mesh {
    @apply absolute inset-0 opacity-25;
    background-image:
        radial-gradient(circle at 1px 1px, rgba(148, 163, 184, 0.12) 1px, transparent 1px);
    background-size: 32px 32px;
    animation: mesh-move 25s linear infinite;
}

.bg-orbs {
    @apply absolute inset-0;
}

.orb {
    @apply absolute rounded-full blur-3xl;
}

.orb-1 {
    @apply w-96 h-96 bg-gradient-to-r from-blue-400 to-purple-400 opacity-30 -top-48 -left-48;
    animation: float 25s ease-in-out infinite;
}

.orb-2 {
    @apply w-80 h-80 bg-gradient-to-r from-purple-400 to-pink-400 opacity-25 -bottom-40 -right-40;
    animation: float 30s ease-in-out infinite reverse;
}

.orb-3 {
    @apply w-64 h-64 bg-gradient-to-r from-cyan-400 to-blue-400 opacity-20 top-1/3 right-1/4;
    animation: float 35s ease-in-out infinite;
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
    @apply w-16 h-16 rounded-2xl bg-white shadow-xl flex items-center justify-center flex-shrink-0 relative;
    box-shadow:
        0 10px 25px -5px rgba(0, 0, 0, 0.1),
        0 4px 6px -2px rgba(0, 0, 0, 0.05);
}

.logo {
    @apply w-10 h-10 relative z-10;
}

.logo-glow {
    @apply absolute inset-0 rounded-2xl bg-gradient-to-r from-blue-500 to-purple-500 opacity-20 blur-xl;
    animation: pulse-glow 3s ease-in-out infinite;
}

.brand-text {
    @apply flex flex-col;
}

.brand-name {
    @apply text-3xl lg:text-4xl font-bold text-gray-900 leading-tight;
    background: linear-gradient(135deg, #1e293b 0%, #475569 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.brand-badge {
    @apply inline-flex items-center space-x-1 px-2.5 py-0.5 rounded-full text-xs font-medium bg-gradient-to-r from-blue-100 to-purple-100 text-blue-800 w-fit mt-1;
    border: 1px solid rgba(59, 130, 246, 0.2);
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
    @apply space-y-4 max-w-sm mx-auto lg:mx-0;
}

.feature-item {
    @apply flex items-start space-x-4 p-4 rounded-xl bg-white/60 backdrop-blur-sm border border-gray-200/80;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.feature-item:hover {
    @apply bg-white/80 transform translate-x-2 border-gray-300/90;
    box-shadow: 0 8px 25px -5px rgba(0, 0, 0, 0.12);
}

.feature-icon {
    @apply w-11 h-11 rounded-xl bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center text-white flex-shrink-0;
    box-shadow: 0 4px 14px rgba(59, 130, 246, 0.25);
}

.feature-content {
    @apply flex flex-col space-y-1.5;
}

.feature-title {
    @apply text-sm font-semibold text-gray-900;
}

.feature-desc {
    @apply text-xs text-gray-600 leading-relaxed;
}

/* Form Panel */
.auth-form-panel {
    @apply flex justify-center lg:justify-end;
}

.form-container {
    @apply w-full max-w-md;
}

.form-card {
    @apply bg-white rounded-3xl border border-gray-300 overflow-hidden;
    box-shadow:
        0 8px 25px -5px rgba(0, 0, 0, 0.08),
        0 4px 6px -2px rgba(0, 0, 0, 0.04);
}

.form-footer {
    @apply px-8 pb-8 pt-6 border-t border-gray-200;
}

.footer-content {
    @apply text-center;
}

.footer-text {
    @apply text-xs text-gray-500 leading-relaxed;
}

.footer-link {
    @apply text-blue-600 hover:text-blue-700 font-medium transition-colors duration-200;
}

/* Statistics Section */
.stats-section {
    @apply flex items-center justify-center lg:justify-start space-x-8 pt-6 border-t border-gray-300;
}

.stat-item {
    @apply flex flex-col items-center lg:items-start space-y-1;
}

.stat-number {
    @apply text-lg font-bold text-gray-900;
}

.stat-label {
    @apply text-xs text-gray-600 font-medium;
}

/* Animations */
@keyframes float {

    0%,
    100% {
        transform: translate(0, 0) scale(1);
    }

    33% {
        transform: translate(20px, -20px) scale(1.05);
    }

    66% {
        transform: translate(-20px, 20px) scale(0.95);
    }
}

@keyframes mesh-move {
    0% {
        transform: translate(0, 0);
    }

    100% {
        transform: translate(32px, 32px);
    }
}

@keyframes pulse-glow {

    0%,
    100% {
        opacity: 0.2;
    }

    50% {
        opacity: 0.4;
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
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%);
    }

    .bg-mesh {
        background-image:
            radial-gradient(circle at 2px 2px, rgba(148, 163, 184, 0.08) 1px, transparent 1px);
    }

    .logo-container {
        @apply bg-slate-800 shadow-2xl;
        box-shadow:
            0 25px 50px -12px rgba(0, 0, 0, 0.4),
            0 10px 25px -5px rgba(0, 0, 0, 0.3);
    }

    .logo-glow {
        @apply opacity-30;
    }

    .brand-name {
        background: linear-gradient(135deg, #ffffff 0%, #e2e8f0 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    .brand-badge {
        @apply bg-gradient-to-r from-blue-900/50 to-purple-900/50 text-blue-200 border-blue-800/30;
    }

    .tagline {
        @apply text-slate-200;
    }

    .subtitle {
        @apply text-slate-400;
    }

    .feature-item {
        @apply bg-slate-800/30 border-slate-700/30;
    }

    .feature-item:hover {
        @apply bg-slate-800/50;
    }

    .feature-title {
        @apply text-slate-200;
    }

    .feature-desc {
        @apply text-slate-400;
    }

    .form-card {
        @apply bg-slate-800 border-slate-600;
        box-shadow:
            0 8px 25px -5px rgba(0, 0, 0, 0.25),
            0 4px 6px -2px rgba(0, 0, 0, 0.15);
    }

    .form-footer {
        @apply border-slate-600;
    }

    .footer-text {
        @apply text-slate-400;
    }

    .footer-link {
        @apply text-blue-400 hover:text-blue-300;
    }

    .feature-item {
        @apply bg-slate-800/30 border-slate-600/60;
    }

    .feature-item:hover {
        @apply bg-slate-800/50 border-slate-500/80;
    }

    .stats-section {
        @apply border-slate-600;
    }

    .stat-number {
        @apply text-white;
    }

    .stat-label {
        @apply text-slate-400;
    }
}
</style>