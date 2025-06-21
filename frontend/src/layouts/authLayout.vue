<template>
    <div class="auth-page">
        <!-- Background with subtle grid pattern -->
        <div class="background-pattern"></div>

        <!-- Subtle floating elements -->
        <div class="floating-shapes">
            <div class="shape shape-1"></div>
            <div class="shape shape-2"></div>
            <div class="shape shape-3"></div>
        </div>

        <!-- Main content container -->
        <div class="auth-container">
            <!-- Left side - Branding -->
            <div class="brand-section">
                <div class="brand-content">
                    <div class="logo-wrapper">
                        <img src="@/assets/images/favicon.svg" class="logo" alt="QuantPulse" />
                    </div>
                    <h1 class="brand-title">QuantPulse</h1>
                    <p class="brand-tagline">Smart trading decisions, powered by data</p>

                    <!-- Value propositions based on QuantPulse features -->
                    <div class="features">
                        <div class="feature">
                            <div class="feature-icon">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                                    stroke-width="2">
                                    <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
                                </svg>
                            </div>
                            <span>Intelligent buy & sell recommendations</span>
                        </div>
                        <div class="feature">
                            <div class="feature-icon">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                                    stroke-width="2">
                                    <path d="M3 3h18v18H3zM9 9h6v6H9z" />
                                </svg>
                            </div>
                            <span>Test strategies before you invest</span>
                        </div>
                        <div class="feature">
                            <div class="feature-icon">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                                    stroke-width="2">
                                    <path
                                        d="M16 4v12l-4-2-4 2V4M6 20h12a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2z" />
                                </svg>
                            </div>
                            <span>Monitor performance & manage risk</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Right side - Auth form -->
            <div class="auth-section">
                <div class="auth-card">
                    <!-- Slot for dynamic auth content (login, register, forgot-password) -->
                    <slot />
                </div>
            </div>
        </div>
    </div>
</template>

<style lang="postcss" scoped>
.auth-page {
    @apply min-h-screen relative flex items-center justify-center;
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%);
}

.background-pattern {
    @apply absolute inset-0;
    background-image:
        radial-gradient(circle at 2px 2px, rgba(148, 163, 184, 0.15) 1px, transparent 1px);
    background-size: 32px 32px;
}

.floating-shapes {
    @apply absolute inset-0 overflow-hidden pointer-events-none;
}

.shape {
    @apply absolute rounded-full;
    background: radial-gradient(circle, rgba(59, 130, 246, 0.1), transparent 70%);
}

.shape-1 {
    @apply w-96 h-96 -top-48 -left-48;
    animation: float 20s linear infinite;
}

.shape-2 {
    @apply w-80 h-80 -bottom-40 -right-40;
    animation: float 25s linear infinite reverse;
}

.shape-3 {
    @apply w-64 h-64 top-1/2 left-1/4;
    animation: float 30s linear infinite;
}

.auth-container {
    @apply w-full max-w-6xl mx-auto p-8 grid lg:grid-cols-2 gap-16 items-center;
}

/* Brand Section */
.brand-section {
    @apply space-y-8;
}

.brand-content {
    @apply space-y-6;
}

.logo-wrapper {
    @apply flex items-center justify-start;
}

.logo-container {
    @apply relative p-3 rounded-2xl;
    background: rgba(59, 130, 246, 0.1);
    backdrop-filter: blur(8px);
    border: 1px solid rgba(59, 130, 246, 0.2);
}

.logo {
    @apply w-12 h-12;
    filter: drop-shadow(0 4px 12px rgba(59, 130, 246, 0.4));
}

.brand-title {
    @apply text-5xl font-bold text-white;
    background: linear-gradient(135deg, #ffffff 0%, #e2e8f0 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    line-height: 1.1;
}

.brand-tagline {
    @apply text-xl text-slate-300 leading-relaxed max-w-md;
}

.features {
    @apply space-y-4 pt-4;
}

.feature {
    @apply flex items-center space-x-3 text-slate-300;
}

.feature-icon {
    @apply w-10 h-10 rounded-lg bg-slate-800/50 flex items-center justify-center text-blue-400;
    backdrop-filter: blur(8px);
}

/* Auth Section */
.auth-section {
    @apply space-y-6;
}

.auth-card {
    @apply bg-white/5 backdrop-blur-xl rounded-2xl p-8 border border-white/10;
    box-shadow:
        0 20px 25px -5px rgba(0, 0, 0, 0.3),
        0 10px 10px -5px rgba(0, 0, 0, 0.2),
        inset 0 1px 0 rgba(255, 255, 255, 0.1);
}

/* Animations */
@keyframes float {

    0%,
    100% {
        transform: translate(0, 0) rotate(0deg);
    }

    33% {
        transform: translate(30px, -30px) rotate(120deg);
    }

    66% {
        transform: translate(-20px, 20px) rotate(240deg);
    }
}

/* Responsive Design */
@media (max-width: 1024px) {
    .auth-container {
        @apply grid-cols-1 gap-8 max-w-md;
    }

    .brand-section {
        @apply text-center;
    }

    .brand-title {
        @apply text-4xl;
    }

    .logo-wrapper {
        @apply justify-center;
    }
}

@media (max-width: 640px) {
    .auth-container {
        @apply p-4;
    }

    .auth-card {
        @apply p-6;
    }

    .brand-title {
        @apply text-3xl;
    }

    .brand-tagline {
        @apply text-lg;
    }
}
</style>