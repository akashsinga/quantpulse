<template>
    <div class="qp-min-h-screen qp-flex qp-items-center qp-justify-center qp-relative qp-overflow-hidden qp-bg-white qp-text-primary-900">
        <!-- Background Design -->
        <div class="qp-fixed qp-inset-0 qp-z-0 qp-pointer-events-none">
            <div class="qp-absolute qp-inset-0 qp-bg-white"></div>
            <div class="qp-absolute qp-inset-0 dot-pattern qp-opacity-40"></div>
            <div class="qp-absolute qp-top-10 qp-left-10 qp-w-64 qp-h-64 qp-bg-secondary-200 qp-rounded-full qp-opacity-30 qp-blur-3xl animate-float-slow"></div>
            <div class="qp-absolute qp-bottom-20 qp-right-20 qp-w-56 qp-h-56 qp-bg-primary-200 qp-rounded-full qp-opacity-35 qp-blur-3xl animate-float-slow-reverse"></div>
            <div class="qp-absolute qp-top-1/3 qp-right-1/4 qp-w-48 qp-h-48 qp-bg-secondary-100 qp-rounded-full qp-opacity-25 qp-blur-2xl animate-float-slower"></div>
        </div>

        <!-- Auth Layout Container -->
        <div class="qp-relative qp-z-10 qp-w-full qp-max-w-6xl qp-mx-auto qp-p-4">
            <div class="qp-grid qp-grid-cols-1 lg:qp-grid-cols-2 qp-bg-white/90 qp-backdrop-blur-lg qp-border qp-border-primary-200 qp-rounded-3xl qp-overflow-hidden">

                <!-- Left Panel (Brand) -->
                <div class="qp-hidden lg:qp-flex qp-flex-col qp-justify-center qp-px-12 qp-py-16 qp-space-y-8 qp-border-r qp-border-primary-200 qp-bg-gradient-to-br qp-from-white/90 qp-to-primary-50/90">
                    <!-- Logo & Brand -->
                    <div class="qp-flex qp-items-center qp-space-x-4">
                        <div class="qp-w-12 qp-h-12 qp-bg-primary-900 qp-rounded-md qp-flex qp-items-center qp-justify-center">
                            <img src="@/assets/images/favicon.svg" alt="QuantPulse" class="qp-w-7 qp-h-7">
                        </div>
                        <div class="qp-flex qp-flex-col">
                            <span class="qp-text-2xl qp-font-bold qp-text-primary-900">QuantPulse</span>
                            <div class="qp-inline-flex qp-items-center qp-px-2 qp-py-0.5 qp-rounded-full qp-bg-primary-100 qp-text-primary-700 qp-text-xs qp-font-medium qp-w-fit">
                                <div class="qp-w-1.5 qp-h-1.5 qp-bg-secondary-500 qp-rounded-full qp-mr-1.5"></div>
                                {{ landingPageI18n.heroSection.aiPoweredTradingPlatform }}
                            </div>
                        </div>
                    </div>

                    <!-- Tagline -->
                    <h2 class="qp-text-3xl qp-font-bold qp-text-primary-900 qp-leading-tight qp-max-w-sm">
                        {{ landingPageI18n.heroSection.whereDataBecomes }} <span class="qp-text-secondary-600">{{ landingPageI18n.heroSection.alpha }}</span>
                    </h2>

                    <!-- Description -->
                    <p class="qp-text-sm qp-text-primary-600 qp-leading-relaxed qp-max-w-md">{{ landingPageI18n.heroSection.subtitle }}</p>

                    <!-- Key Features -->
                    <div class="qp-space-y-4 qp-max-w-md">
                        <div class="qp-flex qp-items-center qp-space-x-3 qp-text-primary-600">
                            <div class="qp-w-8 qp-h-8 qp-rounded-lg qp-bg-primary-900 qp-flex qp-items-center qp-justify-center">
                                <i class="ph ph-pulse qp-text-white qp-text-sm"></i>
                            </div>
                            <span class="qp-text-sm qp-font-medium">{{ landingPageI18n.footer.strategyBuilder }}</span>
                        </div>

                        <div class="qp-flex qp-items-center qp-space-x-3 qp-text-primary-600">
                            <div class="qp-w-8 qp-h-8 qp-rounded-lg qp-bg-primary-900 qp-flex qp-items-center qp-justify-center">
                                <i class="ph ph-chart-line-up qp-text-white qp-text-sm"></i>
                            </div>
                            <span class="qp-text-sm qp-font-medium">{{ landingPageI18n.footer.backtestingEngine }}</span>
                        </div>

                        <div class="qp-flex qp-items-center qp-space-x-3 qp-text-primary-600">
                            <div class="qp-w-8 qp-h-8 qp-rounded-lg qp-bg-primary-900 qp-flex qp-items-center qp-justify-center">
                                <i class="ph ph-shield-check qp-text-white qp-text-sm"></i>
                            </div>
                            <span class="qp-text-sm qp-font-medium">{{ landingPageI18n.stats.services.securePlatform }}</span>
                        </div>
                    </div>

                    <!-- Trust Stats -->
                    <div class="qp-bg-primary-50/80 qp-border qp-border-primary-200 qp-rounded-md qp-p-4">
                        <div class="qp-grid qp-grid-cols-3 qp-gap-4 qp-text-center">
                            <div>
                                <div class="qp-text-lg qp-font-bold qp-text-primary-900">25+</div>
                                <div class="qp-text-xs qp-text-primary-600">{{ landingPageI18n.stats.services.yearsOfData }}</div>
                            </div>
                            <div>
                                <div class="qp-text-lg qp-font-bold qp-text-primary-900">24/7</div>
                                <div class="qp-text-xs qp-text-primary-600">{{ landingPageI18n.stats.services.marketMonitoring }}</div>
                            </div>
                            <div>
                                <div class="qp-text-lg qp-font-bold qp-text-primary-900">100%</div>
                                <div class="qp-text-xs qp-text-primary-600">{{ landingPageI18n.stats.services.securePlatform }}</div>
                            </div>
                        </div>
                    </div>

                    <!-- Copyright -->
                    <p class="qp-text-xs qp-text-primary-400 qp-pt-4">
                        &copy; 2025 QuantPulse. {{ landingPageI18n.footer.allRightsReserved }}
                    </p>
                </div>

                <!-- Right Panel (Auth Form) -->
                <div class="qp-flex qp-items-center qp-justify-center qp-px-6 qp-py-12 sm:qp-px-10 lg:qp-py-16">
                    <div class="qp-w-full qp-max-w-md">
                        <!-- Router View for Auth Pages -->
                        <router-view />

                        <!-- Footer -->
                        <div class="qp-mt-6 qp-text-center">
                            <slot name="footer">
                                <p class="qp-text-xs qp-text-primary-500 qp-leading-relaxed">
                                    By continuing, you agree to our
                                    <a href="#" class="qp-text-secondary-600 hover:qp-text-secondary-700 qp-font-medium qp-transition-colors qp-duration-200 focus:qp-outline-none focus:qp-ring-2 focus:qp-ring-secondary-500 focus:qp-ring-offset-2 qp-rounded">Terms of Service</a>
                                    and
                                    <a href="#" class="qp-text-secondary-600 hover:qp-text-secondary-700 qp-font-medium qp-transition-colors qp-duration-200 focus:qp-outline-none focus:qp-ring-2 focus:qp-ring-secondary-500 focus:qp-ring-offset-2 qp-rounded">Privacy Policy</a>
                                </p>
                            </slot>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>
<script>
export default {
    data() {
        return {
            landingPageI18n: this.$tm('pages.landingPage')
        }
    }
}
</script>
<style scoped>
/* CSS Dot Pattern */
.dot-pattern {
    background-image: radial-gradient(circle at 2px 2px, rgba(71, 85, 105, 0.08) 2px, transparent 2px);
    background-size: 30px 30px;
    animation: dot-breathe 6s ease-in-out infinite;
}

/* Background Animations */
@keyframes dot-breathe {

    0%,
    100% {
        opacity: 0.6;
    }

    50% {
        opacity: 1;
    }
}

@keyframes float-slow {

    0%,
    100% {
        transform: translateY(0px) translateX(0px);
    }

    50% {
        transform: translateY(-20px) translateX(10px);
    }
}

@keyframes float-slow-reverse {

    0%,
    100% {
        transform: translateY(0px) translateX(0px);
    }

    50% {
        transform: translateY(15px) translateX(-15px);
    }
}

@keyframes float-slower {

    0%,
    100% {
        transform: translateY(0px) translateX(0px);
    }

    50% {
        transform: translateY(-10px) translateX(-5px);
    }
}

/* Animation Classes */
.animate-float-slow {
    animation: float-slow 15s ease-in-out infinite;
}

.animate-float-slow-reverse {
    animation: float-slow-reverse 18s ease-in-out infinite;
}

.animate-float-slower {
    animation: float-slower 20s ease-in-out infinite;
    animation-delay: -5s;
}

/* Responsive Design */
@media (max-width: 1024px) {
    .qp-hidden.lg\\:qp-flex {
        display: none;
    }
}

@media (max-width: 640px) {
    .qp-p-4 {
        padding: 0.75rem;
    }

    .qp-px-6.qp-py-12 {
        padding: 2rem 1.5rem;
    }
}
</style>