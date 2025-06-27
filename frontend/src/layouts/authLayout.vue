<template>
    <div class="min-h-screen flex items-center justify-center relative overflow-hidden bg-white text-slate-900">
        <!-- Background Design -->
        <div class="fixed inset-0 z-0 pointer-events-none">
            <div class="absolute inset-0 bg-white"></div>
            <div class="absolute inset-0 dot-pattern opacity-40"></div>
            <div class="absolute top-10 left-10 w-64 h-64 bg-blue-200 rounded-full opacity-30 blur-3xl animate-float-slow"></div>
            <div class="absolute bottom-20 right-20 w-56 h-56 bg-slate-200 rounded-full opacity-35 blur-3xl animate-float-slow-reverse"></div>
            <div class="absolute top-1/3 right-1/4 w-48 h-48 bg-blue-100 rounded-full opacity-25 blur-2xl animate-float-slower"></div>
        </div>

        <!-- Auth Layout Container -->
        <div class="relative z-10 w-full max-w-6xl mx-auto p-4">
            <div class="grid grid-cols-1 lg:grid-cols-2 bg-white/90 backdrop-blur-lg border border-slate-200 rounded-3xl overflow-hidden">

                <!-- Left Panel (Brand) -->
                <div class="hidden lg:flex flex-col justify-center px-12 py-16 space-y-8 border-r border-slate-200 bg-gradient-to-br from-white/90 to-slate-50/90">
                    <!-- Logo & Brand -->
                    <div class="flex items-center space-x-4">
                        <div class="w-12 h-12 bg-slate-900 rounded-xl flex items-center justify-center">
                            <img src="@/assets/images/favicon.svg" alt="QuantPulse" class="w-7 h-7">
                        </div>
                        <div class="flex flex-col">
                            <span class="text-2xl font-bold text-slate-900">QuantPulse</span>
                            <div class="inline-flex items-center px-2 py-0.5 rounded-full bg-slate-100 text-slate-700 text-xs font-medium w-fit">
                                <div class="w-1.5 h-1.5 bg-blue-500 rounded-full mr-1.5"></div>
                                AI-Powered
                            </div>
                        </div>
                    </div>

                    <!-- Tagline -->
                    <h2 class="text-3xl font-bold text-slate-900 leading-tight max-w-sm">
                        Where Data Becomes <span class="text-blue-600">Alpha</span>
                    </h2>

                    <!-- Description -->
                    <p class="text-sm text-slate-600 leading-relaxed max-w-md">
                        Advanced quantitative research and backtesting platform powered by machine learning.
                        Develop strategies, analyze performance, and enhance your trading research workflow.
                    </p>

                    <!-- Key Features -->
                    <div class="space-y-4 max-w-md">
                        <div class="flex items-center space-x-3 text-slate-600">
                            <div class="w-8 h-8 rounded-lg bg-slate-900 flex items-center justify-center">
                                <i class="ph ph-pulse text-white text-sm"></i>
                            </div>
                            <span class="text-sm font-medium">Strategy research & development</span>
                        </div>

                        <div class="flex items-center space-x-3 text-slate-600">
                            <div class="w-8 h-8 rounded-lg bg-slate-900 flex items-center justify-center">
                                <i class="ph ph-chart-line-up text-white text-sm"></i>
                            </div>
                            <span class="text-sm font-medium">Comprehensive backtesting engine</span>
                        </div>

                        <div class="flex items-center space-x-3 text-slate-600">
                            <div class="w-8 h-8 rounded-lg bg-slate-900 flex items-center justify-center">
                                <i class="ph ph-shield-check text-white text-sm"></i>
                            </div>
                            <span class="text-sm font-medium">Performance analysis & enhancement</span>
                        </div>
                    </div>

                    <!-- Trust Stats -->
                    <div class="bg-slate-50/80 border border-slate-200 rounded-xl p-4">
                        <div class="grid grid-cols-3 gap-4 text-center">
                            <div>
                                <div class="text-lg font-bold text-slate-900">25+</div>
                                <div class="text-xs text-slate-600">Years of Data</div>
                            </div>
                            <div>
                                <div class="text-lg font-bold text-slate-900">24/7</div>
                                <div class="text-xs text-slate-600">Monitoring</div>
                            </div>
                            <div>
                                <div class="text-lg font-bold text-slate-900">100%</div>
                                <div class="text-xs text-slate-600">Secure</div>
                            </div>
                        </div>
                    </div>

                    <!-- Copyright -->
                    <p class="text-xs text-slate-400 pt-4">
                        &copy; 2025 QuantPulse. All rights reserved.
                    </p>
                </div>

                <!-- Right Panel (Auth Form) -->
                <div class="flex items-center justify-center px-6 py-12 sm:px-10 lg:py-16">
                    <div class="w-full max-w-md">
                        <!-- Router View for Auth Pages -->
                        <router-view />

                        <!-- Footer -->
                        <div class="mt-6 text-center">
                            <slot name="footer">
                                <p class="text-xs text-slate-500 leading-relaxed">
                                    By continuing, you agree to our
                                    <a href="#" class="text-blue-600 hover:text-blue-700 font-medium transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 rounded">Terms of Service</a>
                                    and
                                    <a href="#" class="text-blue-600 hover:text-blue-700 font-medium transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 rounded">Privacy Policy</a>
                                </p>
                            </slot>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

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
    .hidden.lg\\:flex {
        display: none;
    }
}

@media (max-width: 640px) {
    .p-4 {
        padding: 0.75rem;
    }

    .px-6.py-12 {
        padding: 2rem 1.5rem;
    }
}
</style>