<template>
    <div class="min-h-screen bg-white text-slate-900 relative">
        <!-- Enhanced background effects -->
        <div class="fixed inset-0 pointer-events-none z-0">
            <!-- Animated gradient mesh -->
            <div class="absolute inset-0 bg-gradient-to-br from-slate-50/50 via-blue-50/30 to-slate-50/50"></div>

            <!-- CSS dot pattern -->
            <div class="absolute inset-0 opacity-80 dot-pattern"></div>

            <!-- Floating orbs -->
            <div
                class="absolute top-20 left-10 w-64 h-64 bg-blue-100 rounded-full opacity-20 blur-3xl animate-float-slow">
            </div>
            <div
                class="absolute bottom-32 right-16 w-80 h-80 bg-slate-100 rounded-full opacity-25 blur-3xl animate-float-slow-reverse">
            </div>
            <div
                class="absolute top-1/2 left-1/3 w-48 h-48 bg-blue-50 rounded-full opacity-15 blur-2xl animate-float-slower">
            </div>
        </div>

        <div class="relative z-10">
            <!-- Simplified Header -->
            <header class="fixed w-full top-0 z-50 bg-white/95 backdrop-blur-sm border-b border-slate-200">
                <nav class="max-w-7xl mx-auto px-6 py-4">
                    <div class="flex items-center justify-between">
                        <div class="flex items-center space-x-3">
                            <div class="w-10 h-10 bg-slate-900 rounded-xl flex items-center justify-center">
                                <img src="@/assets/images/favicon.svg">
                            </div>
                            <span class="text-2xl font-bold text-slate-900">QuantPulse</span>
                        </div>
                        <div class="hidden md:flex items-center space-x-8">
                            <a href="#features"
                                class="text-slate-600 hover:text-slate-900 transition-colors font-medium">Features</a>
                            <a href="#how-it-works"
                                class="text-slate-600 hover:text-slate-900 transition-colors font-medium">How It
                                Works</a>
                            <a href="#contact"
                                class="text-slate-600 hover:text-slate-900 transition-colors font-medium">Contact</a>
                            <button
                                class="bg-slate-900 text-white px-6 py-2.5 rounded-lg hover:bg-slate-800 transition-colors font-medium">
                                Get Started
                            </button>
                        </div>
                    </div>
                </nav>
            </header>

            <!-- Enhanced Hero -->
            <section class="pt-32 pb-20 relative overflow-hidden">
                <!-- Section-specific background effects -->
                <div class="absolute inset-0 bg-gradient-to-b from-transparent via-blue-50/20 to-transparent"></div>
                <div
                    class="absolute top-10 right-10 w-32 h-32 bg-blue-200 rounded-full opacity-20 blur-2xl animate-pulse-slow">
                </div>

                <div class="max-w-7xl mx-auto px-6 text-center relative z-10">
                    <div class="max-w-4xl mx-auto">
                        <div
                            class="inline-flex items-center px-4 py-2 rounded-full bg-slate-100 text-slate-700 text-sm font-medium mb-8">
                            <div class="w-2 h-2 bg-blue-500 rounded-full mr-2"></div>
                            AI-Powered Trading Platform
                        </div>

                        <h1 class="text-5xl md:text-6xl font-bold text-slate-900 mb-8 leading-tight">
                            Where Data Becomes
                            <span class="text-blue-600">Alpha</span>
                        </h1>

                        <p class="text-xl text-slate-600 mb-12 max-w-3xl mx-auto leading-relaxed">
                            Sophisticated quantitative strategies meet cutting-edge machine learning. Backtest with
                            precision, execute with confidence, and outperform with intelligence.
                        </p>

                        <div class="flex flex-col sm:flex-row gap-4 justify-center">
                            <button
                                class="bg-slate-900 text-white px-8 py-4 rounded-lg text-lg font-semibold hover:bg-slate-800 transition-colors">
                                Start Building Strategies
                            </button>
                            <button
                                class="border-2 border-slate-300 text-slate-700 px-8 py-4 rounded-lg text-lg font-semibold hover:bg-slate-50 transition-colors">
                                Documentation
                            </button>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced Features -->
            <section id="features" class="py-20 bg-slate-50 relative overflow-hidden">
                <!-- Section background effects -->
                <div class="absolute inset-0 bg-gradient-to-r from-slate-50 via-blue-50/50 to-slate-50"></div>
                <div class="absolute bottom-0 left-0 w-96 h-96 bg-slate-200 rounded-full opacity-10 blur-3xl"></div>

                <div class="max-w-7xl mx-auto px-6 relative z-10">
                    <div class="text-center mb-16">
                        <h2 class="text-4xl font-bold text-slate-900 mb-4">
                            Platform Capabilities
                        </h2>
                        <p class="text-xl text-slate-600 max-w-3xl mx-auto">
                            Comprehensive tools for professional traders and quantitative analysts
                        </p>
                    </div>

                    <div class="grid md:grid-cols-2 gap-8 max-w-5xl mx-auto">
                        <div class="bg-white p-8 rounded-xl border border-slate-200 hover:shadow-lg transition-shadow">
                            <div class="w-12 h-12 bg-slate-900 rounded-lg flex items-center justify-center mb-6">
                                <i class="ph ph-pulse text-white text-xl"></i>
                            </div>
                            <h3 class="text-xl font-bold text-slate-900 mb-4">Real-time Signal Generation</h3>
                            <p class="text-slate-600 leading-relaxed">
                                ML-driven insights updated every second with sophisticated quantitative strategies.
                            </p>
                        </div>

                        <div class="bg-white p-8 rounded-xl border border-slate-200 hover:shadow-lg transition-shadow">
                            <div class="w-12 h-12 bg-slate-900 rounded-lg flex items-center justify-center mb-6">
                                <i class="ph ph-chart-line-up text-white text-xl"></i>
                            </div>
                            <h3 class="text-xl font-bold text-slate-900 mb-4">Historical Strategy Backtesting</h3>
                            <p class="text-slate-600 leading-relaxed">
                                Test strategies against 20+ years of data with comprehensive performance metrics.
                            </p>
                        </div>

                        <div class="bg-white p-8 rounded-xl border border-slate-200 hover:shadow-lg transition-shadow">
                            <div class="w-12 h-12 bg-slate-900 rounded-lg flex items-center justify-center mb-6">
                                <i class="ph ph-shield-warning text-white text-xl"></i>
                            </div>
                            <h3 class="text-xl font-bold text-slate-900 mb-4">Portfolio Risk Management</h3>
                            <p class="text-slate-600 leading-relaxed">
                                Advanced risk metrics and protection with real-time monitoring and safeguards.
                            </p>
                        </div>

                        <div class="bg-white p-8 rounded-xl border border-slate-200 hover:shadow-lg transition-shadow">
                            <div class="w-12 h-12 bg-slate-900 rounded-lg flex items-center justify-center mb-6">
                                <i class="ph ph-brain text-white text-xl"></i>
                            </div>
                            <h3 class="text-xl font-bold text-slate-900 mb-4">Machine Learning Enhancement</h3>
                            <p class="text-slate-600 leading-relaxed">
                                Continuously improve performance using adaptive algorithms that learn from market
                                conditions.
                            </p>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced Platform Overview -->
            <section class="py-20 relative overflow-hidden">
                <!-- Section background effects -->
                <div class="absolute top-0 right-0 w-72 h-72 bg-blue-100 rounded-full opacity-15 blur-3xl"></div>
                <div class="absolute bottom-10 left-10 w-64 h-64 bg-slate-100 rounded-full opacity-20 blur-2xl"></div>

                <div class="max-w-7xl mx-auto px-6 relative z-10">
                    <div class="grid lg:grid-cols-2 gap-16 items-center">
                        <div>
                            <div
                                class="inline-flex items-center px-3 py-1 rounded-full bg-blue-100 text-blue-700 text-sm font-medium mb-6">
                                Smart Solutions
                            </div>

                            <h2 class="text-4xl font-bold text-slate-900 mb-6">
                                Smart Trading
                                <span class="text-blue-600">Solutions</span>
                            </h2>

                            <p class="text-xl text-slate-600 mb-8 leading-relaxed">
                                Everything you need for sophisticated quantitative trading in one integrated platform.
                            </p>

                            <div class="space-y-6">
                                <div class="flex items-start space-x-4">
                                    <div class="w-2 h-2 bg-blue-500 rounded-full mt-3 flex-shrink-0"></div>
                                    <div>
                                        <h4 class="text-slate-900 font-semibold text-lg mb-2">Strategy Definition
                                            Framework</h4>
                                        <p class="text-slate-600">Component library with indicators, entry/exit rules,
                                            and filters</p>
                                    </div>
                                </div>

                                <div class="flex items-start space-x-4">
                                    <div class="w-2 h-2 bg-blue-500 rounded-full mt-3 flex-shrink-0"></div>
                                    <div>
                                        <h4 class="text-slate-900 font-semibold text-lg mb-2">Signal Enhancement</h4>
                                        <p class="text-slate-600">Signal probability calculation and confidence scoring
                                        </p>
                                    </div>
                                </div>

                                <div class="flex items-start space-x-4">
                                    <div class="w-2 h-2 bg-blue-500 rounded-full mt-3 flex-shrink-0"></div>
                                    <div>
                                        <h4 class="text-slate-900 font-semibold text-lg mb-2">Performance Analytics</h4>
                                        <p class="text-slate-600">Comprehensive performance tracking and risk
                                            attribution analysis</p>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div class="bg-slate-50 p-12 rounded-2xl border border-slate-200">
                            <div class="text-center">
                                <div
                                    class="w-16 h-16 bg-slate-900 rounded-2xl flex items-center justify-center mx-auto mb-6">
                                    <i class="ph ph-chart-line text-white text-2xl"></i>
                                </div>
                                <h4 class="text-slate-900 font-bold text-xl mb-4">Professional Grade Platform</h4>
                                <p class="text-slate-600 leading-relaxed">
                                    Institutional-grade analytics for sophisticated trading strategies
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced How It Works -->
            <section id="how-it-works" class="py-20 bg-slate-50 relative overflow-hidden">
                <!-- Section background effects -->
                <div class="absolute inset-0 bg-gradient-to-b from-slate-50 via-white/80 to-slate-50"></div>
                <div
                    class="absolute top-20 left-1/4 w-48 h-48 bg-blue-100 rounded-full opacity-15 blur-3xl animate-pulse-slower">
                </div>
                <div class="absolute bottom-20 right-1/4 w-56 h-56 bg-slate-200 rounded-full opacity-20 blur-2xl"></div>

                <div class="max-w-7xl mx-auto px-6 relative z-10">
                    <div class="text-center mb-16">
                        <h2 class="text-4xl font-bold text-slate-900 mb-4">
                            How It Works
                        </h2>
                        <p class="text-xl text-slate-600 max-w-3xl mx-auto">
                            Get started with quantitative trading in four simple steps
                        </p>
                    </div>

                    <div class="max-w-4xl mx-auto">
                        <div class="space-y-12">
                            <div class="flex flex-col md:flex-row items-center gap-8">
                                <div
                                    class="w-16 h-16 bg-slate-900 rounded-full flex items-center justify-center flex-shrink-0">
                                    <span class="text-white font-bold text-xl">1</span>
                                </div>
                                <div class="text-center md:text-left">
                                    <h3 class="text-2xl font-bold text-slate-900 mb-4">Define Your Strategy</h3>
                                    <p class="text-slate-600 text-lg leading-relaxed">
                                        Create and customize trading strategies using our intuitive strategy builder
                                        with flexible parameters and rules
                                    </p>
                                </div>
                            </div>

                            <div class="flex flex-col md:flex-row items-center gap-8">
                                <div
                                    class="w-16 h-16 bg-slate-900 rounded-full flex items-center justify-center flex-shrink-0">
                                    <span class="text-white font-bold text-xl">2</span>
                                </div>
                                <div class="text-center md:text-left">
                                    <h3 class="text-2xl font-bold text-slate-900 mb-4">Test & Optimize</h3>
                                    <p class="text-slate-600 text-lg leading-relaxed">
                                        Run comprehensive backtests against historical data and optimize your strategy
                                        using machine learning
                                    </p>
                                </div>
                            </div>

                            <div class="flex flex-col md:flex-row items-center gap-8">
                                <div
                                    class="w-16 h-16 bg-slate-900 rounded-full flex items-center justify-center flex-shrink-0">
                                    <span class="text-white font-bold text-xl">3</span>
                                </div>
                                <div class="text-center md:text-left">
                                    <h3 class="text-2xl font-bold text-slate-900 mb-4">Get Signals</h3>
                                    <p class="text-slate-600 text-lg leading-relaxed">
                                        Receive daily trading signals with confidence scores and detailed analysis for
                                        both long and short positions
                                    </p>
                                </div>
                            </div>

                            <div class="flex flex-col md:flex-row items-center gap-8">
                                <div
                                    class="w-16 h-16 bg-slate-900 rounded-full flex items-center justify-center flex-shrink-0">
                                    <span class="text-white font-bold text-xl">4</span>
                                </div>
                                <div class="text-center md:text-left">
                                    <h3 class="text-2xl font-bold text-slate-900 mb-4">Track Performance</h3>
                                    <p class="text-slate-600 text-lg leading-relaxed">
                                        Monitor your portfolio performance with detailed analytics and continuously
                                        improve your trading approach
                                    </p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced Stats -->
            <section class="py-20 relative overflow-hidden">
                <!-- Section background effects -->
                <div class="absolute inset-0 bg-gradient-to-br from-transparent via-slate-50/30 to-transparent"></div>
                <div class="absolute top-10 left-10 w-40 h-40 bg-blue-200 rounded-full opacity-10 blur-2xl"></div>
                <div class="absolute bottom-10 right-10 w-52 h-52 bg-slate-200 rounded-full opacity-15 blur-3xl"></div>

                <div class="max-w-7xl mx-auto px-6 relative z-10">
                    <div class="text-center mb-12">
                        <h2 class="text-3xl font-bold text-slate-900 mb-4">
                            Trusted by Professionals
                        </h2>
                        <p class="text-lg text-slate-600">
                            Numbers that speak to our platform's reliability and performance
                        </p>
                    </div>

                    <div class="grid grid-cols-2 md:grid-cols-4 gap-8 text-center max-w-4xl mx-auto">
                        <div class="p-6">
                            <div class="text-4xl font-bold text-slate-900 mb-2">25+</div>
                            <div class="text-slate-600 font-medium">Years of Data</div>
                        </div>

                        <div class="p-6">
                            <div class="text-4xl font-bold text-slate-900 mb-2">7</div>
                            <div class="text-slate-600 font-medium">Core Services</div>
                        </div>

                        <div class="p-6">
                            <div class="text-4xl font-bold text-slate-900 mb-2">24/7</div>
                            <div class="text-slate-600 font-medium">Market Monitoring</div>
                        </div>

                        <div class="p-6">
                            <div class="text-4xl font-bold text-slate-900 mb-2">100%</div>
                            <div class="text-slate-600 font-medium">Secure Platform</div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced CTA -->
            <section class="py-20 bg-slate-50 relative overflow-hidden">
                <!-- Section background effects -->
                <div class="absolute inset-0 bg-gradient-to-r from-slate-50 via-blue-50/40 to-slate-50"></div>
                <div class="absolute top-0 left-1/3 w-60 h-60 bg-blue-100 rounded-full opacity-20 blur-3xl"></div>
                <div class="absolute bottom-0 right-1/4 w-44 h-44 bg-slate-200 rounded-full opacity-25 blur-2xl"></div>

                <div class="max-w-7xl mx-auto px-6 text-center relative z-10">
                    <div class="max-w-3xl mx-auto">
                        <h2 class="text-4xl font-bold text-slate-900 mb-6">
                            Ready to Elevate Your Trading?
                        </h2>
                        <p class="text-xl text-slate-600 mb-8">
                            Join traders who use data-driven strategies to make smarter investment decisions
                        </p>
                        <button
                            class="bg-slate-900 text-white px-12 py-4 rounded-lg text-lg font-semibold hover:bg-slate-800 transition-colors">
                            Start Your Journey
                        </button>
                    </div>
                </div>
            </section>

            <!-- Simplified Footer -->
            <footer id="contact" class="bg-slate-900 text-white py-16">
                <div class="max-w-7xl mx-auto px-6">
                    <div class="grid md:grid-cols-3 gap-12 mb-12">
                        <div>
                            <div class="flex items-center space-x-3 mb-6">
                                <div class="w-10 h-10 bg-white rounded-xl flex items-center justify-center">
                                    <svg viewBox="0 0 100 100" class="w-6 h-6 text-slate-900">
                                        <path
                                            d="M15 50 L25 50 L30 35 L35 65 L40 25 L45 75 L50 15 L55 85 L60 30 L65 70 L70 40 L75 50 L85 50"
                                            fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round"
                                            stroke-linejoin="round" />
                                    </svg>
                                </div>
                                <span class="text-2xl font-bold">QuantPulse</span>
                            </div>
                            <p class="text-slate-300 leading-relaxed">
                                Smart trading solutions for modern investors
                            </p>
                        </div>

                        <div>
                            <h4 class="text-white font-semibold mb-6">Platform</h4>
                            <ul class="space-y-3 text-slate-300">
                                <li>Strategy Builder</li>
                                <li>Backtesting Engine</li>
                                <li>Signal Generation</li>
                                <li>Portfolio Tracking</li>
                            </ul>
                        </div>

                        <div>
                            <h4 class="text-white font-semibold mb-6">Get Started</h4>
                            <p class="text-slate-300 mb-6">
                                Ready to transform your trading approach?
                            </p>
                            <button
                                class="border border-white text-white px-6 py-3 rounded-lg hover:bg-white hover:text-slate-900 transition-colors">
                                Contact Us
                            </button>
                        </div>
                    </div>

                    <div class="border-t border-slate-700 pt-8 text-center text-slate-400">
                        <p>&copy; 2025 QuantPulse. All rights reserved.</p>
                    </div>
                </div>
            </footer>
        </div>
    </div>
</template>

<script>
export default {
    name: 'QuantPulseLanding',
    mounted() {
        // Smooth scrolling for navigation links
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {
            anchor.addEventListener('click', function (e) {
                e.preventDefault();
                document.querySelector(this.getAttribute('href')).scrollIntoView({
                    behavior: 'smooth'
                });
            });
        });
    }
}
</script>

<style lang="postcss" scoped>
/* Custom container max-width */
.max-w-7xl {
    max-width: 80rem;
}

/* Focus states for accessibility */
a {
    @apply px-3;
    @apply py-1;

    &:focus {
        @apply ring-1;
        @apply ring-blue-100;
        @apply rounded-md;
    }
}

/* CSS Dot Pattern */
.dot-pattern {
    background-image: radial-gradient(circle at 2px 2px, rgba(71, 85, 105, 0.1) 2px, transparent 2px);
    background-size: 30px 30px;
    animation: dot-breathe 6s ease-in-out infinite;
}

@keyframes dot-breathe {

    0%,
    100% {
        opacity: 0.6;
    }

    50% {
        opacity: 1;
    }
}

/* Subtle background animations */
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

@keyframes pulse-slow {

    0%,
    100% {
        opacity: 0.2;
    }

    50% {
        opacity: 0.4;
    }
}

@keyframes pulse-slower {

    0%,
    100% {
        opacity: 0.15;
    }

    50% {
        opacity: 0.25;
    }
}

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

.animate-pulse-slow {
    animation: pulse-slow 3s ease-in-out infinite;
}

.animate-pulse-slower {
    animation: pulse-slower 4s ease-in-out infinite;
    animation-delay: -2s;
}
</style>