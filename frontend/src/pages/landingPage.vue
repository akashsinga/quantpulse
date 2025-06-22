<template>
    <div class="landing-page">
        <!-- Navigation -->
        <nav class="nav-bar">
            <div class="nav-container">
                <div class="nav-brand">
                    <div class="logo-container">
                        <img src="@/assets/images/favicon.svg" alt="QuantPulse" class="logo" />
                    </div>
                    <span class="brand-name">QuantPulse</span>
                </div>
                <div class="nav-menu">
                    <a href="#features" class="nav-link">Features</a>
                    <a href="#platform" class="nav-link">Platform</a>
                    <a href="#how-it-works" class="nav-link">How It Works</a>
                    <router-link to="/login" class="btn-primary">Get Started</router-link>
                </div>
                <div class="mobile-menu-container">
                    <button @click="toggleMobileMenu" class="mobile-menu-btn">
                        <i class="ph ph-list" v-if="!showMobileMenu"></i>
                        <i class="ph ph-x" v-else></i>
                    </button>
                    <div class="mobile-menu" :class="{ 'show': showMobileMenu }">
                        <a href="#features" class="mobile-nav-link" @click="closeMobileMenu">Features</a>
                        <a href="#platform" class="mobile-nav-link" @click="closeMobileMenu">Platform</a>
                        <a href="#how-it-works" class="mobile-nav-link" @click="closeMobileMenu">How It Works</a>
                        <router-link to="/login" class="btn-primary w-full text-center" @click="closeMobileMenu">Get
                            Started</router-link>
                    </div>
                </div>
                <button @click="toggleTheme" class="theme-toggle-btn">
                    <i class="ph ph-sun" v-if="isDark"></i>
                    <i class="ph ph-moon" v-else></i>
                </button>
            </div>
        </nav>

        <!-- Hero Section -->
        <section class="hero-section">
            <div class="hero-background">
                <div class="floating-elements">
                    <div class="float-element" v-for="i in 6" :key="i"></div>
                </div>
            </div>
            <div class="hero-container">
                <div class="hero-content">
                    <div class="hero-badge">
                        <div class="status-dot"></div>
                        <span>AI-Powered Trading Platform</span>
                    </div>
                    <h1 class="hero-title">
                        Where Data Becomes
                        <span class="gradient-text">Alpha</span>
                    </h1>
                    <p class="hero-description">
                        Advanced quantitative strategies meet cutting-edge AI. Backtest with precision,
                        trade with confidence, and consistently outperform the market with our
                        institutional-grade analytics platform.
                    </p>
                    <div class="hero-actions">
                        <router-link to="/login" class="btn-primary-lg">
                            <span>Start Building Strategies</span>
                            <i class="ph ph-arrow-right"></i>
                        </router-link>
                        <button class="btn-secondary-lg">
                            <i class="ph ph-book-open"></i>
                            <span>Documentation</span>
                        </button>
                    </div>
                    <div class="hero-badges">
                        <div class="badge-item">
                            <i class="ph ph-shield-check"></i>
                            <span>Secure & Reliable</span>
                        </div>
                        <div class="badge-item">
                            <i class="ph ph-lightning"></i>
                            <span>Real-time Processing</span>
                        </div>
                        <div class="badge-item">
                            <i class="ph ph-users"></i>
                            <span>Professional Grade</span>
                        </div>
                    </div>
                </div>
                <div class="hero-visual">
                    <div class="dashboard-preview">
                        <div class="preview-layout">
                            <div class="preview-sidebar">
                                <div class="sidebar-header">
                                    <div class="sidebar-logo">
                                        <div class="mini-logo">
                                            <i class="ph ph-pulse"></i>
                                        </div>
                                    </div>
                                </div>
                                <div class="sidebar-nav">
                                    <div class="nav-item active" title="Dashboard">
                                        <i class="ph ph-house"></i>
                                    </div>
                                    <div class="nav-item" title="Signals">
                                        <i class="ph ph-pulse"></i>
                                    </div>
                                    <div class="nav-item" title="Portfolio">
                                        <i class="ph ph-chart-pie"></i>
                                    </div>
                                    <div class="nav-item" title="Analytics">
                                        <i class="ph ph-chart-line-up"></i>
                                    </div>
                                    <div class="nav-item" title="Settings">
                                        <i class="ph ph-gear"></i>
                                    </div>
                                </div>
                                <div class="sidebar-footer">
                                    <div class="user-avatar">
                                        <i class="ph ph-user"></i>
                                    </div>
                                </div>
                            </div>
                            <div class="preview-main">
                                <div class="preview-header">
                                    <div class="preview-controls">
                                        <div class="control-dot red"></div>
                                        <div class="control-dot yellow"></div>
                                        <div class="control-dot green"></div>
                                    </div>
                                    <div class="header-content">
                                        <div class="header-title">Portfolio Dashboard</div>
                                        <div class="header-subtitle">Real-time Performance</div>
                                    </div>
                                    <div class="header-actions">
                                        <div class="notification-dot"></div>
                                    </div>
                                </div>
                                <div class="preview-content">
                                    <div class="performance-cards">
                                        <div class="perf-card">
                                            <div class="perf-label">Total Return</div>
                                            <div class="perf-value positive">+12.5%</div>
                                        </div>
                                        <div class="perf-card">
                                            <div class="perf-label">Today</div>
                                            <div class="perf-value positive">+2.1%</div>
                                        </div>
                                    </div>
                                    <div class="chart-container">
                                        <div class="chart-placeholder">
                                            <svg class="chart-svg" viewBox="0 0 280 100">
                                                <defs>
                                                    <linearGradient id="chartGradient" x1="0%" y1="0%" x2="0%"
                                                        y2="100%">
                                                        <stop offset="0%" style="stop-color:#3b82f6;stop-opacity:0.3" />
                                                        <stop offset="100%" style="stop-color:#3b82f6;stop-opacity:0" />
                                                    </linearGradient>
                                                </defs>
                                                <path class="chart-area"
                                                    d="M10,90 Q50,60 90,65 T170,45 Q210,35 250,25 T280,15 L280,90 Z"
                                                    fill="url(#chartGradient)" />
                                                <path class="chart-path"
                                                    d="M10,90 Q50,60 90,65 T170,45 Q210,35 250,25 T280,15" />
                                                <circle class="chart-dot" cx="280" cy="15" r="3" />
                                                <circle class="chart-point" cx="90" cy="65" r="2" />
                                                <circle class="chart-point" cx="170" cy="45" r="2" />
                                                <circle class="chart-point" cx="210" cy="35" r="2" />
                                            </svg>
                                        </div>
                                    </div>
                                    <div class="chart-stats">
                                        <div class="stat-item">
                                            <div class="stat-indicator bullish"></div>
                                            <div class="stat-content">
                                                <span class="stat-label">Volume</span>
                                                <span class="stat-value">1.2M</span>
                                            </div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="stat-indicator neutral"></div>
                                            <div class="stat-content">
                                                <span class="stat-label">Signals</span>
                                                <span class="stat-value">8 Active</span>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </section>

        <!-- Features Section -->
        <section id="features" class="features-section">
            <div class="section-container">
                <div class="section-header">
                    <h2 class="section-title">Platform Capabilities</h2>
                    <p class="section-description">
                        Comprehensive tools for professional traders and quantitative analysts
                    </p>
                </div>
                <div class="features-grid">
                    <div class="feature-card" v-for="(feature, index) in mainFeatures" :key="index">
                        <div class="feature-icon">
                            <i :class="feature.icon"></i>
                        </div>
                        <h3 class="feature-title">{{ feature.title }}</h3>
                        <p class="feature-description">{{ feature.description }}</p>
                        <div class="feature-benefits">
                            <div class="benefit-item" v-for="(benefit, idx) in feature.benefits" :key="idx">
                                <i class="ph ph-check"></i>
                                <span>{{ benefit }}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </section>

        <!-- Platform Overview -->
        <section id="platform" class="platform-section">
            <div class="section-container">
                <div class="platform-grid">
                    <div class="platform-content">
                        <h2 class="platform-title">Complete Trading Ecosystem</h2>
                        <p class="platform-description">
                            Everything you need for sophisticated quantitative trading in one integrated platform.
                        </p>
                        <div class="platform-features">
                            <div class="platform-feature" v-for="(feature, index) in platformFeatures" :key="index">
                                <div class="platform-icon">
                                    <i :class="feature.icon"></i>
                                </div>
                                <div class="platform-info">
                                    <h4 class="platform-feature-title">{{ feature.title }}</h4>
                                    <p class="platform-feature-desc">{{ feature.description }}</p>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="platform-visual">
                        <div class="platform-showcase">
                            <div class="showcase-item">
                                <div class="showcase-header">Real-time Signals</div>
                                <div class="showcase-content">
                                    <div class="signal-item bullish">
                                        <span class="signal-symbol">INFY</span>
                                        <span class="signal-action">BUY</span>
                                        <span class="signal-confidence">94%</span>
                                    </div>
                                    <div class="signal-item bearish">
                                        <span class="signal-symbol">TCS</span>
                                        <span class="signal-action">SELL</span>
                                        <span class="signal-confidence">87%</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </section>

        <!-- How It Works -->
        <section id="how-it-works" class="how-it-works">
            <div class="section-container">
                <div class="section-header">
                    <h2 class="section-title">How It Works</h2>
                    <p class="section-description">
                        Get started with quantitative trading in three simple steps
                    </p>
                </div>
                <div class="steps-grid">
                    <div class="step-item">
                        <div class="step-number">1</div>
                        <div class="step-icon">
                            <i class="ph ph-database"></i>
                        </div>
                        <h3 class="step-title">Connect Your Data</h3>
                        <p class="step-desc">Import historical data or connect to live market feeds from NSE, BSE and
                            other major exchanges</p>
                    </div>
                    <div class="step-connector">
                        <i class="ph ph-arrow-right"></i>
                    </div>
                    <div class="step-item">
                        <div class="step-number">2</div>
                        <div class="step-icon">
                            <i class="ph ph-code"></i>
                        </div>
                        <h3 class="step-title">Build & Test Strategies</h3>
                        <p class="step-desc">Use our visual builder or code custom strategies, then backtest against
                            historical data</p>
                    </div>
                    <div class="step-connector">
                        <i class="ph ph-arrow-right"></i>
                    </div>
                    <div class="step-item">
                        <div class="step-number">3</div>
                        <div class="step-icon">
                            <i class="ph ph-rocket-launch"></i>
                        </div>
                        <h3 class="step-title">Deploy & Monitor</h3>
                        <p class="step-desc">Launch your strategies with real-time monitoring and automated risk
                            management</p>
                    </div>
                </div>
            </div>
        </section>

        <!-- CTA Section -->
        <section class="cta-section">
            <div class="cta-container">
                <h2 class="cta-title">Ready to Start Smart Trading?</h2>
                <p class="cta-description">
                    Join professionals who use data-driven strategies to make informed trading decisions.
                </p>
                <div class="cta-actions">
                    <router-link to="/login" class="btn-primary-xl">Get Started</router-link>
                    <button class="btn-outline-xl">View Documentation</button>
                </div>
            </div>
        </section>

        <!-- Footer -->
        <footer class="footer">
            <div class="footer-container">
                <div class="footer-brand">
                    <div class="footer-logo">
                        <img src="@/assets/images/favicon.svg" alt="QuantPulse" class="logo" />
                        <span class="brand-name">QuantPulse</span>
                    </div>
                    <p class="footer-description">
                        Professional quantitative trading platform for institutional-grade analytics.
                    </p>
                </div>
                <div class="footer-links">
                    <div class="link-group">
                        <h4 class="link-title">Platform</h4>
                        <a href="#" class="footer-link">Features</a>
                        <a href="#" class="footer-link">Pricing</a>
                        <a href="#" class="footer-link">API</a>
                    </div>
                    <div class="link-group">
                        <h4 class="link-title">Support</h4>
                        <a href="#" class="footer-link">Documentation</a>
                        <a href="#" class="footer-link">Help Center</a>
                        <a href="#" class="footer-link">Contact</a>
                    </div>
                    <div class="link-group">
                        <h4 class="link-title">Legal</h4>
                        <a href="#" class="footer-link">Privacy Policy</a>
                        <a href="#" class="footer-link">Terms of Service</a>
                        <a href="#" class="footer-link">Security</a>
                    </div>
                </div>
            </div>
            <div class="footer-bottom">
                <p>&copy; 2025 QuantPulse. All rights reserved.</p>
            </div>
        </footer>
    </div>
</template>

<script>
export default {
    data() {
        return {
            isDark: false,
            showMobileMenu: false,
            mainFeatures: [
                {
                    icon: 'ph ph-pulse',
                    title: 'Real-time Signals',
                    description: 'Advanced machine learning algorithms analyze market data to generate actionable trading signals with high precision and reliability.',
                    benefits: ['Live market scanning across 1000+ instruments', 'Multi-timeframe analysis from 1-minute to monthly', 'Custom alert system with SMS and email notifications']
                },
                {
                    icon: 'ph ph-chart-line-up',
                    title: 'Strategy Backtesting',
                    description: 'Test your trading strategies against 20+ years of historical data with comprehensive performance metrics and risk analysis.',
                    benefits: ['Historical data back to 2000 for major markets', 'Risk-adjusted returns with Sharpe and Sortino ratios', 'Monte Carlo simulation for strategy validation']
                },
                {
                    icon: 'ph ph-shield-warning',
                    title: 'Risk Management',
                    description: 'Sophisticated portfolio protection with real-time monitoring and automated safeguards to minimize losses and maximize returns.',
                    benefits: ['Dynamic position sizing based on volatility', 'Real-time drawdown protection mechanisms', 'Advanced correlation analysis across assets']
                },
                {
                    icon: 'ph ph-brain',
                    title: 'AI Optimization',
                    description: 'Continuously improve strategy performance using machine learning and adaptive algorithms that learn from market conditions.',
                    benefits: ['Automated parameter optimization', 'Market regime detection and adaptation', 'Ensemble methods for signal combination']
                }
            ],
            platformFeatures: [
                {
                    icon: 'ph ph-database',
                    title: 'Market Data Integration',
                    description: 'Real-time and historical data from major exchanges worldwide'
                },
                {
                    icon: 'ph ph-chart-pie',
                    title: 'Portfolio Analytics',
                    description: 'Comprehensive performance tracking and risk attribution analysis'
                },
                {
                    icon: 'ph ph-bell',
                    title: 'Smart Notifications',
                    description: 'Customizable alerts for signals, market events, and portfolio changes'
                },
                {
                    icon: 'ph ph-code',
                    title: 'Strategy Builder',
                    description: 'Visual strategy designer with Python code export capabilities'
                },
                {
                    icon: 'ph ph-lightning',
                    title: 'High-Frequency Data',
                    description: 'Tick-level data processing for ultra-low latency strategies'
                },
                {
                    icon: 'ph ph-users',
                    title: 'Team Collaboration',
                    description: 'Share strategies and insights with your trading team'
                }
            ]
        }
    },
    methods: {
        toggleMobileMenu() {
            this.showMobileMenu = !this.showMobileMenu
        },
        closeMobileMenu() {
            this.showMobileMenu = false
        },
        toggleTheme() {
            this.isDark = !this.isDark
            this.applyTheme()
        },
        applyTheme() {
            const htmlElement = document.documentElement

            if (this.isDark) {
                htmlElement.classList.add('dark')
                localStorage.setItem('theme', 'dark')
            } else {
                htmlElement.classList.remove('dark')
                localStorage.setItem('theme', 'light')
            }
        },
        initTheme() {
            const savedTheme = localStorage.getItem('theme')
            const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches

            this.isDark = savedTheme === 'dark' || (!savedTheme && prefersDark)
            this.applyTheme()
        },
        initSmoothScrolling() {
            document.querySelectorAll('a[href^="#"]').forEach(anchor => {
                anchor.addEventListener('click', (e) => {
                    e.preventDefault()
                    const target = document.querySelector(anchor.getAttribute('href'))
                    if (target) {
                        target.scrollIntoView({
                            behavior: 'smooth',
                            block: 'start'
                        })
                        this.closeMobileMenu()
                    }
                })
            })
        }
    },
    mounted() {
        this.initTheme()
        this.initSmoothScrolling()

        // Listen for system theme changes
        if (window.matchMedia) {
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
                if (!localStorage.getItem('theme')) {
                    this.isDark = e.matches
                    this.applyTheme()
                }
            })
        }
    }
}
</script>

<style lang="postcss" scoped>
.landing-page {
    @apply min-h-screen bg-white dark:bg-slate-900;
}

/* Navigation */
.nav-bar {
    @apply fixed top-0 left-0 right-0 z-50 bg-white/80 dark:bg-slate-900/80 backdrop-blur-lg border-b border-slate-200 dark:border-slate-700;
}

.nav-container {
    @apply max-w-7xl mx-auto px-6 py-4 flex items-center justify-between;
}

.nav-brand {
    @apply flex items-center space-x-3;
}

.logo-container {
    @apply w-8 h-8 bg-slate-900 dark:bg-white rounded-lg flex items-center justify-center;
}

.logo {
    @apply w-5 h-5;
}

.brand-name {
    @apply text-xl font-bold text-slate-900 dark:text-white;
    font-family: 'Geist', sans-serif;
}

.nav-menu {
    @apply hidden md:flex items-center space-x-8;
}

.nav-link {
    @apply text-slate-600 dark:text-slate-300 hover:text-blue-600 dark:hover:text-blue-400 font-medium transition-colors;
}

.theme-toggle-btn {
    @apply w-10 h-10 bg-slate-100 dark:bg-slate-800 rounded-lg flex items-center justify-center text-slate-600 dark:text-slate-300 hover:text-blue-600 dark:hover:text-blue-400 transition-colors;
}

/* Mobile Menu */
.mobile-menu-container {
    @apply md:hidden relative;
}

.mobile-menu-btn {
    @apply w-10 h-10 bg-slate-100 dark:bg-slate-800 rounded-lg flex items-center justify-center text-slate-600 dark:text-slate-300 hover:text-blue-600 dark:hover:text-blue-400 transition-colors;
}

.mobile-menu {
    @apply absolute top-full right-0 mt-2 w-48 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-xl opacity-0 invisible transform scale-95 transition-all duration-200 origin-top-right;
}

.mobile-menu.show {
    @apply opacity-100 visible scale-100;
}

.mobile-nav-link {
    @apply block px-4 py-3 text-slate-600 dark:text-slate-300 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors;
}

.mobile-nav-link:first-child {
    @apply rounded-t-xl;
}

/* Floating Background Elements */
.hero-background {
    @apply absolute inset-0 overflow-hidden pointer-events-none;
}

.floating-elements {
    @apply absolute inset-0;
}

.float-element {
    @apply absolute w-4 h-4 bg-blue-500 dark:bg-blue-400 rounded-full opacity-20;
    animation: float-around 20s ease-in-out infinite;
}

.float-element:nth-child(1) {
    left: 10%;
    top: 20%;
    animation-delay: 0s;
}

.float-element:nth-child(2) {
    left: 80%;
    top: 40%;
    animation-delay: -5s;
}

.float-element:nth-child(3) {
    left: 30%;
    top: 70%;
    animation-delay: -10s;
}

.float-element:nth-child(4) {
    left: 70%;
    top: 10%;
    animation-delay: -15s;
}

.float-element:nth-child(5) {
    left: 50%;
    top: 60%;
    animation-delay: -20s;
}

.float-element:nth-child(6) {
    left: 20%;
    top: 80%;
    animation-delay: -25s;
}

/* Buttons */
.btn-primary {
    @apply px-4 py-2 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 transition-all duration-300 transform hover:scale-105;
}

.btn-primary-lg {
    @apply px-6 py-3 bg-blue-600 text-white rounded-xl font-semibold hover:bg-blue-700 transition-all duration-300 shadow-lg hover:shadow-xl transform hover:scale-105 flex items-center space-x-2;
}

.btn-primary-xl {
    @apply px-8 py-4 bg-blue-600 text-white rounded-xl font-semibold hover:bg-blue-700 transition-all duration-300 shadow-lg hover:shadow-xl text-lg transform hover:scale-105;
}

.btn-secondary-lg {
    @apply px-6 py-3 bg-slate-100 dark:bg-slate-800 text-slate-900 dark:text-white rounded-xl font-semibold hover:bg-slate-200 dark:hover:bg-slate-700 transition-all duration-300 flex items-center space-x-2 transform hover:scale-105;
}

.btn-outline-xl {
    @apply px-8 py-4 bg-white text-blue-600 rounded-xl font-semibold hover:bg-slate-50 transition-all duration-300 shadow-lg hover:shadow-xl text-lg transform hover:scale-105;
}

/* Hero Section */
.hero-section {
    @apply relative pt-24 pb-16 px-6 overflow-hidden;
}

.hero-container {
    @apply relative z-10 max-w-7xl mx-auto grid lg:grid-cols-2 gap-16 items-center;
}

.hero-content {
    @apply space-y-8;
}

.hero-badge {
    @apply inline-flex items-center space-x-2 px-4 py-2 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-full text-sm font-medium text-blue-800 dark:text-blue-200;
}

.status-dot {
    @apply w-2 h-2 bg-green-500 rounded-full;
    animation: pulse 2s ease-in-out infinite;
}

.hero-title {
    @apply text-4xl lg:text-6xl font-bold text-slate-900 dark:text-white leading-tight;
    font-family: 'Geist', sans-serif;
}

.gradient-text {
    @apply bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent;
}

.hero-description {
    @apply text-xl text-slate-600 dark:text-slate-300 leading-relaxed;
}

.hero-actions {
    @apply flex flex-col sm:flex-row gap-4;
}

.hero-badges {
    @apply flex flex-wrap gap-4 pt-8 border-t border-slate-200 dark:border-slate-700;
}

.badge-item {
    @apply flex items-center space-x-2 px-4 py-2 bg-slate-100 dark:bg-slate-800 rounded-lg text-sm font-medium text-slate-700 dark:text-slate-300 transition-all duration-300 hover:scale-105;
}

.badge-item i {
    @apply text-blue-600 dark:text-blue-400;
}

/* Enhanced Hero Visual with Improved Sidebar */
.hero-visual {
    @apply flex justify-center;
}

.dashboard-preview {
    @apply w-full max-w-lg bg-white dark:bg-slate-800 rounded-2xl border border-slate-200 dark:border-slate-700 shadow-2xl overflow-hidden transform hover:scale-105 transition-transform duration-500;
}

.preview-layout {
    @apply flex h-80;
}

.preview-sidebar {
    @apply w-16 bg-slate-50 dark:bg-slate-900 border-r border-slate-200 dark:border-slate-700 flex flex-col;
}

.sidebar-header {
    @apply p-3 border-b border-slate-200 dark:border-slate-700;
}

.sidebar-logo {
    @apply flex justify-center;
}

.mini-logo {
    @apply w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center text-white text-sm;
}

.sidebar-nav {
    @apply flex-1 p-2 space-y-1;
}

.nav-item {
    @apply w-12 h-10 rounded-lg flex items-center justify-center text-slate-500 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-800 transition-all duration-200 cursor-pointer text-sm relative;
}

.nav-item.active {
    @apply bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 shadow-sm;
}

.nav-item.active::after {
    @apply content-[''] absolute right-0 top-1/2 transform -translate-y-1/2 w-0.5 h-6 bg-blue-600 rounded-l;
}

.sidebar-footer {
    @apply p-2 border-t border-slate-200 dark:border-slate-700;
}

.user-avatar {
    @apply w-12 h-10 rounded-lg bg-slate-200 dark:bg-slate-800 flex items-center justify-center text-slate-600 dark:text-slate-300 text-sm;
}

.preview-main {
    @apply flex-1 flex flex-col;
}

.preview-header {
    @apply px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between bg-slate-50/50 dark:bg-slate-800/50;
}

.preview-controls {
    @apply flex space-x-2;
}

.control-dot {
    @apply w-3 h-3 rounded-full;
}

.control-dot.red {
    @apply bg-red-500;
}

.control-dot.yellow {
    @apply bg-yellow-500;
}

.control-dot.green {
    @apply bg-green-500;
}

.header-content {
    @apply text-center;
}

.header-title {
    @apply text-sm font-semibold text-slate-700 dark:text-slate-300;
}

.header-subtitle {
    @apply text-xs text-slate-500 dark:text-slate-400;
}

.header-actions {
    @apply flex items-center;
}

.notification-dot {
    @apply w-2 h-2 bg-red-500 rounded-full;
}

.preview-content {
    @apply flex-1 p-4 space-y-4;
}

.performance-cards {
    @apply flex space-x-3;
}

.perf-card {
    @apply flex-1 bg-slate-50 dark:bg-slate-800/50 rounded-lg p-3;
}

.perf-label {
    @apply text-xs text-slate-500 dark:text-slate-400;
}

.perf-value {
    @apply text-lg font-bold;
}

.perf-value.positive {
    @apply text-green-600;
}

.chart-container {
    @apply space-y-2;
}

.chart-placeholder {
    @apply h-20 relative;
}

.chart-svg {
    @apply w-full h-full;
}

.chart-area {
    opacity: 0.6;
}

.chart-path {
    @apply fill-none stroke-blue-500 stroke-2;
    stroke-linecap: round;
    stroke-linejoin: round;
    animation: draw-path 4s ease-in-out infinite;
}

.chart-dot {
    @apply fill-blue-500;
    animation: pulse-dot 2s ease-in-out infinite;
}

.chart-point {
    @apply fill-blue-400 opacity-60;
    animation: pulse-point 3s ease-in-out infinite;
}

.chart-stats {
    @apply flex space-x-4;
}

.stat-item {
    @apply flex items-center space-x-2;
}

.stat-indicator {
    @apply w-2 h-2 rounded-full;
}

.stat-indicator.bullish {
    @apply bg-green-500;
}

.stat-indicator.neutral {
    @apply bg-blue-500;
}

.stat-content {
    @apply flex flex-col;
}

.stat-label {
    @apply text-xs text-slate-500 dark:text-slate-400;
}

.stat-value {
    @apply text-xs font-semibold text-slate-900 dark:text-white;
}

/* Sections */
.section-container {
    @apply max-w-7xl mx-auto px-6 py-16;
}

.section-header {
    @apply text-center mb-16;
}

.section-title {
    @apply text-3xl lg:text-4xl font-bold text-slate-900 dark:text-white mb-4;
    font-family: 'Geist', sans-serif;
}

.section-description {
    @apply text-xl text-slate-600 dark:text-slate-300;
}

/* Features */
.features-section {
    @apply bg-slate-50 dark:bg-slate-800;
}

.features-grid {
    @apply grid md:grid-cols-2 lg:grid-cols-2 gap-8;
}

.feature-card {
    @apply bg-white dark:bg-slate-900 p-8 rounded-2xl border border-slate-200 dark:border-slate-700 hover:shadow-xl hover:border-blue-300 dark:hover:border-blue-600 transition-all duration-300 transform hover:-translate-y-2;
}

.feature-icon {
    @apply w-12 h-12 bg-blue-600 text-white rounded-xl flex items-center justify-center text-xl mb-6;
}

.feature-title {
    @apply text-xl font-bold text-slate-900 dark:text-white mb-4;
}

.feature-description {
    @apply text-slate-600 dark:text-slate-300 mb-6 leading-relaxed;
}

.feature-benefits {
    @apply space-y-3;
}

.benefit-item {
    @apply flex items-start space-x-3 text-sm;
}

.benefit-item i {
    @apply text-green-600 mt-0.5 flex-shrink-0;
}

.benefit-item span {
    @apply text-slate-700 dark:text-slate-300 leading-relaxed;
}

/* Platform Section */
.platform-section {
    @apply bg-white dark:bg-slate-900;
}

.platform-grid {
    @apply grid lg:grid-cols-2 gap-16 items-center;
}

.platform-content {
    @apply space-y-8;
}

.platform-title {
    @apply text-3xl lg:text-4xl font-bold text-slate-900 dark:text-white;
    font-family: 'Geist', sans-serif;
}

.platform-description {
    @apply text-xl text-slate-600 dark:text-slate-300 leading-relaxed;
}

.platform-features {
    @apply grid grid-cols-2 gap-6;
}

.platform-feature {
    @apply flex items-start space-x-4 ring-1 p-4 ring-slate-200 dark:ring-slate-700 rounded-lg hover:ring-blue-300 dark:hover:ring-blue-600 hover:shadow-md transition-all duration-300 transform hover:scale-105;
}

.platform-icon {
    @apply w-10 h-10 bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 rounded-lg flex items-center justify-center flex-shrink-0;
}

.platform-info {
    @apply space-y-2;
}

.platform-feature-title {
    @apply font-semibold text-slate-900 dark:text-white;
}

.platform-feature-desc {
    @apply text-slate-600 dark:text-slate-300 text-sm;
}

/* Platform Visual */
.platform-showcase {
    @apply bg-slate-50 dark:bg-slate-800 rounded-2xl p-8 border border-slate-200 dark:border-slate-700;
}

.showcase-item {
    @apply space-y-4;
}

.showcase-header {
    @apply font-semibold text-slate-900 dark:text-white;
}

.showcase-content {
    @apply space-y-3;
}

.signal-item {
    @apply flex items-center justify-between p-4 rounded-lg;
}

.signal-item.bullish {
    @apply bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800;
}

.signal-item.bearish {
    @apply bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800;
}

.signal-symbol {
    @apply font-bold text-slate-900 dark:text-white;
}

.signal-action {
    @apply font-semibold;
}

.signal-item.bullish .signal-action {
    @apply text-green-700 dark:text-green-300;
}

.signal-item.bearish .signal-action {
    @apply text-red-700 dark:text-red-300;
}

.signal-confidence {
    @apply text-sm font-medium text-slate-600 dark:text-slate-300;
}

/* Enhanced How It Works */
.how-it-works {
    @apply bg-white dark:bg-slate-900;
}

.steps-grid {
    @apply flex flex-col lg:flex-row items-center justify-center gap-8 lg:gap-4;
}

.step-item {
    @apply text-center space-y-4 max-w-xs;
}

.step-number {
    @apply w-16 h-16 bg-blue-600 text-white rounded-full flex items-center justify-center text-2xl font-bold mx-auto relative z-10;
}

.step-icon {
    @apply w-12 h-12 bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-xl flex items-center justify-center text-xl mx-auto -mt-2;
}

.step-title {
    @apply text-xl font-bold text-slate-900 dark:text-white;
}

.step-desc {
    @apply text-slate-600 dark:text-slate-300 leading-relaxed;
}

.step-connector {
    @apply hidden lg:flex items-center justify-center text-2xl text-slate-300 dark:text-slate-600;
}

/* CTA Section */
.cta-section {
    @apply bg-gradient-to-r from-blue-600 to-blue-700 text-white;
}

.cta-container {
    @apply max-w-4xl mx-auto px-6 py-20 text-center space-y-8;
}

.cta-title {
    @apply text-3xl lg:text-4xl font-bold;
    font-family: 'Geist', sans-serif;
}

.cta-description {
    @apply text-xl opacity-90 max-w-2xl mx-auto;
}

.cta-actions {
    @apply flex flex-col sm:flex-row gap-4 justify-center;
}

/* Footer */
.footer {
    @apply bg-slate-900 text-white;
}

.footer-container {
    @apply max-w-7xl mx-auto px-6 py-16 grid md:grid-cols-4 gap-8;
}

.footer-brand {
    @apply space-y-4;
}

.footer-logo {
    @apply flex items-center space-x-3;
}

.footer-description {
    @apply text-slate-300 leading-relaxed;
}

.footer-links {
    @apply md:col-span-3 grid md:grid-cols-3 gap-8;
}

.link-group {
    @apply space-y-4;
}

.link-title {
    @apply font-semibold text-white;
}

.footer-link {
    @apply block text-slate-300 hover:text-white transition-colors;
}

.footer-bottom {
    @apply border-t border-slate-700 py-8 text-center text-slate-400;
}

/* Enhanced Animations */
@keyframes float-around {

    0%,
    100% {
        transform: translateY(0px) translateX(0px) rotate(0deg);
    }

    25% {
        transform: translateY(-20px) translateX(10px) rotate(90deg);
    }

    50% {
        transform: translateY(-10px) translateX(-10px) rotate(180deg);
    }

    75% {
        transform: translateY(-30px) translateX(5px) rotate(270deg);
    }
}

@keyframes draw-path {
    0% {
        stroke-dasharray: 0 1000;
    }

    50% {
        stroke-dasharray: 200 1000;
    }

    100% {
        stroke-dasharray: 0 1000;
    }
}

@keyframes pulse-dot {

    0%,
    100% {
        opacity: 1;
        transform: scale(1);
    }

    50% {
        opacity: 0.7;
        transform: scale(1.2);
    }
}

@keyframes pulse {

    0%,
    100% {
        opacity: 1;
    }

    50% {
        opacity: 0.5;
    }
}

@keyframes pulse-point {

    0%,
    100% {
        opacity: 0.6;
        transform: scale(1);
    }

    50% {
        opacity: 0.8;
        transform: scale(1.3);
    }
}

/* Responsive Improvements */
@media (max-width: 768px) {
    .hero-title {
        @apply text-3xl;
    }

    .section-title {
        @apply text-2xl;
    }

    .nav-menu {
        @apply hidden;
    }

    .hero-actions {
        @apply flex-col;
    }

    .hero-badges {
        @apply justify-center;
    }

    .steps-grid {
        @apply flex-col gap-8;
    }

    .step-connector {
        @apply hidden;
    }

    .platform-features {
        @apply grid-cols-1;
    }

    .features-grid {
        @apply grid-cols-1;
    }

    .floating-elements {
        @apply hidden;
    }
}

@media (max-width: 640px) {
    .hero-container {
        @apply gap-8;
    }

    .section-container {
        @apply px-4 py-12;
    }

    .nav-container {
        @apply px-4;
    }

    .cta-container {
        @apply px-4 py-16;
    }

    .footer-container {
        @apply px-4 grid-cols-1 gap-8;
    }
}
</style>