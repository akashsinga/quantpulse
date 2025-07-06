<template>
    <div class="qp-min-h-screen qp-bg-white qp-text-slate-900 qp-relative">
        <!-- Enhanced background effects -->
        <div class="qp-fixed qp-inset-0 qp-pointer-events-none qp-z-0">
            <!-- Animated gradient mesh -->
            <div class="qp-absolute qp-inset-0 qp-bg-gradient-to-br qp-from-slate-50/50 qp-via-blue-50/30 qp-to-slate-50/50"></div>

            <!-- CSS dot pattern -->
            <div class="qp-absolute qp-inset-0 qp-opacity-80 dot-pattern"></div>

            <!-- Floating orbs -->
            <div class="qp-absolute qp-top-20 qp-left-10 qp-w-64 qp-h-64 qp-bg-blue-100 qp-rounded-full qp-opacity-20 qp-blur-3xl animate-float-slow">
            </div>
            <div class="qp-absolute qp-bottom-32 qp-right-16 qp-w-80 qp-h-80 qp-bg-slate-100 qp-rounded-full qp-opacity-25 qp-blur-3xl animate-float-slow-reverse">
            </div>
            <div class="qp-absolute qp-top-1/2 qp-left-1/3 qp-w-48 qp-h-48 qp-bg-blue-50 qp-rounded-full qp-opacity-15 qp-blur-2xl animate-float-slower">
            </div>
        </div>

        <div class="qp-relative qp-z-10">
            <!-- Simplified Header -->
            <header class="qp-fixed qp-w-full qp-top-0 qp-z-50 qp-bg-white/95 qp-backdrop-blur-sm qp-border-b qp-border-slate-200">
                <nav class="qp-max-w-7xl qp-mx-auto qp-px-6 qp-py-4">
                    <div class="qp-flex qp-items-center qp-justify-between">
                        <div class="qp-flex qp-items-center qp-space-x-3">
                            <div class="qp-w-10 qp-h-10 qp-bg-slate-900 qp-rounded-xl qp-flex qp-items-center qp-justify-center">
                                <img src="@/assets/images/favicon.svg">
                            </div>
                            <span class="qp-text-2xl qp-font-bold qp-text-slate-900">QuantPulse</span>
                        </div>
                        <div class="qp-hidden md:qp-flex qp-items-center qp-space-x-8">
                            <a href="#features" class="qp-text-slate-600 hover:qp-text-slate-900 qp-transition-colors qp-font-medium">{{ landingPageI18n.navbar.features }}</a>
                            <a href="#how-it-works" class="qp-text-slate-600 hover:qp-text-slate-900 qp-transition-colors qp-font-medium">{{ landingPageI18n.navbar.howItWorks }}</a>
                            <a href="#contact" class="qp-text-slate-600 hover:qp-text-slate-900 qp-transition-colors qp-font-medium">{{ landingPageI18n.navbar.contact }}</a>
                            <button class="qp-bg-slate-900 qp-text-white qp-px-6 qp-py-2.5 qp-rounded-lg hover:qp-bg-slate-800 qp-transition-colors qp-font-medium">{{ landingPageI18n.navbar.getStarted }}</button>
                        </div>
                    </div>
                </nav>
            </header>

            <!-- Enhanced Hero -->
            <section class="qp-pt-32 qp-pb-20 qp-relative qp-overflow-hidden">
                <!-- Section-specific background effects -->
                <div class="qp-absolute qp-inset-0 qp-bg-gradient-to-b qp-from-transparent qp-via-blue-50/20 qp-to-transparent"></div>
                <div class="qp-absolute qp-top-10 qp-right-10 qp-w-32 qp-h-32 qp-bg-blue-200 qp-rounded-full qp-opacity-20 qp-blur-2xl animate-pulse-slow">
                </div>

                <div class="qp-max-w-7xl qp-mx-auto qp-px-6 qp-text-center qp-relative qp-z-10">
                    <div class="qp-max-w-4xl qp-mx-auto">
                        <div class="qp-inline-flex qp-items-center qp-px-4 qp-py-2 qp-rounded-full qp-bg-slate-100 qp-text-slate-700 qp-text-sm qp-font-medium qp-mb-8">
                            <div class="qp-w-2 qp-h-2 qp-bg-blue-500 qp-rounded-full qp-mr-2"></div>
                            {{ landingPageI18n.heroSection.aiPoweredTradingPlatform }}
                        </div>

                        <h1 class="qp-text-5xl md:qp-text-6xl qp-font-bold qp-text-slate-900 qp-mb-8 qp-leading-tight">
                            {{ landingPageI18n.heroSection.whereDataBecomes }}
                            <span class="qp-text-blue-600">{{ landingPageI18n.heroSection.alpha }}</span>
                        </h1>

                        <p class="qp-text-xl qp-text-slate-600 qp-mb-12 qp-max-w-3xl qp-mx-auto qp-leading-relaxed">
                            {{ landingPageI18n.heroSection.subtitle }}
                        </p>

                        <div class="qp-flex qp-flex-col sm:qp-flex-row qp-gap-4 qp-justify-center">
                            <button class="qp-bg-slate-900 qp-text-white qp-px-8 qp-py-4 qp-rounded-lg qp-text-lg qp-font-semibold hover:qp-bg-slate-800 qp-transition-colors">
                                {{ landingPageI18n.heroSection.startBuildingStrategies }}
                            </button>
                            <button class="qp-border-2 qp-border-slate-300 qp-text-slate-700 qp-px-8 qp-py-4 qp-rounded-lg qp-text-lg qp-font-semibold hover:qp-bg-slate-50 qp-transition-colors">
                                {{ landingPageI18n.heroSection.documentation }}
                            </button>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced Features -->
            <section id="features" class="qp-py-20 qp-bg-slate-50 qp-relative qp-overflow-hidden qp-border-y">
                <!-- Section background effects -->
                <div class="qp-absolute qp-inset-0 qp-bg-gradient-to-r qp-from-slate-50 qp-via-blue-50/50 qp-to-slate-50"></div>
                <div class="qp-absolute qp-bottom-0 qp-left-0 qp-w-96 qp-h-96 qp-bg-slate-200 qp-rounded-full qp-opacity-10 qp-blur-3xl"></div>

                <div class="qp-max-w-7xl qp-mx-auto qp-px-6 qp-relative qp-z-10">
                    <div class="qp-text-center qp-mb-16">
                        <h2 class="qp-text-4xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.features.platformCapabilities }}</h2>
                        <p class="qp-text-xl qp-text-slate-600 qp-max-w-3xl qp-mx-auto">{{ landingPageI18n.features.platformCapabilitiesSubtitle }}</p>
                    </div>

                    <div class="qp-grid md:qp-grid-cols-2 qp-gap-8 qp-max-w-5xl qp-mx-auto">
                        <div class="qp-bg-white qp-p-8 qp-rounded-xl qp-border qp-border-slate-200 hover:qp-shadow-lg qp-transition-shadow">
                            <div class="qp-w-12 qp-h-12 qp-bg-slate-900 qp-rounded-lg qp-flex qp-items-center qp-justify-center qp-mb-6">
                                <i class="ph ph-pulse qp-text-white qp-text-xl"></i>
                            </div>
                            <h3 class="qp-text-xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.features.featureList.realTimeSignalGeneration }}</h3>
                            <p class="qp-text-slate-600 qp-leading-relaxed">{{ landingPageI18n.features.featureList.realTimeSignalGenerationSubtitle }}</p>
                        </div>

                        <div class="qp-bg-white qp-p-8 qp-rounded-xl qp-border qp-border-slate-200 hover:qp-shadow-lg qp-transition-shadow">
                            <div class="qp-w-12 qp-h-12 qp-bg-slate-900 qp-rounded-lg qp-flex qp-items-center qp-justify-center qp-mb-6">
                                <i class="ph ph-chart-line-up qp-text-white qp-text-xl"></i>
                            </div>
                            <h3 class="qp-text-xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.features.featureList.historicalStrategyBacktesting }}</h3>
                            <p class="qp-text-slate-600 qp-leading-relaxed">{{ landingPageI18n.features.featureList.historicalStrategyBacktestingSubtitle }}</p>
                        </div>

                        <div class="qp-bg-white qp-p-8 qp-rounded-xl qp-border qp-border-slate-200 hover:qp-shadow-lg qp-transition-shadow">
                            <div class="qp-w-12 qp-h-12 qp-bg-slate-900 qp-rounded-lg qp-flex qp-items-center qp-justify-center qp-mb-6">
                                <i class="ph ph-shield-warning qp-text-white qp-text-xl"></i>
                            </div>
                            <h3 class="qp-text-xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.features.featureList.portfolioRiskManagement }}</h3>
                            <p class="qp-text-slate-600 qp-leading-relaxed">{{ landingPageI18n.features.featureList.portfolioRiskManagementSubtitle }}</p>
                        </div>

                        <div class="qp-bg-white qp-p-8 qp-rounded-xl qp-border qp-border-slate-200 hover:qp-shadow-lg qp-transition-shadow">
                            <div class="qp-w-12 qp-h-12 qp-bg-slate-900 qp-rounded-lg qp-flex qp-items-center qp-justify-center qp-mb-6">
                                <i class="ph ph-brain qp-text-white qp-text-xl"></i>
                            </div>
                            <h3 class="qp-text-xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.features.featureList.machineLearningEnhancement }}</h3>
                            <p class="qp-text-slate-600 qp-leading-relaxed">{{ landingPageI18n.features.featureList.machineLearningEnhancementSubtitle }}</p>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced Platform Overview -->
            <section class="qp-py-20 qp-relative qp-overflow-hidden">
                <!-- Section background effects -->
                <div class="qp-absolute qp-top-0 qp-right-0 qp-w-72 qp-h-72 qp-bg-blue-100 qp-rounded-full qp-opacity-15 qp-blur-3xl"></div>
                <div class="qp-absolute qp-bottom-10 qp-left-10 qp-w-64 qp-h-64 qp-bg-slate-100 qp-rounded-full qp-opacity-20 qp-blur-2xl"></div>

                <div class="qp-max-w-7xl qp-mx-auto qp-px-6 qp-relative qp-z-10">
                    <div class="qp-grid lg:qp-grid-cols-2 qp-gap-16 qp-items-center">
                        <div>
                            <div class="qp-inline-flex qp-items-center qp-px-3 qp-py-1 qp-rounded-full qp-bg-blue-100 qp-text-blue-700 qp-text-sm qp-font-medium qp-mb-6">{{ landingPageI18n.platformOverview.smartSolutions }}</div>

                            <h2 class="qp-text-4xl qp-font-bold qp-text-slate-900 qp-mb-6">
                                {{ landingPageI18n.platformOverview.smartTrading }}
                                <span class="qp-text-blue-600">{{ landingPageI18n.platformOverview.solutions }}</span>
                            </h2>

                            <p class="qp-text-xl qp-text-slate-600 qp-mb-8 qp-leading-relaxed">{{ landingPageI18n.platformOverview.subtitle }}</p>

                            <div class="qp-space-y-6">
                                <div class="qp-flex qp-items-start qp-space-x-4">
                                    <div class="qp-w-2 qp-h-2 qp-bg-blue-500 qp-rounded-full qp-mt-3 qp-flex-shrink-0"></div>
                                    <div>
                                        <h4 class="qp-text-slate-900 qp-font-semibold qp-text-lg qp-mb-2">{{ landingPageI18n.platformOverview.solutionList.strategyDefinitionFramework }}</h4>
                                        <p class="qp-text-slate-600">{{ landingPageI18n.platformOverview.solutionList.strategyDefinitionFrameworkSubtitle }}</p>
                                    </div>
                                </div>

                                <div class="qp-flex qp-items-start qp-space-x-4">
                                    <div class="qp-w-2 qp-h-2 qp-bg-blue-500 qp-rounded-full qp-mt-3 qp-flex-shrink-0"></div>
                                    <div>
                                        <h4 class="qp-text-slate-900 qp-font-semibold qp-text-lg qp-mb-2">{{ landingPageI18n.platformOverview.solutionList.signalEnhancement }}</h4>
                                        <p class="qp-text-slate-600">{{ landingPageI18n.platformOverview.solutionList.signalEnhancementSubtitle }}</p>
                                    </div>
                                </div>

                                <div class="qp-flex qp-items-start qp-space-x-4">
                                    <div class="qp-w-2 qp-h-2 qp-bg-blue-500 qp-rounded-full qp-mt-3 qp-flex-shrink-0"></div>
                                    <div>
                                        <h4 class="qp-text-slate-900 qp-font-semibold qp-text-lg qp-mb-2">{{ landingPageI18n.platformOverview.solutionList.performanceAnalytics }}</h4>
                                        <p class="qp-text-slate-600">{{ landingPageI18n.platformOverview.solutionList.performanceAnalyticsSubtitle }}</p>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div class="qp-bg-slate-50 qp-p-12 qp-rounded-2xl qp-border qp-border-slate-200">
                            <div class="qp-text-center">
                                <div class="qp-w-16 qp-h-16 qp-bg-slate-900 qp-rounded-2xl qp-flex qp-items-center qp-justify-center qp-mx-auto qp-mb-6">
                                    <i class="ph ph-chart-line qp-text-white qp-text-2xl"></i>
                                </div>
                                <h4 class="qp-text-slate-900 qp-font-bold qp-text-xl qp-mb-4">{{ landingPageI18n.platformOverview.professionalGradePlatform }}</h4>
                                <p class="qp-text-slate-600 qp-leading-relaxed">{{ landingPageI18n.platformOverview.professionalGradePlatformSubtitle }}</p>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced How It Works -->
            <section id="how-it-works" class="qp-py-20 qp-bg-slate-50 qp-relative qp-overflow-hidden qp-border-y">
                <!-- Section background effects -->
                <div class="qp-absolute qp-inset-0 qp-bg-gradient-to-b qp-from-slate-50 qp-via-white/80 qp-to-slate-50"></div>
                <div class="qp-absolute qp-top-20 qp-left-1/4 qp-w-48 qp-h-48 qp-bg-blue-100 qp-rounded-full qp-opacity-15 qp-blur-3xl animate-pulse-slower">
                </div>
                <div class="qp-absolute qp-bottom-20 qp-right-1/4 qp-w-56 qp-h-56 qp-bg-slate-200 qp-rounded-full qp-opacity-20 qp-blur-2xl"></div>

                <div class="qp-max-w-7xl qp-mx-auto qp-px-6 qp-relative qp-z-10">
                    <div class="qp-text-center qp-mb-16">
                        <h2 class="qp-text-4xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.howItWorks.howItWorks }}</h2>
                        <p class="qp-text-xl qp-text-slate-600 qp-max-w-3xl qp-mx-auto">{{ landingPageI18n.howItWorks.howItWorksSubtitle }}</p>
                    </div>

                    <div class="qp-max-w-4xl qp-mx-auto">
                        <div class="qp-space-y-12">
                            <div class="qp-flex qp-flex-col md:qp-flex-row qp-items-center qp-gap-8">
                                <div class="qp-w-16 qp-h-16 qp-bg-slate-900 qp-rounded-full qp-flex qp-items-center qp-justify-center qp-flex-shrink-0">
                                    <span class="qp-text-white qp-font-bold qp-text-xl">1</span>
                                </div>
                                <div class="qp-text-center md:qp-text-left">
                                    <h3 class="qp-text-2xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.howItWorks.steps.defineYourStrategy }}</h3>
                                    <p class="qp-text-slate-600 qp-text-lg qp-leading-relaxed">{{ landingPageI18n.howItWorks.steps.defineYourStrategySubtitle }}</p>
                                </div>
                            </div>

                            <div class="qp-flex qp-flex-col md:qp-flex-row qp-items-center qp-gap-8">
                                <div class="qp-w-16 qp-h-16 qp-bg-slate-900 qp-rounded-full qp-flex qp-items-center qp-justify-center qp-flex-shrink-0">
                                    <span class="qp-text-white qp-font-bold qp-text-xl">2</span>
                                </div>
                                <div class="qp-text-center md:qp-text-left">
                                    <h3 class="qp-text-2xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.howItWorks.steps.testAndOptimize }}</h3>
                                    <p class="qp-text-slate-600 qp-text-lg qp-leading-relaxed">{{ landingPageI18n.howItWorks.steps.testAndOptimizeSubtitle }}</p>
                                </div>
                            </div>

                            <div class="qp-flex qp-flex-col md:qp-flex-row qp-items-center qp-gap-8">
                                <div class="qp-w-16 qp-h-16 qp-bg-slate-900 qp-rounded-full qp-flex qp-items-center qp-justify-center qp-flex-shrink-0">
                                    <span class="qp-text-white qp-font-bold qp-text-xl">3</span>
                                </div>
                                <div class="qp-text-center md:qp-text-left">
                                    <h3 class="qp-text-2xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.howItWorks.steps.getSignals }}</h3>
                                    <p class="qp-text-slate-600 qp-text-lg qp-leading-relaxed">{{ landingPageI18n.howItWorks.steps.getSignalsSubtitle }}</p>
                                </div>
                            </div>

                            <div class="qp-flex qp-flex-col md:qp-flex-row qp-items-center qp-gap-8">
                                <div class="qp-w-16 qp-h-16 qp-bg-slate-900 qp-rounded-full qp-flex qp-items-center qp-justify-center qp-flex-shrink-0">
                                    <span class="qp-text-white qp-font-bold qp-text-xl">4</span>
                                </div>
                                <div class="qp-text-center md:qp-text-left">
                                    <h3 class="qp-text-2xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.howItWorks.steps.trackPerformance }}</h3>
                                    <p class="qp-text-slate-600 qp-text-lg qp-leading-relaxed">{{ landingPageI18n.howItWorks.steps.trackPerformanceSubtitle }}</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced Stats -->
            <section class="qp-py-20 qp-relative qp-overflow-hidden">
                <!-- Section background effects -->
                <div class="qp-absolute qp-inset-0 qp-bg-gradient-to-br qp-from-transparent qp-via-slate-50/30 qp-to-transparent"></div>
                <div class="qp-absolute qp-top-10 qp-left-10 qp-w-40 qp-h-40 qp-bg-blue-200 qp-rounded-full qp-opacity-10 qp-blur-2xl"></div>
                <div class="qp-absolute qp-bottom-10 qp-right-10 qp-w-52 qp-h-52 qp-bg-slate-200 qp-rounded-full qp-opacity-15 qp-blur-3xl"></div>

                <div class="qp-max-w-7xl qp-mx-auto qp-px-6 qp-relative qp-z-10">
                    <div class="qp-text-center qp-mb-12">
                        <h2 class="qp-text-3xl qp-font-bold qp-text-slate-900 qp-mb-4">{{ landingPageI18n.stats.trustedProfessionals }}</h2>
                        <p class="qp-text-lg qp-text-slate-600">{{ landingPageI18n.stats.trustedProfessionalsSubtitle }}</p>
                    </div>

                    <div class="qp-grid qp-grid-cols-2 md:qp-grid-cols-4 qp-gap-8 qp-text-center qp-max-w-4xl qp-mx-auto">
                        <div class="qp-p-6">
                            <div class="qp-text-4xl qp-font-bold qp-text-slate-900 qp-mb-2">25+</div>
                            <div class="qp-text-slate-600 qp-font-medium">{{ landingPageI18n.stats.services.yearsOfData }}</div>
                        </div>

                        <div class="qp-p-6">
                            <div class="qp-text-4xl qp-font-bold qp-text-slate-900 qp-mb-2">7</div>
                            <div class="qp-text-slate-600 qp-font-medium">{{ landingPageI18n.stats.services.coreServices }}</div>
                        </div>

                        <div class="qp-p-6">
                            <div class="qp-text-4xl qp-font-bold qp-text-slate-900 qp-mb-2">24/7</div>
                            <div class="qp-text-slate-600 qp-font-medium">{{ landingPageI18n.stats.services.marketMonitoring }}</div>
                        </div>

                        <div class="qp-p-6">
                            <div class="qp-text-4xl qp-font-bold qp-text-slate-900 qp-mb-2">100%</div>
                            <div class="qp-text-slate-600 qp-font-medium">{{ landingPageI18n.stats.services.securePlatform }}</div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Enhanced CTA -->
            <section class="qp-py-20 qp-bg-slate-50 qp-relative qp-overflow-hidden qp-border-y">
                <!-- Section background effects -->
                <div class="qp-absolute qp-inset-0 qp-bg-gradient-to-r qp-from-slate-50 qp-via-blue-50/40 qp-to-slate-50"></div>
                <div class="qp-absolute qp-top-0 qp-left-1/3 qp-w-60 qp-h-60 qp-bg-blue-100 qp-rounded-full qp-opacity-20 qp-blur-3xl"></div>
                <div class="qp-absolute qp-bottom-0 qp-right-1/4 qp-w-44 qp-h-44 qp-bg-slate-200 qp-rounded-full qp-opacity-25 qp-blur-2xl"></div>

                <div class="qp-max-w-7xl qp-mx-auto qp-px-6 qp-text-center qp-relative qp-z-10">
                    <div class="qp-max-w-3xl qp-mx-auto">
                        <h2 class="qp-text-4xl qp-font-bold qp-text-slate-900 qp-mb-6">{{ landingPageI18n.cta.readyToElevate }}</h2>
                        <p class="qp-text-xl qp-text-slate-600 qp-mb-8">{{ landingPageI18n.cta.joinTraders }}</p>
                        <button class="qp-bg-slate-900 qp-text-white qp-px-12 qp-py-4 qp-rounded-lg qp-text-lg qp-font-semibold hover:qp-bg-slate-800 qp-transition-colors">{{ landingPageI18n.cta.startYourJourney }}</button>
                    </div>
                </div>
            </section>

            <!-- Simplified Footer -->
            <footer id="contact" class="qp-bg-slate-900 qp-text-white qp-py-16">
                <div class="qp-max-w-7xl qp-mx-auto qp-px-6">
                    <div class="qp-grid md:qp-grid-cols-3 qp-gap-12 qp-mb-12">
                        <div>
                            <div class="qp-flex qp-items-center qp-space-x-3 qp-mb-6">
                                <div class="qp-w-10 qp-h-10 qp-bg-white qp-rounded-xl qp-flex qp-items-center qp-justify-center">
                                    <svg viewBox="0 0 100 100" class="qp-w-6 qp-h-6 qp-text-slate-900">
                                        <path d="M15 50 L25 50 L30 35 L35 65 L40 25 L45 75 L50 15 L55 85 L60 30 L65 70 L70 40 L75 50 L85 50" fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />
                                    </svg>
                                </div>
                                <span class="qp-text-2xl qp-font-bold">QuantPulse</span>
                            </div>
                            <p class="qp-text-slate-300 qp-leading-relaxed">{{ landingPageI18n.footer.description }}</p>
                        </div>

                        <div>
                            <h4 class="qp-text-white qp-font-semibold qp-mb-6">{{ landingPageI18n.footer.platform }}</h4>
                            <ul class="qp-space-y-3 qp-text-slate-300">
                                <li>{{ landingPageI18n.footer.strategyBuilder }}</li>
                                <li>{{ landingPageI18n.footer.backtestingEngine }}</li>
                                <li>{{ landingPageI18n.footer.signalGeneration }}</li>
                                <li>{{ landingPageI18n.footer.portfolioTracking }}</li>
                            </ul>
                        </div>

                        <div>
                            <h4 class="qp-text-white qp-font-semibold qp-mb-6">{{ landingPageI18n.navbar.getStarted }}</h4>
                            <p class="qp-text-slate-300 qp-mb-6">{{ landingPageI18n.footer.readyToTransform }}</p>
                            <button class="qp-border qp-border-white qp-text-white qp-px-6 qp-py-3 qp-rounded-lg hover:qp-bg-white hover:qp-text-slate-900 qp-transition-colors">{{ landingPageI18n.footer.contactUs }}</button>
                        </div>
                    </div>

                    <div class="qp-border-t qp-border-slate-700 qp-pt-8 qp-text-center qp-text-slate-400">
                        <p>&copy; 2025 QuantPulse. {{ landingPageI18n.footer.allRightsReserved }}</p>
                    </div>
                </div>
            </footer>
        </div>
    </div>
</template>

<script>
export default {
    name: 'QuantPulseLanding',
    data() {
        return {
            landingPageI18n: this.$tm('pages.landingPage')
        }
    },
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
.qp-max-w-7xl {
    max-width: 80rem;
}

/* Focus states for accessibility */
a {
    @apply qp-px-3;
    @apply qp-py-1;

    &:focus {
        @apply qp-ring-1;
        @apply qp-ring-blue-100;
        @apply qp-rounded-md;
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