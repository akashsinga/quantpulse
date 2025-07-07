<template>
    <div class="admin-layout">
        <aside class="sidebar" :class="{ 'collapsed': isSidebarCollapsed }">
            <!-- Logo & Brand -->
            <div class="sidebar-header">
                <div class="brand-container">
                    <div class="logo">
                        <img src="@/assets/images/favicon.svg">
                    </div>
                    <div v-if="!isSidebarCollapsed" class="brand-info">
                        <h1 class="brand-name">QuantPulse</h1>
                        <span class="brand-subtitle">{{ adminLayoutI18n.adminPanel }}</span>
                    </div>
                </div>
            </div>

            <!-- Collapse Toggle -->
            <Button class="collapse-btn" :pt="{ root: { class: 'qp-border-0' } }" text rounded size="small" @click="isSidebarCollapsed = !isSidebarCollapsed">
                <i :class="isSidebarCollapsed ? 'ph ph-caret-right' : 'ph ph-caret-left'"></i>
            </Button>

            <!-- Navigation -->
            <div class="sidebar-nav">
                <div class="nav-content">
                    <div v-for="group in navigationItems" :key="group.id" class="nav-group">
                        <div v-if="!isSidebarCollapsed && group.title" class="nav-group-title">{{ group.title }}</div>
                        <ul class="nav-items">
                            <li v-for="item in group.items" :key="item.id" class="nav-item">
                                <router-link :to="item.path" class="nav-link" :class="{ 'active': isActivePath(item.path) }" @click="onNavLinkClick()">
                                    <div class="nav-icon">
                                        <i :class="item.icon"></i>
                                    </div>
                                    <span v-if="!isSidebarCollapsed" class="nav-text">{{ item.title }}</span>
                                </router-link>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </aside>

        <!-- Main Content -->
        <div class="main-wrapper" :class="{ 'expanded': isSidebarCollapsed }">
            <!-- Top Header -->
            <header class="top-header">
                <div class="header-left">
                    <nav class="breadcrumbs">
                        <ol class="breadcrumb-list">
                            <li class="breadcrumb-item">
                                <router-link to="/admin" class="breadcrumb-link">{{ adminLayoutI18n.admin }}</router-link>
                            </li>
                            <li v-for="(crumb, index) in breadcrumbs" :key="index" class="breadcrumb-item">
                                <span class="breadcrumb-separator">/</span>
                                <router-link v-if="crumb.path && index < breadcrumbs.length - 1" :to="crumb.path" class="breadcrumb-link">{{ crumb.title }}</router-link>
                                <span v-else class="breadcrumb-current">{{ crumb.title }}</span>
                            </li>
                        </ol>
                    </nav>

                    <h1 class="page-title">{{ currentPageTitle }}</h1>
                </div>

                <div class="header-right">
                    <div class="search-container">
                        <IconField iconPosition="left">
                            <InputIcon class="ph ph-magnifying-glass"></InputIcon>
                            <InputText class="search-query" size="small" v-model="searchQuery" :placeholder="$tm('common.search')" @focus="isSearchFocused = true" @blue="isSearchFocused = false"></InputText>
                        </IconField>
                        <kbd v-if="!isSearchFocused" class="search-shortcut">Ctrl + K</kbd>
                    </div>

                    <Button class="notification-btn" text size="small" :pt="{ root: { class: 'qp-relative' } }">
                        <i class="ph ph-bell"></i>
                    </Button>

                    <!-- User Profile -->
                    <div class="user-profile-container">
                        <Button class="user-profile-btn" text :pt="{ root: { class: 'qp-p-2' } }" @click="toggleUserMenu">
                            <div class="user-profile-content">
                                <div class="user-avatar">
                                    <img v-if="userProfile.avatar" :src="userProfile.avatar" />
                                    <span v-else class="avatar-initials">{{ userInitials }}</span>
                                </div>
                                <div v-if="!isMobile" class="user-info">
                                    <span class="user-name">{{ userProfile?.full_name || adminLayoutI18n.superAdmin }}</span>
                                    <span class="user-role">{{ userProfile?.is_superuser ? adminLayoutI18n.superAdmin : adminLayoutI18n.admin }}</span>
                                </div>
                                <i class="ph ph-caret-down user-dropdown-icon"></i>
                            </div>
                        </Button>
                        <!-- <Menu ref="userMenu" :model="userMenuItems" :popup="true" class="user-menu-popup">

                        </Menu> -->
                    </div>
                </div>
            </header>

            <!-- Page Content -->
            <main class="page-content">
                <router-view></router-view>
            </main>
        </div>
    </div>
</template>
<script>
import { mapState } from 'pinia'
import { useAuthStore } from '@/stores/auth'
export default {
    data() {
        return {
            adminLayoutI18n: this.$tm('pages.adminLayout'),
            isSidebarCollapsed: false,
            isSearchFocused: false,
            navigationItems: [
                {
                    id: 'overview',
                    items: [
                        { id: 'dashboard', icon: 'ph ph-squares-four', path: '/admin/dashboard' },
                        { id: 'health', icon: 'ph ph-heart', path: '/admin/health' }
                    ]
                },
                {
                    id: 'platform',
                    items: [
                        { id: 'strategyTemplates', icon: 'ph ph-pulse', path: '/admin/strategy' },
                        { id: 'mlModels', icon: 'ph ph-brain', path: '/admin/models' },
                        { id: 'signalAlgorithms', icon: 'ph ph-lightning', path: '/admin/signals' }
                    ]
                },
                {
                    id: 'data',
                    items: [
                        { id: 'securities', icon: 'ph ph-database', path: '/admin/securities' },
                        { id: 'exchanges', icon: 'ph ph-buildings', path: '/admin/exchanges' },
                        { id: 'marketData', icon: 'ph ph-chart-bar', path: '/admin/market' },
                        { id: 'dataFeed', icon: 'ph ph-rss', path: '/admin/data' }
                    ]
                },
                {
                    id: 'analytics',
                    items: [
                        { id: 'usageMetrics', icon: 'ph ph-chart-line-up', path: '/admin/usage' },
                        { id: 'systemPerformance', icon: 'ph ph-trend-up', path: '/admin/syste' },
                        { id: 'resourceUsage', icon: 'ph ph-cpu', path: '/admin/resources' }
                    ]
                },
                {
                    id: 'administration',
                    items: [
                        { id: 'users', icon: 'ph ph-users', path: '/admin/users' },
                        { id: 'roles', icon: 'ph ph-shield-check', path: '/admin/roles' },
                        { id: 'tasks', icon: 'ph ph-list-checks', path: '/admin/tasks' },
                        { id: 'notifications', icon: 'ph ph-bell', path: '/admin/notifications' }
                    ]
                },
                {
                    id: 'system',
                    items: [
                        { id: 'logs', icon: 'ph ph-file-text', path: '/admin/logs' },
                        { id: 'apiMonitoring', icon: 'ph ph-monitor', path: '/admin/api' },
                        { id: 'database', icon: 'ph ph-cylinder', path: '/admin/database' },
                        { id: 'settings', icon: 'ph ph-gear', path: '/admin/settings' }
                    ]
                }
            ],
            searchQuery: null,
            showUserMenu: false,
            isMobile: window.innerWidth < 768
        }
    },
    computed: {
        ...mapState(useAuthStore, ['userProfile']),
        breadcrumbs() {
            const path = this.$route.path
            const segments = path.split('/').filter(Boolean)
            const crumbs = []

            let currentPath = ''
            for (let i = 1; i < segments.length; i++) {
                currentPath += '/' + segments[i]
                const title = this.getPageTitle(currentPath) || this.formatTitle(segments[i])
                crumbs.push({ path: currentPath, title })
            }

            return crumbs
        },

        currentPageTitle() {
            return this.getPageTitle(this.$route.path) || 'Dashboard'
        },

        userInitials() {
            if (!this.userProfile?.full_name) return 'A'
            return this.userProfile.full_name.split(' ').map(name => name.charAt(0)).join('').toUpperCase().slice(0, 2)
        }
    },
    methods: {
        /**
         * Identifies page title from path.
         * @param {String} path
         * @returns {String}
         */
        getPageTitle: function (path) {
            for (const group of this.navigationItems) {
                for (const item in group.items) {
                    if (item.path === path) {
                        return item.title
                    }
                }
            }
            return null
        },

        /**
         * Formats title from path
         * @param {String} path
         * @returns {String}
         */
        formatTitle: function (path) {
            return path.split('-').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ')
        },

        /**
         * Initializes metadata for the component.
         */
        init: function () {
            this.navigationItems = this.$lodash.map(this.navigationItems, (navGroup) => {
                navGroup.items = this.$lodash.map(navGroup.items, (navItem) => ({ ...navItem, title: this.adminLayoutI18n.navItems[navGroup.id].items[navItem.id] }))
                navGroup.title = this.adminLayoutI18n.navItems[navGroup.id].title
                return navGroup
            })
        },

        /**
         * Returns if the specific path is currently active.
         * @param {String} path
         */
        isActivePath: function (path) {
            return this.$route.path.startsWith(path)
        },

        /**
         * Executed on navlink click.
         */
        onNavLinkClick: function () {
            this.isSidebarCollapsed = this.isMobile
        },

        /**
         * Resize Handler
         */
        onResize: function () {
            this.isMobile = window.innerWidth < 768
        },

        /**
         * Toggles user menu.
         * @param {Object} event
         */
        toggleUserMenu: function (event) {
            this.$refs.userMenu.toggle(event)
        }
    },
    mounted() {
        this.init()
        window.addEventListener('resize', this.onResize)
    },
    beforeUnmount() {
        window.removeEventListener('resize', this.onResize)
    }
}
</script>
<style lang="postcss" scoped>
.admin-layout {
    @apply qp-flex qp-h-screen qp-bg-slate-50;
}

/* Sidebar Styles */
.sidebar {
    @apply qp-fixed qp-left-0 qp-top-0 qp-h-screen qp-w-64 qp-bg-white qp-border-r qp-border-slate-200 qp-transition-all qp-duration-300;

    .sidebar-header {
        @apply qp-flex qp-items-center qp-p-4 qp-border-b qp-border-slate-200 qp-relative qp-flex-shrink-0 qp-z-[1];

        .brand-container {
            @apply qp-flex qp-items-center qp-gap-3 qp-min-w-0;

            .logo {
                @apply qp-relative qp-flex-shrink-0 qp-w-12 qp-h-12 qp-rounded-xl qp-flex qp-items-center qp-justify-center qp-p-1 qp-bg-transparent qp-border qp-border-slate-200;

                img {
                    @apply qp-w-12 qp-h-12 qp-relative qp-z-10;
                }
            }

            .brand-info {
                @apply qp-min-w-0 qp-flex qp-flex-col qp-space-y-1;

                .brand-name {
                    @apply qp-text-xl qp-font-bold qp-text-slate-900 qp-leading-tight qp-truncate;
                    background: linear-gradient(135deg, #1e293b, #475569);
                    -webkit-background-clip: text;
                    -webkit-text-fill-color: transparent;
                    background-clip: text;
                }

                .brand-subtitle {
                    @apply qp-text-xs qp-text-blue-600 qp-font-semibold qp-tracking-wide;
                }
            }
        }
    }

    .collapse-btn {
        @apply qp-absolute -qp-right-3 qp-top-1/2 qp-w-6 qp-h-6 qp-bg-white qp-border-2 qp-border-slate-300 qp-rounded-full qp-flex qp-items-center qp-justify-center qp-text-slate-600 hover:qp-text-blue-600 hover:qp-border-blue-500 qp-transition-all qp-duration-200;
        @apply -qp-translate-y-1/2 qp-z-[100];
    }

    /* Navigation */
    .sidebar-nav {
        @apply qp-flex qp-overflow-y-auto qp-overflow-x-hidden;
        scroll-behavior: smooth;
        scrollbar-width: thin;
        scrollbar-color: rgba(148, 163, 184, 0.3) transparent;

        .nav-content {
            @apply qp-p-4 qp-space-y-4 qp-pb-6 qp-w-full;
            min-height: min-content;

            .nav-group {
                @apply qp-mb-6;

                .nav-group-title {
                    @apply qp-text-xs qp-font-semibold qp-text-slate-400 qp-uppercase qp-tracking-wide qp-mb-2 qp-px-1;
                }

                .nav-items {
                    @apply qp-space-y-1;

                    .nav-item {
                        @apply qp-list-none;

                        .nav-link {
                            @apply qp-flex qp-items-center qp-gap-3 qp-px-3 qp-py-2.5 qp-rounded-lg qp-text-slate-600 qp-transition-all qp-duration-200 qp-relative qp-no-underline hover:qp-bg-slate-50 hover:qp-text-slate-900;

                            &.active {
                                @apply qp-bg-blue-50 qp-text-blue-700 qp-font-medium;

                                &::before {
                                    @apply qp-content-[''];
                                    @apply qp-absolute qp-left-0 qp-top-1/2 qp-w-0.5 qp-h-4 qp-bg-blue-600 qp-rounded-r -qp-translate-y-1/2;
                                }
                            }

                            .nav-icon {
                                @apply qp-flex-shrink-0 qp-w-5 qp-h-5 qp-flex qp-items-center qp-justify-center qp-text-base;

                                i {
                                    @apply qp-text-xl;
                                }
                            }

                            .nav-text {
                                @apply qp-flex qp-truncate qp-text-sm;
                            }
                        }
                    }
                }
            }
        }
    }

    &.collapsed {
        @apply qp-w-16;

        .sidebar-header {
            @apply qp-justify-center qp-px-2;

            .brand-container {
                @apply qp-justify-center;

                .logo {
                    @apply qp-w-12 qp-h-12;
                }
            }
        }

        .nav-content {
            @apply qp-px-2 qp-w-full;

            .nav-group {
                @apply qp-mb-4;

                .nav-group-title {
                    @apply qp-hidden;
                }

                .nav-link {
                    @apply qp-justify-center qp-px-1 qp-py-3 qp-rounded-xl;

                    .nav-icon {
                        @apply qp-w-8 qp-h-8 qp-text-lg;
                    }
                }
            }
        }
    }
}

/* Main Content */
.main-wrapper {
    @apply qp-flex qp-flex-col qp-flex-1 qp-ml-64 qp-transition-all qp-duration-300;

    &.expanded {
        @apply qp-ml-16;
    }

    .top-header {
        @apply qp-sticky qp-top-0 qp-z-20 qp-bg-white qp-border-b qp-border-slate-200 qp-px-6 qp-py-3 qp-flex qp-items-center qp-justify-between;

        .header-left {
            @apply qp-flex qp-flex-col qp-gap-1;

            .breadcrumbs {
                @apply qp-mb-1;

                .breadcrumb-list {
                    @apply qp-flex qp-items-center qp-gap-1 qp-text-sm qp-text-slate-500;

                    .breadcrumb-item {
                        @apply qp-flex qp-items-center qp-gap-1;

                        .breadcrumb-link {
                            @apply qp-text-slate-500 hover:qp-text-blue-600 qp-transition-colors qp-no-underline;
                        }

                        .breadcrumb-separator {
                            @apply qp-text-slate-300;
                        }

                        .breadcrumb-current {
                            @apply qp-text-slate-700 qp-font-medium;
                        }
                    }
                }
            }

            .page-title {
                @apply qp-text-xl qp-font-bold qp-text-slate-900;
            }
        }

        .header-right {
            @apply qp-flex qp-items-center qp-gap-4;

            .search-container {
                @apply qp-relative qp-flex qp-items-center;

                .search-input {
                    @apply qp-w-80 qp-pr-12;
                }

                .search-shortcut {
                    @apply qp-absolute qp-right-3 qp-px-2 qp-py-1 qp-text-xs qp-bg-slate-100 qp-text-slate-500 qp-rounded qp-font-mono qp-pointer-events-none;
                }
            }

            .notification-btn {
                @apply qp-relative;

                .notification-badge {
                    @apply qp-absolute -qp-top-1 -qp-right-1;
                }
            }

            .user-profile-container {
                @apply qp-relative;

                .user-profile-btn {
                    @apply qp-bg-slate-50 qp-border qp-border-slate-200 qp-rounded-lg hover:qp-bg-slate-100 qp-transition-all qp-duration-200;

                    .user-profile-content {
                        @apply qp-flex qp-items-center qp-gap-3;

                        .user-avatar {
                            @apply qp-w-8 qp-h-8 qp-bg-blue-600 qp-rounded-lg qp-flex qp-items-center qp-justify-center qp-overflow-hidden;

                            img {
                                @apply qp-w-full qp-h-full qp-object-cover;
                            }

                            .avatar-initials {
                                @apply qp-text-white qp-text-sm qp-font-semibold;
                            }
                        }

                        .user-info {
                            @apply qp-flex qp-flex-col qp-text-left;

                            .user-name {
                                @apply qp-text-sm qp-font-semibold qp-text-slate-900;
                            }

                            .user-role {
                                @apply qp-text-xs qp-text-slate-500;
                            }

                            .user-dropdown-icon {
                                @apply qp-text-slate-400 qp-text-sm;
                            }
                        }
                    }
                }
            }
        }
    }

    .page-content {
        @apply qp-flex qp-p-6 qp-overflow-y-auto qp-bg-slate-50 qp-h-full;
        background-image: radial-gradient(circle at 1px 1px, rgba(71, 85, 105, 0.15) 1px, transparent 0);
        background-size: 20px 20px;
    }
}

/* Mobile Responsive */
@media (max-width: 768px) {
    .sidebar {
        @apply qp-transform -qp-translate-x-full;
    }

    .sidebar:not(.collapsed) {
        @apply qp-translate-x-0;
    }

    .main-wrapper {
        @apply qp-ml-0;
    }

    .search-input {
        @apply qp-w-48;
    }

    .user-info {
        @apply qp-hidden;
    }
}
</style>