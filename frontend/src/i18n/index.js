import { createI18n } from "vue-i18n";
import { en } from "./locales/english";

const i18n = createI18n({
    warnHtmlInMessage: 'off',
    warnHtmlMessage: false,
    locale: 'en',
    legacy: false,
    globalInjection: true,
    messages: { en: en }
})

export { i18n }