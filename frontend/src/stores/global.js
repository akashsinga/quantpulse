// frontend/src/stores/global.js
import { defineStore } from "pinia";

const state = () => ({

})

const actions = {
    /**
     * Formats date to readable format
     * @param {String} date
     * @returns {String}
     */
    getFormattedDate: function (date) {
        if (!date) return 'N/A'
        return new Date(date).toLocaleDateString('en-IN', { year: 'numeric', month: 'short', day: 'numeric' })
    },

    /**
     * Formats number to readable format.
     * @param {Number} number
     * @returns {*}
     */
    getFormattedNumber: function (number) {
        if (!number && number !== 0) return 'N/A';
        return new Intl.NumberFormat('en-IN').format(number)
    },

    /**
     * Formats datetime to readable format
     * @param {String} date
     * @returns {String}
     */
    getFormattedDateTime: function (date) {
        if (!date) return 'N/A'
        return new Date(date).toLocaleString('en-IN', { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })
    },

    /**
     * Formats seconds to readable duration.
     * @param {Number} seconds
     * @returns {String}
     */
    getFormattedDuration: function (seconds) {
        if (!seconds) return 'N/A'
        if (seconds < 60) return `${seconds}s`
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`
        const hours = Math.floor(seconds / 3600)
        const minutes = Math.floor((seconds % 3600) / 60)
        return `${hours}h ${minutes}m`
    },

    /**
     * Returns Elapsed time in readable format.
     * @param {String} datetime
     * @returns {String}
     */
    getElapsedTime: function (datetime) {
        if (!datetime) return ''
        const now = new Date();
        const date = new Date(datetime);
        const diffInHours = Math.floor((now - date) / (1000 * 60 * 60));

        if (diffInHours < 1) return 'Just now';
        if (diffInHours < 24) return `${diffInHours} hours ago`;
        if (diffInHours < 168) return `${Math.floor(diffInHours / 24)} days ago`;
        return `${Math.floor(diffInHours / 168)} weeks ago`;
    }
}

const getters = {

}

export const useGlobalStore = defineStore('global', { state, actions, getters })