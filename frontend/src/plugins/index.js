import _ from 'lodash'
import Axios from 'axios'
import queryString from 'query-string'

const axiosInstance = Axios.create({
    baseURL: import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1',
    withCredentials: true,
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
    paramsSerializer: (params) => queryString.stringify(params, { arrayFormat: 'repeat' })

})

axiosInstance.interceptors.request.use((config) => {
    const token = localStorage.getItem('access_token')

    if (token) {
        config.headers.Authorization = `Bearer ${token}`
    }
    return config
}, (error) => {
    return Promise.reject(error)
})

axiosInstance.interceptors.response.use((response) => response, (error) => {
    if (error.response?.status === 401) {
        localStorage.removeItem('access_token')
        localStorage.removeItem('user')

        window.location.href = "/auth/login"
    }
    return Promise.reject(error)
})

export const $lodash = _
export const $http = axiosInstance

export default {
    install(app) {
        app.config.globalProperties.$http = axiosInstance
        app.config.globalProperties.$lodash = _
    }
}