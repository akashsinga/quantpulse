import _ from 'lodash'
import Axios from 'axios'

const axiosInstance = Axios.create({
    baseURL: import.meta.env.BASE_URL || window.location.origin,
    headers: {
        'Content-Type': 'application/json'
    }
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
    install(app: any) {
        app.config.globalProperties.$http = axiosInstance
        app.config.globalProperties.$lodash = _
    }
}