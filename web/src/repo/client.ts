import axios from 'axios'

// Use VITE_API_URL if set, otherwise:
// - Development: use localhost:8000/api/v1 directly
// - Production: use relative path '/api' which nginx will proxy to backend
const defaultApiUrl = import.meta.env.MODE === 'development' 
    ? 'http://localhost:8000/api/v1' 
    : '/api'
const baseURL = import.meta.env.VITE_API_URL || defaultApiUrl

export const api = axios.create({
    baseURL,
    headers: {
        'Content-Type': 'application/json',
    },
})

// Add response interceptor for error handling if needed
api.interceptors.response.use(
    (response) => response,
    (error) => {
        // You can handle global errors here, e.g., 401 Unauthorized
        return Promise.reject(error)
    }
)
