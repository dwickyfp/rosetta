import axios from 'axios'

// API URL configuration:
// 1. If VITE_API_URL is set explicitly, use it
// 2. In development mode (npm run dev), use localhost:8000/api/v1
// 3. In production mode (npm run build), use relative /api path
const getBaseUrl = (): string => {
    // Explicit env var takes priority
    if (import.meta.env.VITE_API_URL) {
        return import.meta.env.VITE_API_URL
    }
    // Development uses localhost
    if (import.meta.env.DEV) {
        return 'http://localhost:8000/api/v1'
    }
    // Production uses relative path (nginx will proxy)
    return '/api'
}

const baseURL = getBaseUrl()
console.log('[API Client] Base URL:', baseURL, '| Mode:', import.meta.env.MODE)

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
