import { api } from './client'

export interface Destination {
    id: number
    name: string
    snowflake_account: string | null
    snowflake_user: string | null
    snowflake_database: string | null
    snowflake_schema: string | null
    snowflake_landing_database: string | null
    snowflake_landing_schema: string | null
    snowflake_role: string | null
    snowflake_private_key: string | null
    snowflake_private_key_passphrase?: string | null // Optional in response, handled securely
    snowflake_warehouse: string | null
    created_at: string
    updated_at: string
}

export interface DestinationCreate {
    name: string
    snowflake_account?: string
    snowflake_user?: string
    snowflake_database?: string
    snowflake_schema?: string
    snowflake_landing_database?: string
    snowflake_landing_schema?: string
    snowflake_role?: string
    snowflake_private_key?: string
    snowflake_private_key_passphrase?: string
    snowflake_warehouse?: string
}

export interface DestinationUpdate extends Partial<DestinationCreate> { }

export interface DestinationListResponse {
    destinations: Destination[]
    total: number
}

export const destinationsRepo = {
    getAll: async () => {
        // Assuming the list endpoint returns a list directly based on current backend patterns, 
        // but often it might be { data: [], total: ... } or just [].
        // Looking at backend sources endpoint it returned List[SourceResponse].
        // Checking sourcesRepo: it maps data to { sources: data, total: data.length }.
        const { data } = await api.get<Destination[]>('/destinations')
        return {
            destinations: data,
            total: data.length
        }
    },
    create: async (destination: DestinationCreate) => {
        const { data } = await api.post<Destination>('/destinations', destination)
        return data
    },
    update: async (id: number, destination: DestinationUpdate) => {
        const { data } = await api.put<Destination>(`/destinations/${id}`, destination)
        return data
    },
    delete: async (id: number) => {
        await api.delete(`/destinations/${id}`)
    },
    get: async (id: number) => {
        const { data } = await api.get<Destination>(`/destinations/${id}`)
        return data
    },
    testConnection: async (destination: DestinationCreate) => {
        const { data } = await api.post<{ message: string, error?: boolean }>('/destinations/test-connection', destination)
        return data
    }
}
