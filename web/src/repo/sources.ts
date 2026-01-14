import { api } from './client'

export interface Source {
    id: number
    name: string
    pg_host: string
    pg_port: number
    pg_database: string
    pg_username: string
    publication_name: string
    replication_id: number
    is_publication_enabled: boolean
    is_replication_enabled: boolean
    last_check_replication_publication: string | null
    total_tables: number
    created_at: string
    updated_at: string
}

export interface SourceCreate {
    name: string
    pg_host: string
    pg_port: number
    pg_database: string
    pg_username: string
    pg_password?: string
    publication_name: string
    replication_id: number
}

export interface SourceUpdate extends Partial<SourceCreate> { }

export interface SourceListResponse {
    sources: Source[]
    total: number
}

export const sourcesRepo = {
    getAll: async () => {
        const { data } = await api.get<Source[]>('/sources')
        return {
            sources: data,
            total: data.length
        }
    },
    create: async (source: SourceCreate) => {
        const { data } = await api.post<Source>('/sources', source)
        return data
    },
    update: async (id: number, source: SourceUpdate) => {
        const { data } = await api.put<Source>(`/sources/${id}`, source)
        return data
    },
    delete: async (id: number) => {
        await api.delete(`/sources/${id}`)
    },
    testConnection: async (config: SourceCreate) => {
        // We use SourceCreate as it contains all necessary fields for connection test
        // Ensure required fields for test are present if using a partial type elsewhere
        const { data } = await api.post<boolean>('/sources/test_connection', config)
        return data
    }
}
