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
    created_at: string
    updated_at: string
}

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
}
