import { api } from './client'

export interface Pipeline {
    id: number
    name: string
    source_id: number
    destination_id: number
    status: string
    created_at: string
    updated_at: string
}

export interface PipelineListResponse {
    pipelines: Pipeline[]
    total: number
}

export const pipelinesRepo = {
    getAll: async () => {
        const { data } = await api.get<Pipeline[]>('/pipelines')
        return {
            pipelines: data,
            total: data.length
        }
    },
}
