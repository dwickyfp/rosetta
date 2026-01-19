import { AxiosResponse } from 'axios'
import { api } from './client'

export interface Pipeline {
  id: number
  name: string
  source_id: number
  destination_id: number
  status: 'START' | 'PAUSE' | 'REFRESH'
  pipeline_metadata?: {
    status: 'RUNNING' | 'PAUSED' | 'ERROR'
    last_error?: string
    last_start_at?: string
  }
  pipeline_progress?: {
    progress: number
    step?: string
    status: 'PENDING' | 'IN_PROGRESS' | 'COMPLETED' | 'FAILED'
    details?: string
  }
  source?: {
    name: string
  }
  destination?: {
    name: string
  }
}

export interface CreatePipelineRequest {
  name: string
  source_id: number
  destination_id: number
  status?: string
}

export interface PipelineListResponse {
  pipelines: Pipeline[]
  total: number
}

export const pipelinesRepo = {
  getAll: async (): Promise<PipelineListResponse> => {
    const response: AxiosResponse<Pipeline[]> = await api.get('/pipelines')
    return {
      pipelines: response.data,
      total: response.data.length
    }
  },
  create: async (data: CreatePipelineRequest): Promise<Pipeline> => {
    const response: AxiosResponse<Pipeline> = await api.post('/pipelines', data)
    return response.data
  },
  delete: async (id: number): Promise<void> => {
    await api.delete(`/pipelines/${id}`)
  }
}
