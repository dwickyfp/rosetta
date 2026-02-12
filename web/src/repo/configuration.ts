import { api } from './client'

export interface WALThresholdsConfig {
    warning: number  // in MB
    error: number    // in MB
    webhook_url: string
    notification_iteration: number
}

export interface BatchConfiguration {
    max_batch_size: number
    max_queue_size: number
}

export const configurationRepo = {
    getWALThresholds: async () => {
        const { data } = await api.get<WALThresholdsConfig>('/configuration/wal-thresholds')
        return data
    },

    updateWALThresholds: async (config: WALThresholdsConfig) => {
        const { data } = await api.put<WALThresholdsConfig>('/configuration/wal-thresholds', config)
        return data
    },

    testNotification: async (webhook_url?: string) => {
        const { data } = await api.post('/configuration/wal-thresholds/test', { webhook_url })
        return data
    },

    getBatchConfiguration: async () => {
        const { data } = await api.get<BatchConfiguration>('/configuration/batch')
        return data
    },

    updateBatchConfiguration: async (config: BatchConfiguration) => {
        const { data } = await api.put<BatchConfiguration>('/configuration/batch', config)
        return data
    },
}
