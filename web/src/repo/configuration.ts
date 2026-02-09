import { api } from './client'

export interface WALThresholdsConfig {
    warning: number  // in MB
    error: number    // in MB
    webhook_url: string
    notification_iteration: number
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
}
