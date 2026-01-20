import { api } from './client'

export interface DashboardSummary {
  pipelines: {
    [status: string]: number
    total: number
  }
  data_flow: {
    today: number
    yesterday: number
  }
  credits: {
    month_total: number
  }
}

export interface FlowChartData {
  total_today: number
  total_yesterday: number
  history: {
    date: string
    [key: string]: string | number // Allow dynamic pipeline keys
  }[]
  pipelines: string[]
}

export interface CreditChartData {
  current_month_total: number
  history: {
    date: string
    [key: string]: string | number
  }[]
  destinations: string[]
}

export const dashboardRepo = {
  getSummary: async (): Promise<DashboardSummary> => {
    const response = await api.get<DashboardSummary>('/dashboard/summary')
    return response.data
  },

  getFlowChart: async (days: number = 7): Promise<FlowChartData> => {
    const response = await api.get<FlowChartData>(`/dashboard/flow-chart?days=${days}`)
    return response.data
  },

  getCreditChart: async (days: number = 30): Promise<CreditChartData> => {
    const response = await api.get<CreditChartData>(`/dashboard/credit-chart?days=${days}`)
    return response.data
  },
}
