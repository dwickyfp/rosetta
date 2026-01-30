import { useQuery } from "@tanstack/react-query"
import { GlassCard, GlassCardContent, GlassCardHeader, GlassCardTitle } from "./glass-card"
import { Activity, Database, Server } from "lucide-react"
import { api } from "@/repo/client"

interface HealthResponse {
  status: string
  version: string
  timestamp: string
  checks: {
    database: boolean
    redis: boolean
    wal_monitor: boolean
  }
}

const getSystemHealth = async () => {
  const { data } = await api.get<HealthResponse>("/health")
  return data
}

export function SystemHealthWidget() {
  const { data, isLoading, isError } = useQuery({
    queryKey: ["system-health"],
    queryFn: getSystemHealth,
    refetchInterval: 30000, // Check every 30s
  })

  const StatusIndicator = ({ healthy }: { healthy?: boolean }) => (
    <div className={`h-3 w-3 rounded-full ${healthy ? "bg-emerald-500 box-shadow-emerald" : "bg-rose-500 box-shadow-rose"} ring-2 ring-white/10`} />
  )

  return (
    <GlassCard>
      <GlassCardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <GlassCardTitle className="text-sm font-medium">System Status</GlassCardTitle>
        <Activity className="h-4 w-4 text-muted-foreground" />
      </GlassCardHeader>
      <GlassCardContent>
        {isLoading ? (
          <div className="text-sm text-muted-foreground pt-4">Loading status...</div>
        ) : isError || !data ? (
          <div className="text-sm text-rose-500 pt-4">Failed to fetch status</div>
        ) : (
          <div className="space-y-4 pt-4">
            <div className="flex items-center justify-between p-2 rounded-lg hover:bg-white/5 transition-colors">
              <div className="flex items-center space-x-2">
                <Database className="h-4 w-4 text-muted-foreground" />
                <span className="text-sm font-medium">Postgres Core</span>
              </div>
              <StatusIndicator healthy={data.checks.database} />
            </div>
            
            <div className="flex items-center justify-between p-2 rounded-lg hover:bg-white/5 transition-colors">
              <div className="flex items-center space-x-2">
                <Server className="h-4 w-4 text-muted-foreground" />
                <span className="text-sm font-medium">Redis Cache</span>
              </div>
              <StatusIndicator healthy={data.checks.redis} />
            </div>

            <div className="flex items-center justify-between pt-2 px-2 border-t border-white/5">
               <div className="flex items-center space-x-2">
                 <span className="text-xs text-muted-foreground font-mono">v{data.version}</span>
               </div>
               <span className="text-xs text-muted-foreground">{new Date(data.timestamp).toLocaleTimeString()}</span>
            </div>
          </div>
        )}
      </GlassCardContent>
    </GlassCard>
  )
}
