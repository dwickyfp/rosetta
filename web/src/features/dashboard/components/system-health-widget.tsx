import { useQuery } from "@tanstack/react-query"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
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
    <div className={`h-3 w-3 rounded-full ${healthy ? "bg-green-500" : "bg-red-500"} ring-2 ring-white dark:ring-slate-950`} />
  )

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium">System Status</CardTitle>
        <Activity className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="text-sm text-muted-foreground">Loading status...</div>
        ) : isError || !data ? (
          <div className="text-sm text-red-500">Failed to fetch status</div>
        ) : (
          <div className="space-y-4 pt-2">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <Database className="h-4 w-4 text-muted-foreground" />
                <span className="text-sm font-medium">Postgres Core</span>
              </div>
              <StatusIndicator healthy={data.checks.database} />
            </div>
            
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <Server className="h-4 w-4 text-muted-foreground" />
                <span className="text-sm font-medium">Redis Cache</span>
              </div>
              <StatusIndicator healthy={data.checks.redis} />
            </div>

            <div className="flex items-center justify-between">
               <div className="flex items-center space-x-2">
                 <div className="h-4 w-4" /> {/* Spacer */}
                 <span className="text-xs text-muted-foreground">Version: {data.version}</span>
               </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
