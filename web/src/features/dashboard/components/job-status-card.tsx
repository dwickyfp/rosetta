import { useQuery } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { jobMetricsRepo, JobMetric } from '@/repo/job-metrics'
import { formatDistanceToNow } from 'date-fns'
import { Activity } from 'lucide-react'

export function JobStatusCard() {
  const { data: metrics, isLoading } = useQuery({
    queryKey: ['job-metrics'],
    queryFn: jobMetricsRepo.getAll,
    refetchInterval: 30000, // Refresh every 30s
  })

  // Define expected intervals for health check (in seconds)
  const jobThresholds: Record<string, number> = {
    'wal_monitor': 60,
    'replication_monitor': 60,
    'schema_monitor': 60,
    'system_metric_collection': 10, 
    'table_list_refresh': 600,
    'credit_monitor': 21600, // 6 hours
  }

  const getStatus = (key: string, lastRun: string) => {
    const threshold = jobThresholds[key] || 60
    const diffSeconds = (new Date().getTime() - new Date(lastRun).getTime()) / 1000
    
    // Allow 2x threshold + buffer before warning
    if (diffSeconds < threshold * 3) return 'healthy'
    return 'delayed'
  }

  const getJobDisplayName = (key: string) => {
      // Convert snake_case to Title Case
      return key.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ')
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium">Job Status</CardTitle>
        <Activity className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="text-sm text-muted-foreground">Loading...</div>
        ) : !metrics || metrics.length === 0 ? (
          <div className="text-sm text-muted-foreground">No job history found.</div>
        ) : (
          <div className="space-y-4">
            {metrics.map((metric: JobMetric) => {
              const status = getStatus(metric.key_job_scheduler, metric.last_run_at)
              return (
                <div key={metric.key_job_scheduler} className="flex items-center justify-between">
                  <div className="flex flex-col">
                    <span className="text-sm font-medium">{getJobDisplayName(metric.key_job_scheduler)}</span>
                    <span className="text-xs text-muted-foreground">
                      Last run: {formatDistanceToNow(new Date(metric.last_run_at), { addSuffix: true })}
                    </span>
                  </div>
                  <Badge variant={status === 'healthy' ? 'default' : 'destructive'}>
                    {status === 'healthy' ? 'Active' : 'Delayed'}
                  </Badge>
                </div>
              )
            })}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
