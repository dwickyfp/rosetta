import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import { GlassCard, GlassCardContent, GlassCardHeader, GlassCardTitle } from './glass-card'
import { BellRing, AlertCircle, Info, CheckCircle2 } from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'

export function ActivityFeed() {
  const { data: activities } = useQuery({
    queryKey: ['dashboard', 'activity-feed'],
    queryFn: () => dashboardRepo.getActivityFeed(20),
    refetchInterval: 10000,
  })

  // Helper to get icon based on type or message content
  const getIcon = (type: string, message: string) => {
    if (type === 'ERROR' || message.toLowerCase().includes('error')) {
      return <AlertCircle className="w-4 h-4 text-rose-500" />
    }
    if (message.includes('RUNNING') || message.includes('START')) {
      return <CheckCircle2 className="w-4 h-4 text-emerald-500" />
    }
    return <Info className="w-4 h-4 text-blue-500" />
  }

  // Helper to get relative time (simple version, could use date-fns)
  const getTimeAgo = (timestamp: string) => {
    const diff = new Date().getTime() - new Date(timestamp).getTime()
    const minutes = Math.floor(diff / 60000)
    if (minutes < 1) return 'Just now'
    if (minutes < 60) return `${minutes}m ago`
    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours}h ago`
    return new Date(timestamp).toLocaleDateString()
  }

  return (
    <GlassCard className="col-span-3 lg:col-span-2 h-[400px] flex flex-col">
      <GlassCardHeader className="flex-row items-center justify-between space-y-0 pb-4">
        <GlassCardTitle className="text-sm font-medium">Activity Feed</GlassCardTitle>
        <BellRing className="w-4 h-4 text-muted-foreground" />
      </GlassCardHeader>
      <GlassCardContent className="flex-1 min-h-0 p-0">
        <ScrollArea className="h-full px-6">
            <div className="space-y-4 pb-6">
                {activities?.length === 0 && (
                    <div className="text-sm text-muted-foreground text-center py-4">No recent activity</div>
                )}
                {activities?.map((item, i) => (
                    <div key={i} className="flex gap-4 items-start group">
                        <div className={cn(
                            "mt-1 p-1.5 rounded-full bg-muted/50 transition-colors",
                            item.type === 'ERROR' ? "group-hover:bg-rose-500/10" : "group-hover:bg-emerald-500/10"
                        )}>
                            {getIcon(item.type, item.message)}
                        </div>
                        <div className="space-y-1">
                            <p className="text-sm font-medium leading-none">{item.source}</p>
                            <p className="text-xs text-muted-foreground break-all">{item.message}</p>
                            <p className="text-[10px] text-muted-foreground/60">{getTimeAgo(item.timestamp)}</p>
                        </div>
                    </div>
                ))}
            </div>
        </ScrollArea>
      </GlassCardContent>
    </GlassCard>
  )
}
