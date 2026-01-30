import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import { GlassCard, GlassCardContent, GlassCardHeader, GlassCardTitle } from './glass-card'
import { Database, CheckCircle, AlertOctagon, PauseCircle } from 'lucide-react'

export function SourceHealthCard() {
  const { data: health } = useQuery({
    queryKey: ['dashboard', 'source-health'],
    queryFn: dashboardRepo.getSourceHealth,
    refetchInterval: 10000,
  })

  return (
    <GlassCard className="h-full">
      <GlassCardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <GlassCardTitle className="text-sm font-medium">
          Source Health
        </GlassCardTitle>
        <Database className="h-4 w-4 text-muted-foreground" />
      </GlassCardHeader>
      <GlassCardContent>
        <div className="flex items-center justify-between mt-4">
          <div className="flex flex-col items-center">
             <div className="p-2 mb-2 rounded-full bg-emerald-500/10 text-emerald-500">
                <CheckCircle className="w-5 h-5" />
             </div>
             <span className="text-2xl font-bold">{health?.ACTIVE || 0}</span>
             <span className="text-xs text-muted-foreground">Active</span>
          </div>
          <div className="w-px h-12 bg-border/50" />
          <div className="flex flex-col items-center">
             <div className="p-2 mb-2 rounded-full bg-amber-500/10 text-amber-500">
                <PauseCircle className="w-5 h-5" />
             </div>
             <span className="text-2xl font-bold">{health?.IDLE || 0}</span>
             <span className="text-xs text-muted-foreground">Idle</span>
          </div>
          <div className="w-px h-12 bg-border/50" />
          <div className="flex flex-col items-center">
             <div className="p-2 mb-2 rounded-full bg-rose-500/10 text-rose-500">
                <AlertOctagon className="w-5 h-5" />
             </div>
             <span className="text-2xl font-bold">{health?.ERROR || 0}</span>
             <span className="text-xs text-muted-foreground">Error</span>
          </div>
        </div>
      </GlassCardContent>
    </GlassCard>
  )
}
