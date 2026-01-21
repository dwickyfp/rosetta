import { useQuery } from '@tanstack/react-query'
import { walMonitorRepo } from '@/repo/wal-monitor'
import { formatBytes } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Database, AlertTriangle, CheckCircle2 } from 'lucide-react'

export function WALMonitorList() {
    const { data } = useQuery({
        queryKey: ['wal-monitor', 'all'],
        queryFn: walMonitorRepo.getAll,
        refetchInterval: 5000,
    })

    const monitors = data?.monitors || []

    if (monitors.length === 0) {
        return (
            <div className='flex h-[100px] items-center justify-center rounded-md border border-dashed text-sm text-muted-foreground'>
                No active WAL monitors found.
            </div>
        )
    }

    // Helper to get badge variant based on threshold status
    const getWalSizeBadgeVariant = (status: 'OK' | 'WARNING' | 'ERROR' | null) => {
        switch (status) {
            case 'OK':
                return 'default' // Green
            case 'WARNING':
                return 'secondary' // Yellow-ish
            case 'ERROR':
                return 'destructive' // Red
            default:
                return 'outline'
        }
    }

     const getStatusIcon = (status: string) => {
        if (status === 'ACTIVE') return <CheckCircle2 className="h-4 w-4 text-green-500" />
        return <AlertTriangle className="h-4 w-4 text-red-500" />
    }

    return (
        <ScrollArea className='h-[300px] pr-4'>
            <div className='space-y-4'>
                {monitors.map((monitor) => (
                    <div
                        key={monitor.id}
                        className='flex items-center justify-between rounded-lg border p-3 hover:bg-muted/50 transition-colors'
                    >
                        <div className='flex items-center gap-3'>
                            <div className='flex h-9 w-9 items-center justify-center rounded-full bg-primary/10'>
                                <Database className='h-4 w-4 text-primary' />
                            </div>
                            <div>
                                <div className='flex items-center gap-2'>
                                    <span className='font-medium text-sm'>
                                        {monitor.source?.name || `Source #${monitor.source_id}`}
                                    </span>
                                    {getStatusIcon(monitor.status || 'UNKNOWN')}
                                </div>
                                <div className='flex items-center gap-2 text-xs text-muted-foreground'>
                                    <span className="font-mono">{monitor.wal_lsn || 'No LSN'}</span>
                                </div>
                            </div>
                        </div>

                        <div className='flex items-center gap-3 text-right'>
                             <div className='flex flex-col items-end'>
                                <span className='text-xs font-medium text-muted-foreground mb-1'>Lag</span>
                                <Badge variant="outline" className="font-mono text-xs">
                                     {formatBytes(monitor.replication_lag_bytes || 0)}
                                </Badge>
                             </div>
                             
                             <div className='flex flex-col items-end'>
                                <span className='text-xs font-medium text-muted-foreground mb-1'>Size</span>
                                <Badge 
                                    variant={getWalSizeBadgeVariant(monitor.wal_threshold_status)}
                                    className={
                                        monitor.wal_threshold_status === 'OK' 
                                            ? 'bg-green-500/15 text-green-700 hover:bg-green-500/25 border-green-500/20' 
                                            : monitor.wal_threshold_status === 'WARNING'
                                            ? 'bg-yellow-500/15 text-yellow-700 hover:bg-yellow-500/25 border-yellow-500/20'
                                            : monitor.wal_threshold_status === 'ERROR'
                                            ? 'bg-red-500/15 text-red-700 hover:bg-red-500/25 border-red-500/20'
                                            : ''
                                    }
                                >
                                    {monitor.total_wal_size || '0 B'}
                                </Badge>
                             </div>
                        </div>
                    </div>
                ))}
            </div>
        </ScrollArea>
    )
}
