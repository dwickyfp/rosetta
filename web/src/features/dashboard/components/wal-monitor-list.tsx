import { useQuery } from '@tanstack/react-query'
import { walMonitorRepo } from '@/repo/wal-monitor'
import { formatBytes } from '@/lib/utils'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { Badge } from '@/components/ui/badge'

export function WALMonitorList() {
    const { data } = useQuery({
        queryKey: ['wal-monitor', 'all'],
        queryFn: walMonitorRepo.getAll,
        refetchInterval: 5000,
    })

    const monitors = data?.monitors || []

    if (monitors.length === 0) {
        return <div className='text-sm text-muted-foreground'>No active WAL monitors found.</div>
    }

    return (
        <div className='space-y-8'>
            {monitors.map((monitor) => (
                <div key={monitor.id} className='flex items-center gap-4'>
                    <Avatar className='h-9 w-9'>
                        <AvatarFallback>{monitor.source_id}</AvatarFallback>
                    </Avatar>
                    <div className='flex flex-1 flex-wrap items-center justify-between'>
                        <div className='space-y-1'>
                            <div className='font-medium'>
                                {monitor.source?.name || `Source #${monitor.source_id}`}
                            </div>
                            <p className='text-sm text-muted-foreground'>
                                LSN: {monitor.wal_lsn || 'N/A'}
                            </p>
                        </div>
                        <div className='flex flex-col items-end gap-1'>
                            <Badge variant={monitor.status === 'ACTIVE' ? 'default' : 'destructive'}>
                                {monitor.status}
                            </Badge>
                            <span className='text-xs text-muted-foreground'>
                                Lag: {formatBytes(monitor.replication_lag_bytes || 0)}
                            </span>
                            <span className='text-xs text-muted-foreground'>
                                WAL Size: {monitor.total_wal_size || 'N/A'}
                            </span>
                        </div>
                    </div>
                </div>
            ))}
        </div>
    )
}
