import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { WALMonitorResponse } from '@/repo/sources'
import { Activity, Database, Clock, AlertCircle, CheckCircle2 } from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'

interface SourceDetailsMetricsProps {
    data: WALMonitorResponse | null
    dataDestinations: string[]
}

function formatBytes(bytes: number, decimals = 2) {
    if (!+bytes) return '0 Bytes'
    const k = 1024
    const dm = decimals < 0 ? 0 : decimals
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`
}

export function SourceDetailsMetrics({ data }: SourceDetailsMetricsProps) {
    if (!data) {
        return (
            <Card className="bg-muted/50">
                <CardHeader>
                    <CardTitle className="text-sm font-medium text-muted-foreground">Metrics</CardTitle>
                </CardHeader>
                <CardContent>
                    <div className='text-sm text-muted-foreground'>No metrics available.</div>
                </CardContent>
            </Card>
        )
    }

    const isHealthy = data.status === 'active' || data.status === 'streaming'
    const lagBytes = data.replication_lag_bytes || 0

    return (
        <div className='grid gap-4 md:grid-cols-2 lg:grid-cols-4'>
            <Card>
                <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                    <CardTitle className='text-sm font-medium'>Health Status</CardTitle>
                    {isHealthy ? (
                        <CheckCircle2 className='h-4 w-4 text-green-500' />
                    ) : (
                        <AlertCircle className='h-4 w-4 text-red-500' />
                    )}
                </CardHeader>
                <CardContent>
                    <div className='text-2xl font-bold uppercase'>{data.status}</div>
                    <p className='text-xs text-muted-foreground mt-1'>
                        {data.error_message || 'System operational'}
                    </p>
                </CardContent>
            </Card>

            <Card>
                <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                    <CardTitle className='text-sm font-medium'>Replication Lag</CardTitle>
                    <Activity className='h-4 w-4 text-muted-foreground' />
                </CardHeader>
                <CardContent>
                    <div className='text-2xl font-bold'>{formatBytes(lagBytes)}</div>
                    <p className='text-xs text-muted-foreground mt-1'>
                        current lag
                    </p>
                </CardContent>
            </Card>

            <Card>
                <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                    <CardTitle className='text-sm font-medium'>Total WAL Size</CardTitle>
                    <Database className='h-4 w-4 text-muted-foreground' />
                </CardHeader>
                <CardContent>
                    <div className='text-2xl font-bold'>
                        {data.total_wal_size || 'N/A'}
                    </div>
                    <p className='text-xs text-muted-foreground mt-1'>
                        processed size
                    </p>
                </CardContent>
            </Card>

            <Card>
                <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                    <CardTitle className='text-sm font-medium'>Last Received</CardTitle>
                    <Clock className='h-4 w-4 text-muted-foreground' />
                </CardHeader>
                <CardContent>
                    <div className='text-2xl font-bold truncate text-lg pt-1'>
                        {data.last_wal_received
                            ? formatDistanceToNow(new Date(data.last_wal_received), { addSuffix: true })
                            : 'Never'}
                    </div>
                    <p className='text-xs text-muted-foreground mt-1 truncate'>
                        {data.last_wal_received ? new Date(data.last_wal_received).toLocaleString() : '-'}
                    </p>
                </CardContent>
            </Card>
        </div>
    )
}
