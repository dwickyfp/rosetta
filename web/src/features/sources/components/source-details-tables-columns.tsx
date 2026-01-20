import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type SourceTableInfo } from '@/repo/sources'
import { Button } from '@/components/ui/button'
import { type PipelineStats } from '@/repo/pipelines'
import { LineChart, Line, ResponsiveContainer } from 'recharts'
import { useMemo } from 'react'

export const getSourceDetailsTablesColumns = (
    onUnregister: ((tableName: string) => void) | undefined,
    statsMap: Record<string, PipelineStats> = {}
): ColumnDef<SourceTableInfo>[] => {

    const columns: ColumnDef<SourceTableInfo>[] = [

        {
            accessorKey: 'table_name',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Table Name' />
            ),
            cell: ({ row }) => (
                <span className='font-medium'>{row.getValue('table_name')}</span>
            ),
            enableSorting: true,
            enableHiding: false,
        },
        {
            id: 'message_per_day',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Message Per Day' className='justify-end' />
            ),
            cell: ({ row }) => {
                const tableName = row.getValue('table_name') as string
                const stats = statsMap[tableName]
                // Get today's count (last entry in daily_stats usually, or check date)
                const today = new Date().toISOString().split('T')[0]
                const todayStat = stats?.daily_stats.find(d => d.date.startsWith(today))
                const count = todayStat ? todayStat.count : 0
                
                return (
                   <div className="text-right font-medium">
                       {count.toLocaleString()}
                   </div>
                )
            }
        },
        {
            id: 'monitoring',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Monitoring (5m)' className='w-[150px]' />
            ),
            cell: ({ row }) => {
                const tableName = row.getValue('table_name') as string
                const stats = statsMap[tableName]
                
                // Prepare data for sparkline
                // recent_stats has timestamp and count. 
                // We want to show a flow. 
                // If data is empty, it should look flat or empty.
                const data = useMemo(() => {
                    if (!stats || !stats.recent_stats || stats.recent_stats.length === 0) {
                        return Array(10).fill(0).map((_, i) => ({ count: 0, i }))
                    }
                    return stats.recent_stats.map(s => ({ count: s.count, timestamp: s.timestamp }))
                }, [stats])

                return (
                    <div className="h-[40px] w-[150px]">
                        <ResponsiveContainer width="100%" height="100%">
                            <LineChart data={data}>
                                <Line 
                                    type="monotone" 
                                    dataKey="count" 
                                    stroke="#8884d8" 
                                    strokeWidth={2} 
                                    dot={false} 
                                    isAnimationActive={false} // Disable animation for smoother updates
                                />
                                {/* Optional: Add YAxis to scale properly if counts vary widely, or keep min/max auto */}
                                {/* <YAxis domain={['auto', 'auto']} hide /> */}
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                )
            }
        }
    ]

    if (onUnregister) {
        columns.push({
            id: 'actions',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Action' className='w-full justify-center' />
            ),
            cell: ({ row }) => (
                <div className='flex justify-center'>
                    <Button
                        variant='destructive'
                        size='sm'
                        onClick={() => onUnregister(row.original.table_name)}
                    >
                        Drop
                    </Button>
                </div>
            )
        })
    }

    return columns
}
