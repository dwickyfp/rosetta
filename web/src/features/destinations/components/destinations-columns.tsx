import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type Destination } from '../data/schema'
import { DestinationsRowActions } from './destinations-row-actions'
import { Snowflake, Database } from 'lucide-react'
import { cn } from '@/lib/utils'
import { formatDistanceToNow } from 'date-fns'
import { CopyButton } from '@/components/copy-button'

export const destinationsColumns: ColumnDef<Destination>[] = [
    {
        accessorKey: 'name',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Name' />
        ),
        cell: ({ row }) => (
            <div className='flex flex-col'>
                <span className='truncate font-medium'>{row.getValue('name')}</span>
                <span className='truncate text-xs text-muted-foreground'>{row.original.type}</span>
            </div>
        ),
        meta: { title: 'Name' },
    },
    {
        accessorKey: 'status',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Status' />
        ),
        cell: ({ row }) => {
            const isActive = row.original.is_used_in_active_pipeline
            return (
                <div className='flex items-center gap-2'>
                    <div className={cn("h-2 w-2 rounded-full", isActive ? "bg-green-500" : "bg-zinc-300 dark:bg-zinc-700")} />
                    <span className={cn("text-sm", isActive ? "text-foreground" : "text-muted-foreground")}>
                        {isActive ? "Active" : "Idle"}
                    </span>
                </div>
            )
        },
        meta: { title: 'Status' },
    },
    {
        id: 'connection',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Connection Details' />
        ),
        cell: ({ row }) => {
            const type = row.original.type
            const config = row.original.config
            const isSnowflake = type === 'SNOWFLAKE'

            let mainInfo = ''
            let subInfo = ''

            if (isSnowflake) {
                mainInfo = config.account
                subInfo = `${config.database} / ${config.schema}`
            } else {
                mainInfo = `${config.host}:${config.port}`
                subInfo = config.database
            }

            return (
                <div className="flex flex-col gap-1 max-w-[300px]">
                    <div className="flex items-center gap-2 text-sm font-medium">
                        {isSnowflake ? <Snowflake className="h-3.5 w-3.5 text-[#29b5e8]" /> : <Database className="h-3.5 w-3.5 text-blue-500" />}
                        <span className="truncate">{mainInfo}</span>
                        <CopyButton value={mainInfo} className="opacity-0 group-hover:opacity-100 transition-opacity h-6 w-6" />
                    </div>
                    <span className="text-xs text-muted-foreground truncate" title={subInfo}>{subInfo}</span>
                </div>
            )
        },
        meta: { title: 'Connection' },
    },
    {
        accessorKey: 'created_at',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Created' />
        ),
        cell: ({ row }) => {
            return (
                <span className="text-muted-foreground text-sm">
                    {formatDistanceToNow(new Date(row.getValue('created_at')), { addSuffix: true })}
                </span>
            )
        },
        meta: { title: 'Created' },
    },
    {
        id: 'actions',
        cell: ({ row }) => <div className="w-[50px] flex justify-end"><DestinationsRowActions row={row} /></div>,
        meta: { title: 'Actions' },
    },
]

