import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type Destination } from '../data/schema'
import { DestinationsRowActions } from './destinations-row-actions'
import { Snowflake } from 'lucide-react'

export const destinationsColumns: ColumnDef<Destination>[] = [
    {
        accessorKey: 'name',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Name' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center'>
                <span className='truncate font-medium'>{row.getValue('name')}</span>
            </div>
        ),
        meta: { title: 'Name' },
    },
    {
        id: 'connection',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Connection' />
        ),
        cell: ({ row }) => {
            const type = row.original.type
            const config = row.original.config
            const isSnowflake = type === 'SNOWFLAKE'
            
            if (isSnowflake) {
                return <span className="truncate">{config.account}</span>
            }
            return <span className="truncate">{config.host}:{config.port}</span>
        },
        meta: { title: 'Connection' },
    },
    {
        id: 'database',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Database' />
        ),
        cell: ({ row }) => {
            const type = row.original.type
            const config = row.original.config
            const isSnowflake = type === 'SNOWFLAKE'
            
            if (isSnowflake) {
                return <span className="truncate">{config.database} / {config.schema}</span>
            }
            return <span className="truncate">{config.database}</span>
        },
        meta: { title: 'Database' },
    },
    {
        accessorKey: 'type',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Type' className="w-[150px]" />
        ),
        cell: ({ row }) => {
            const type = row.getValue('type') as string
            const isSnowflake = type.toLowerCase() === 'snowflake'
            const isPostgres = type.toLowerCase() === 'postgres'
            return (
                <div className={`flex items-center gap-2 w-[150px] ${isSnowflake ? 'text-[#29b5e8]' : ''}`}>
                    {isSnowflake && <Snowflake className='h-4 w-4' />}
                    <span className='truncate font-medium capitalize'>
                        {isPostgres ? (
                            <span>Postgre<span style={{ color: '#316192' }}>SQL</span></span>
                        ) : (
                            type
                        )}
                    </span>
                </div>
            )
        },
        meta: { title: 'Type' },
    },
    {
        id: 'actions',
        cell: ({ row }) => <div className="w-[50px] flex justify-end"><DestinationsRowActions row={row} /></div>,
        meta: { title: 'Actions' },
    },
]

