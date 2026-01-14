import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type Source } from '../data/schema'
import { SourcesRowActions } from './sources-row-actions'

import { Badge } from '@/components/ui/badge'

export const sourcesColumns: ColumnDef<Source>[] = [
    {
        accessorKey: 'id',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='ID' className='w-full justify-center' />
        ),
        cell: ({ row }) => <div className='w-full text-center'>{row.getValue('id')}</div>,
        enableSorting: false,
        enableHiding: false,
    },
    {
        accessorKey: 'name',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Name' className='w-full justify-center' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center justify-center'>
                <span className='truncate font-medium'>{row.getValue('name')}</span>
            </div>
        ),
    },
    {
        accessorKey: 'pg_host',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Host' className='w-full justify-center' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center justify-center'>
                <span className='truncate'>{row.getValue('pg_host')}</span>
            </div>
        ),
    },
    {
        accessorKey: 'pg_database',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Database' className='w-full justify-center' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center justify-center'>
                <span>{row.getValue('pg_database')}</span>
            </div>
        ),
    },
    {
        accessorKey: 'pg_username',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='User' className='w-full justify-center' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center justify-center'>
                <span>{row.getValue('pg_username')}</span>
            </div>
        ),
    },
    {
        accessorKey: 'is_replication_enabled',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Replication' className='w-full justify-center' />
        ),
        cell: ({ row }) => {
            const isActive = row.getValue('is_replication_enabled')
            return (
                <div className='flex justify-center'>
                    <Badge
                        variant={isActive ? 'default' : 'secondary'}
                        className={isActive ? 'bg-green-500 hover:bg-green-600' : ''}
                    >
                        {isActive ? 'Active' : 'Not Active'}
                    </Badge>
                </div>
            )
        },
    },
    {
        accessorKey: 'is_publication_enabled',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Publication' className='w-full justify-center' />
        ),
        cell: ({ row }) => {
            const isActive = row.getValue('is_publication_enabled')
            return (
                <div className='flex justify-center'>
                    <Badge
                        variant={isActive ? 'default' : 'secondary'}
                        className={isActive ? 'bg-green-500 hover:bg-green-600' : ''}
                    >
                        {isActive ? 'Active' : 'Not Active'}
                    </Badge>
                </div>
            )
        },
    },
    {
        accessorKey: 'total_tables',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Total Tables' className='w-full justify-center' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center justify-center'>
                <span className='font-medium'>{row.getValue('total_tables')}</span>
            </div>
        ),
    },
    {
        id: 'actions',
        cell: ({ row }) => <SourcesRowActions row={row} />,
    },
]
