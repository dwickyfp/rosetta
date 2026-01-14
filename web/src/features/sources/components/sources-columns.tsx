import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type Source } from '../data/schema'
import { SourcesRowActions } from './sources-row-actions'

export const sourcesColumns: ColumnDef<Source>[] = [
    {
        accessorKey: 'id',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='ID' />
        ),
        cell: ({ row }) => <div className='w-[40px]'>{row.getValue('id')}</div>,
        enableSorting: false,
        enableHiding: false,
    },
    {
        accessorKey: 'name',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Name' />
        ),
        cell: ({ row }) => (
            <div className='flex space-x-2'>
                <span className='truncate font-medium'>{row.getValue('name')}</span>
            </div>
        ),
    },
    {
        accessorKey: 'pg_host',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Host' />
        ),
        cell: ({ row }) => (
            <div className='flex space-x-2'>
                <span className='truncate'>{row.getValue('pg_host')}</span>
            </div>
        ),
    },
    {
        accessorKey: 'pg_database',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Database' />
        ),
    },
    {
        accessorKey: 'pg_username',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='User' />
        ),
    },
    {
        accessorKey: 'publication_name',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Publication' />
        ),
    },
    {
        accessorKey: 'replication_id',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Slot ID' />
        ),
    },
    {
        id: 'actions',
        cell: ({ row }) => <SourcesRowActions row={row} />,
    },
]
