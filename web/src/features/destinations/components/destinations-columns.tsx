import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type Destination } from '../data/schema'
import { DestinationsRowActions } from './destinations-row-actions'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { Info } from 'lucide-react'

export const destinationsColumns: ColumnDef<Destination>[] = [
    {
        id: 'details',
        header: () => <div className="text-center font-semibold">Action</div>,
        cell: ({ row }) => (
            <div className='flex items-center justify-center'>
                <DestinationDetailsButton destinationId={row.original.id} />
            </div>
        ),
        meta: { title: 'Detail' },
    },
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
        accessorKey: 'snowflake_account',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Account' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center'>
                <span className='truncate'>{row.getValue('snowflake_account')}</span>
            </div>
        ),
        meta: { title: 'Account' },
    },
    {
        accessorKey: 'snowflake_user',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='User' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center'>
                <span>{row.getValue('snowflake_user')}</span>
            </div>
        ),
        meta: { title: 'User' },
    },
    {
        accessorKey: 'snowflake_database',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Database' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center'>
                <span>{row.getValue('snowflake_database')}</span>
            </div>
        ),
        meta: { title: 'Database' },
    },
    {
        accessorKey: 'snowflake_role',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Role' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center'>
                <span>{row.getValue('snowflake_role')}</span>
            </div>
        ),
        meta: { title: 'Role' },
    },
    {
        accessorKey: 'snowflake_warehouse',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Warehouse' />
        ),
        cell: ({ row }) => (
            <div className='flex items-center'>
                <span>{row.getValue('snowflake_warehouse')}</span>
            </div>
        ),
        meta: { title: 'Warehouse' },
    },
    {
        id: 'actions',
        cell: ({ row }) => <DestinationsRowActions row={row} />,
        meta: { title: 'Actions' },
    },
]

function DestinationDetailsButton({ destinationId }: { destinationId: number }) {
    const navigate = useNavigate()
    return (
        <Button
            variant="ghost"
            size="icon"
            className='h-8 w-8 p-0'
            onClick={() => navigate({ to: '/destinations/$destinationId', params: { destinationId: destinationId } })}
        >
            <Info className="h-4 w-4" />
        </Button>
    )
}
