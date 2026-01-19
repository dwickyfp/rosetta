import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type Destination } from '../data/schema'
import { DestinationsRowActions } from './destinations-row-actions'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'

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
    },
    {
        id: 'details',
        cell: ({ row }) => (
            <div className='flex items-center justify-center'>
                <DestinationDetailsButton destinationId={row.original.id} />
            </div>
        ),
    },
    {
        id: 'actions',
        cell: ({ row }) => <DestinationsRowActions row={row} />,
    },
]

function DestinationDetailsButton({ destinationId }: { destinationId: number }) {
    const navigate = useNavigate()
    return (
        <Button
            variant="outline"
            size="sm"
            onClick={() => navigate({ to: '/destinations/$destinationId', params: { destinationId: destinationId } })}
        >
            Details
        </Button>
    )
}
