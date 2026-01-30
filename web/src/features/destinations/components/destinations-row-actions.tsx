import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import { type Row } from '@tanstack/react-table'
import { Trash2, Lock, Copy, Info } from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { toast } from 'sonner'
import { destinationsRepo } from '@/repo/destinations'
import { Button } from '@/components/ui/button'
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuSeparator,
    DropdownMenuShortcut,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { destinationSchema } from '../data/schema'
import { useDestinations } from './destinations-provider'

type DataTableRowActionsProps<TData> = {
    row: Row<TData>
}

export function DestinationsRowActions<TData>({
    row,
}: DataTableRowActionsProps<TData>) {
    const destination = destinationSchema.parse(row.original)
    const navigate = useNavigate()

    const { setOpen, setCurrentRow } = useDestinations()
    const queryClient = useQueryClient()

    const duplicateMutation = useMutation({
        mutationFn: destinationsRepo.duplicate,
        onSuccess: async () => {
            await queryClient.invalidateQueries({ queryKey: ['destinations'] })
            toast.success('Destination duplicated successfully')
        },
        onError: () => {
            toast.error('Failed to duplicate destination')
        }
    })

    return (
        <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
                <Button
                    variant='ghost'
                    className='flex h-8 w-8 p-0 data-[state=open]:bg-muted'
                >
                    <DotsHorizontalIcon className='h-4 w-4' />
                    <span className='sr-only'>Open menu</span>
                </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align='end' className='w-[160px]'>
                {destination.type === 'SNOWFLAKE' && (
                    <DropdownMenuItem
                        onClick={() => navigate({ to: '/destinations/$destinationId', params: { destinationId: destination.id } })}
                    >
                        Detail
                        <DropdownMenuShortcut>
                            <Info size={16} />
                        </DropdownMenuShortcut>
                    </DropdownMenuItem>
                )}
                <DropdownMenuItem
                    onClick={() => {
                        setCurrentRow(destination)
                        setOpen('update')
                    }}
                    disabled={destination.is_used_in_active_pipeline}
                >
                    Edit
                    {destination.is_used_in_active_pipeline && (
                        <DropdownMenuShortcut>
                            <Lock size={16} />
                        </DropdownMenuShortcut>
                    )}
                </DropdownMenuItem>
                <DropdownMenuItem
                    onClick={() => {
                        duplicateMutation.mutate(destination.id)
                    }}
                >
                    Duplicate
                    <DropdownMenuShortcut>
                        <Copy size={16} />
                    </DropdownMenuShortcut>
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                    onClick={() => {
                        setCurrentRow(destination)
                        setOpen('delete')
                    }}
                >
                    Delete
                    <DropdownMenuShortcut>
                        <Trash2 size={16} />
                    </DropdownMenuShortcut>
                </DropdownMenuItem>
            </DropdownMenuContent>
        </DropdownMenu>
    )
}
