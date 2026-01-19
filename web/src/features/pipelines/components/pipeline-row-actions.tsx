import { MoreHorizontal } from 'lucide-react'
import { Row } from '@tanstack/react-table'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Pipeline, pipelinesRepo } from '@/repo/pipelines'
import { useQueryClient, useMutation } from '@tanstack/react-query'
import { toast } from 'sonner'



interface DataTableRowActionsProps<TData> {
  row: Row<TData>
}

export function PipelineRowActions<TData>({ row }: DataTableRowActionsProps<TData>) {
  const pipeline = row.original as Pipeline
  const queryClient = useQueryClient()

  const { mutate: deleteMutate } = useMutation({
    mutationFn: pipelinesRepo.delete,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      toast.success('Pipeline deleted')
    },
    onError: () => {
      toast.error('Failed to delete pipeline')
    }
  })

  const { mutate: startMutate } = useMutation({
    mutationFn: pipelinesRepo.start,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      toast.success('Pipeline started')
    },
    onError: () => {
      toast.error('Failed to start pipeline')
    }
  })

  const { mutate: pauseMutate } = useMutation({
    mutationFn: pipelinesRepo.pause,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      toast.success('Pipeline paused')
    },
    onError: () => {
      toast.error('Failed to pause pipeline')
    }
  })

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant='ghost' className='flex h-8 w-8 p-0 data-[state=open]:bg-muted'>
          <MoreHorizontal className='h-4 w-4' />
          <span className='sr-only'>Open menu</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align='end' className='w-[160px]'>
        {pipeline.status === 'START' ? (
          <DropdownMenuItem onClick={() => pauseMutate(pipeline.id)}>
            Pause
          </DropdownMenuItem>
        ) : (
          <DropdownMenuItem onClick={() => startMutate(pipeline.id)}>
            Start
          </DropdownMenuItem>
        )}
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={() => deleteMutate(pipeline.id)}>
          Delete
          <DropdownMenuShortcut>⌘⌫</DropdownMenuShortcut>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

