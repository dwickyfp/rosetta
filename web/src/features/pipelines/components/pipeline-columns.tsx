import { ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { Pipeline } from '@/repo/pipelines'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { PipelineAnimatedArrow } from './pipeline-animated-arrow.tsx'
import { PipelineRowActions } from './pipeline-row-actions.tsx'
import { Info } from 'lucide-react'
import { Switch } from '@/components/ui/switch'
import { pipelinesRepo } from '@/repo/pipelines'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'


export const pipelineColumns: ColumnDef<Pipeline>[] = [

  {
    id: 'details',
    header: () => <div className="text-center font-semibold">Action</div>,
    cell: ({ row }) => (
      <div className='flex items-center justify-center space-x-2'>
        <PipelineStatusSwitch pipeline={row.original} />
        <PipelineDetailsButton pipelineId={row.original.id} />
      </div>
    ),
    meta: { title: 'Action' },
  },
  {
    accessorKey: 'name',
    header: 'Name',
    cell: ({ row }) => <div className='w-[150px] font-medium'>{row.getValue('name')}</div>,
    meta: { title: 'Name' },
  },
  {
    id: 'pipelines',
    header: 'Pipelines',
    cell: ({ row }) => {
      const sourceName = row.original.source?.name || 'Unknown Source'
      const destCount = row.original.destinations?.length || 0
      return (
        <div className='flex items-center space-x-2'>
          <span className='font-medium'>{sourceName}</span>
          <PipelineAnimatedArrow />
          <span className='font-medium'>{destCount} Destination</span>
        </div>
      )
    },
    meta: { title: 'Pipelines' },
  },

  {
    accessorKey: 'status',
    header: 'Status',
    cell: ({ row }) => {
      const status = row.getValue('status') as string
      const isStart = status === 'START'
      const isRefresh = status === 'REFRESH'
      return (
        <Badge
          variant={isStart || isRefresh ? 'default' : 'secondary'}
          className={
            isStart ? 'bg-green-500 hover:bg-green-600' :
              isRefresh ? 'bg-blue-500 hover:bg-blue-600' : ''
          }
        >
          {isStart ? 'RUNNING' : status}
        </Badge>
      )
    },
    filterFn: (row, id, value) => {
      return value.includes(row.getValue(id))
    },
    meta: { title: 'Status' },
  },

  {
    id: 'actions',
    cell: ({ row }) => <PipelineRowActions row={row} />,
    meta: { title: 'Actions' },
  },
]

function PipelineDetailsButton({ pipelineId }: { pipelineId: number }) {
  const navigate = useNavigate()
  return (
    <Button
      variant="ghost"
      size="icon"
      className='h-8 w-8 p-0'
      onClick={() => navigate({ to: '/pipelines/$pipelineId', params: { pipelineId: String(pipelineId) } })}
    >
      <Info className="h-4 w-4" />
    </Button>
  )
}

function PipelineStatusSwitch({ pipeline }: { pipeline: Pipeline }) {
  const queryClient = useQueryClient()
  const isRunning = pipeline.status === 'START' || pipeline.status === 'REFRESH'

  const { mutate, isPending } = useMutation({
    mutationFn: async (checked: boolean) => {
      if (checked) {
        return pipelinesRepo.start(pipeline.id)
      } else {
        return pipelinesRepo.pause(pipeline.id)
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      toast.success('Pipeline status updated')
    },
    onError: (error) => {
      toast.error(`Failed to update status: ${error}`)
    }
  })

  return (
    <Switch
      checked={isRunning}
      onCheckedChange={(checked) => mutate(checked)}
      disabled={isPending}
    />
  )
}

