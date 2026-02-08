import { ColumnDef } from '@tanstack/react-table'
import { Pipeline } from '@/repo/pipelines'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { PipelineAnimatedArrow } from './pipeline-animated-arrow.tsx'
import { PipelineRowActions } from './pipeline-row-actions.tsx'
import { Workflow, FolderInput, FolderSync } from 'lucide-react'
import { Switch } from '@/components/ui/switch'
import { pipelinesRepo } from '@/repo/pipelines'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'


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
      const destinations = row.original.destinations || []
      const destCount = destinations.length
      const firstDestName = destinations[0]?.destination?.name

      return (
        <div className='flex items-center gap-3'>
          {/* Source */}
          <div className="flex items-center gap-2 justify-end">
            <div className="flex flex-col items-end">
              <span className="font-semibold text-sm leading-none">{sourceName}</span>
              <span className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium mt-0.5">Source</span>
            </div>
            <div className="p-1.5 rounded-md bg-blue-50 dark:bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-100 dark:border-blue-500/20">
              <FolderInput className="h-3.5 w-3.5" />
            </div>
          </div>

          {/* Connection */}
          <div className="shrink-0 -mx-1">
            <PipelineAnimatedArrow />
          </div>

          {/* Destination */}
          <div className="flex items-center gap-2">
            <div className="p-1.5 rounded-md bg-purple-50 dark:bg-purple-500/10 text-purple-600 dark:text-purple-400 border border-purple-100 dark:border-purple-500/20">
              <FolderSync className="h-3.5 w-3.5" />
            </div>
            <div className="flex flex-col">
              {destCount > 1 ? (
                <span className="font-semibold text-sm leading-none">{destCount} Destinations</span>
              ) : (
                <span className="font-semibold text-sm leading-none">{firstDestName || 'No Destination'}</span>
              )}
              <span className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium mt-0.5">Destination</span>
            </div>
          </div>
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
      const isRunning = status === 'START'
      const isRefresh = status === 'REFRESH'
      const isPaused = status === 'PAUSE'

      // Determine display text and styling
      let displayText = status
      let dotColor = 'bg-gray-400 dark:bg-gray-500'
      let bgColor = 'bg-gray-50 dark:bg-gray-900/50'
      let textColor = 'text-gray-700 dark:text-gray-300'

      if (isRunning) {
        displayText = 'Running'
        dotColor = 'bg-green-500 dark:bg-green-400'
        bgColor = 'bg-green-50 dark:bg-green-950/50'
        textColor = 'text-green-700 dark:text-green-400'
      } else if (isRefresh) {
        displayText = 'Refreshing'
        dotColor = 'bg-blue-500 dark:bg-blue-400'
        bgColor = 'bg-blue-50 dark:bg-blue-950/50'
        textColor = 'text-blue-700 dark:text-blue-400'
      } else if (isPaused) {
        displayText = 'Paused'
        dotColor = 'bg-gray-400 dark:bg-gray-500'
        bgColor = 'bg-gray-50 dark:bg-gray-900/50'
        textColor = 'text-gray-600 dark:text-gray-400'
      }

      return (
        <div className={cn(
          'inline-flex items-center gap-2 px-2.5 py-1 rounded-md text-sm font-medium',
          bgColor,
          textColor
        )}>
          <span className={cn('h-2 w-2 rounded-full', dotColor)} />
          {displayText}
        </div>
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
      variant="outline"
      size="icon"
      className='h-8 w-8 p-0'
      onClick={() => navigate({ to: '/pipelines/$pipelineId', params: { pipelineId: String(pipelineId) } })}
    >
      <Workflow className="h-4 w-4" />
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
