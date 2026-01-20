import { ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Pipeline } from '@/repo/pipelines'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { PipelineAnimatedArrow } from './pipeline-animated-arrow.tsx'
import { PipelineRowActions } from './pipeline-row-actions.tsx'


export const pipelineColumns: ColumnDef<Pipeline>[] = [
  {
    id: 'select',
    header: ({ table }) => (
      <Checkbox
        checked={
          table.getIsAllPageRowsSelected() || (table.getIsSomePageRowsSelected() && 'indeterminate')
        }
        onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
        aria-label='Select all'
        className='translate-y-[2px]'
      />
    ),
    cell: ({ row }) => (
      <Checkbox
        checked={row.getIsSelected()}
        onCheckedChange={(value) => row.toggleSelected(!!value)}
        aria-label='Select row'
        className='translate-y-[2px]'
      />
    ),
    enableSorting: false,
    enableHiding: false,
  },
  {
    accessorKey: 'name',
    header: 'Name',
    cell: ({ row }) => <div className='w-[150px] font-medium'>{row.getValue('name')}</div>,
  },
  {
    id: 'pipelines',
    header: 'Pipelines',
    cell: ({ row }) => {
      const sourceName = row.original.source?.name || 'Unknown Source'
      const destName = row.original.destination?.name || 'Unknown Destination'
      return (
        <div className='flex items-center space-x-2'>
          <span className='font-medium text-muted-foreground'>{sourceName}</span>
          <PipelineAnimatedArrow />
          <span className='font-medium text-muted-foreground'>{destName}</span>
        </div>
      )
    },
  },
  {
    accessorKey: 'progress',
    header: 'Initialization',
    cell: ({ row }) => {
      const progress = row.original.pipeline_progress
      if (!progress || progress.status === 'COMPLETED') return <span className='text-muted-foreground'>-</span>

      return (
        <div className='flex flex-col space-y-1 w-[140px]'>
          <div className='flex justify-between text-xs'>
            <span>{progress.status === 'FAILED' ? 'Failed' : `${progress.progress}%`}</span>
          </div>
          {progress.status !== 'FAILED' && (
            <div className='h-2 w-full bg-secondary rounded-full overflow-hidden'>
              <div
                className='h-full bg-primary transition-all duration-500 ease-in-out'
                style={{ width: `${progress.progress}%` }}
              />
            </div>
          )}
          {progress.step && <span className='text-[10px] text-muted-foreground truncate' title={progress.step}>{progress.step}</span>}
          {progress.status === 'FAILED' && <span className='text-[10px] text-destructive truncate' title={progress.details}>{progress.details}</span>}
        </div>
      )
    }
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
  },
  {
    id: 'details',
    header: 'Details',
    cell: ({ row }) => {
      // We can't use hooks here directly if it's not a component definition, 
      // but Tanstack Table cell is rendered as a component.
      // However, to be safe and cleaner, let's extract a component or use a simple Link if available,
      // or just use `window.location`? No, SPA.
      // Best to use a small component.
      return <PipelineDetailsButton pipelineId={row.original.id} />
    }
  },
  {
    id: 'actions',
    cell: ({ row }) => <PipelineRowActions row={row} />,
  },
]

function PipelineDetailsButton({ pipelineId }: { pipelineId: number }) {
  const navigate = useNavigate()
  return (
    <Button
      variant="outline"
      size="sm"
      onClick={() => navigate({ to: '/pipelines/$pipelineId', params: { pipelineId: String(pipelineId) } })}
    >
      Details
    </Button>
  )
}

