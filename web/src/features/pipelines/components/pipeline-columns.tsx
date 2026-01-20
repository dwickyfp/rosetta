import { ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { Pipeline } from '@/repo/pipelines'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { PipelineAnimatedArrow } from './pipeline-animated-arrow.tsx'
import { PipelineRowActions } from './pipeline-row-actions.tsx'
import { Info } from 'lucide-react'


export const pipelineColumns: ColumnDef<Pipeline>[] = [
  
  {
    id: 'details',
    header: () => <div className="text-center font-semibold">Action</div>,
    cell: ({ row }) => (
      <div className='flex items-center justify-center'>
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
      const destName = row.original.destination?.name || 'Unknown Destination'
      return (
        <div className='flex items-center space-x-2'>
          <span className='font-medium'>{sourceName}</span>
          <PipelineAnimatedArrow />
          <span className='font-medium'>{destName}</span>
        </div>
      )
    },
    meta: { title: 'Pipelines' },
  },
  {
    accessorKey: 'progress',
    header: 'Initialization',
    cell: ({ row }) => {
      const progress = row.original.pipeline_progress
      if (progress?.status === 'COMPLETED') return <span className='text-muted-foreground font-medium text-xs'>FINISH</span>
      if (!progress) return <span className='text-muted-foreground'>-</span>

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
    },
    meta: { title: 'Initialization' },
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

