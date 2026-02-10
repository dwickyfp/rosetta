import { useState } from 'react'
import { formatDistanceToNow } from 'date-fns'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { backfillApi, BackfillFilter } from '@/repo/backfill'
import { sourcesRepo } from '@/repo/sources'
import {
  Plus,
  X,
  Loader2,
  AlertCircle,
  CheckCircle2,
  Clock,
} from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

interface BackfillDataTabProps {
  pipelineId: number
  sourceId: number
}

const OPERATORS = [
  { value: '=', label: 'Equals (=)' },
  { value: '!=', label: 'Not Equals (!=)' },
  { value: '>', label: 'Greater Than (>)' },
  { value: '<', label: 'Less Than (<)' },
  { value: '>=', label: 'Greater or Equal (>=)' },
  { value: '<=', label: 'Less or Equal (<=)' },
  { value: 'LIKE', label: 'Like (LIKE)' },
  { value: 'ILIKE', label: 'Case Insensitive Like (ILIKE)' },
  { value: 'IS NULL', label: 'Is Null' },
  { value: 'IS NOT NULL', label: 'Is Not Null' },
]

const STATUS_CONFIG = {
  PENDING: {
    label: 'Pending',
    color:
      'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400',
    icon: Clock,
  },
  EXECUTING: {
    label: 'Executing',
    color: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400',
    icon: Loader2,
  },
  COMPLETED: {
    label: 'Completed',
    color:
      'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400',
    icon: CheckCircle2,
  },
  FAILED: {
    label: 'Failed',
    color: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400',
    icon: AlertCircle,
  },
  CANCELLED: {
    label: 'Cancelled',
    color: 'bg-gray-100 text-gray-800 dark:bg-gray-900/30 dark:text-gray-400',
    icon: X,
  },
}

function CreateBackfillDialog({ pipelineId, sourceId }: BackfillDataTabProps) {
  const [open, setOpen] = useState(false)
  const [tableName, setTableName] = useState('')
  const [filters, setFilters] = useState<BackfillFilter[]>([])
  const queryClient = useQueryClient()

  // Fetch available tables for source
  const { data: sourceDetails } = useQuery({
    queryKey: ['source-details', sourceId],
    queryFn: () => sourcesRepo.getDetails(sourceId),
    enabled: open,
  })

  const createMutation = useMutation({
    mutationFn: (data: { table_name: string; filters?: BackfillFilter[] }) =>
      backfillApi.createJob(pipelineId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backfill-jobs', pipelineId] })
      toast.success('Backfill job created successfully')
      setOpen(false)
      resetForm()
    },
    onError: (error: any) => {
      toast.error(
        `Failed to create backfill job: ${error.response?.data?.detail || error.message}`
      )
    },
  })

  const resetForm = () => {
    setTableName('')
    setFilters([])
  }

  const handleTableChange = (value: string) => {
    setTableName(value)
    // Reset filters when table changes
    setFilters([])
  }

  const addFilter = () => {
    if (!tableName) {
      toast.error('Please select a table first')
      return
    }
    if (filters.length >= 5) {
      toast.error('Maximum 5 filters allowed')
      return
    }
    setFilters([...filters, { column: '', operator: '=', value: '' }])
  }

  const removeFilter = (index: number) => {
    setFilters(filters.filter((_, i) => i !== index))
  }

  const updateFilter = (
    index: number,
    field: keyof BackfillFilter,
    value: string
  ) => {
    const newFilters = [...filters]
    newFilters[index] = { ...newFilters[index], [field]: value }
    setFilters(newFilters)
  }

  // Get columns for selected table
  const selectedTableColumns = sourceDetails?.tables?.find(
    (t) => t.table_name === tableName
  )?.schema_table

  const handleSubmit = () => {
    if (!tableName) {
      toast.error('Please select a table')
      return
    }

    // Validate filters
    const validFilters = filters.filter((f) => f.column && f.operator)
    if (filters.length > 0 && validFilters.length !== filters.length) {
      toast.error('Please complete all filter fields')
      return
    }

    createMutation.mutate({
      table_name: tableName,
      filters: validFilters.length > 0 ? validFilters : undefined,
    })
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className='mr-2 h-4 w-4' />
          Create Backfill
        </Button>
      </DialogTrigger>
      <DialogContent className='sm:max-w-150'>
        <DialogHeader>
          <DialogTitle>Create Backfill Job</DialogTitle>
          <DialogDescription>
            Backfill historical data from source to destination. Add up to 5
            filters.
          </DialogDescription>
        </DialogHeader>

        <div className='space-y-4 py-4'>
          {/* Table Selection */}
          <div className='space-y-2'>
            <Label htmlFor='table'>Table Name</Label>
            <Select value={tableName} onValueChange={handleTableChange}>
              <SelectTrigger id='table'>
                <SelectValue placeholder='Select a table' />
              </SelectTrigger>
              <SelectContent>
                {sourceDetails?.tables?.map((table) => (
                  <SelectItem key={table.table_name} value={table.table_name}>
                    {table.table_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Filters */}
          <div className='space-y-2'>
            <div className='flex items-center justify-between'>
              <Label>Filters (Optional)</Label>
              <Button
                type='button'
                variant='outline'
                size='sm'
                onClick={addFilter}
                disabled={filters.length >= 5 || !tableName}
              >
                <Plus className='mr-1 h-4 w-4' />
                Add Filter
              </Button>
            </div>

            {!tableName && filters.length === 0 && (
              <p className='text-sm text-muted-foreground'>
                Select a table first to add filters.
              </p>
            )}
            {filters.map((filter, index) => (
              <div
                key={index}
                className='flex items-end gap-2 rounded-lg border p-3'
              >
                <div className='flex-1 space-y-1'>
                  <Label className='text-xs'>Column</Label>
                  <Select
                    value={filter.column}
                    onValueChange={(value) =>
                      updateFilter(index, 'column', value)
                    }
                    disabled={!tableName}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder='Select column' />
                    </SelectTrigger>
                    <SelectContent>
                      {selectedTableColumns?.map((col) => (
                        <SelectItem
                          key={col.column_name}
                          value={col.column_name}
                        >
                          {col.column_name}
                          <span className='ml-2 text-xs text-muted-foreground'>
                            ({col.data_type || col.real_data_type})
                          </span>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className='w-45 space-y-1'>
                  <Label className='text-xs'>Operator</Label>
                  <Select
                    value={filter.operator}
                    onValueChange={(value) =>
                      updateFilter(index, 'operator', value)
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {OPERATORS.map((op) => (
                        <SelectItem key={op.value} value={op.value}>
                          {op.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className='flex-1 space-y-1'>
                  <Label className='text-xs'>Value</Label>
                  <Input
                    placeholder='Value'
                    value={filter.value}
                    onChange={(e) =>
                      updateFilter(index, 'value', e.target.value)
                    }
                    disabled={
                      filter.operator === 'IS NULL' ||
                      filter.operator === 'IS NOT NULL'
                    }
                  />
                </div>
                <Button
                  type='button'
                  variant='ghost'
                  size='icon'
                  onClick={() => removeFilter(index)}
                >
                  <X className='h-4 w-4' />
                </Button>
              </div>
            ))}

            {tableName && filters.length === 0 && (
              <p className='text-sm text-muted-foreground'>
                No filters added. All data from the table will be backfilled.
              </p>
            )}
          </div>
        </div>

        <DialogFooter>
          <Button variant='outline' onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={createMutation.isPending}>
            {createMutation.isPending && (
              <Loader2 className='mr-2 h-4 w-4 animate-spin' />
            )}
            Create Job
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export function BackfillDataTab({
  pipelineId,
  sourceId,
}: BackfillDataTabProps) {
  const queryClient = useQueryClient()

  // Fetch backfill jobs
  const { data: jobsData, isLoading } = useQuery({
    queryKey: ['backfill-jobs', pipelineId],
    queryFn: () => backfillApi.listJobs(pipelineId),
    refetchInterval: 5000, // Refresh every 5 seconds to show progress
  })

  // Cancel mutation
  const cancelMutation = useMutation({
    mutationFn: (jobId: number) => backfillApi.cancelJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backfill-jobs', pipelineId] })
      toast.success('Backfill job cancelled')
    },
    onError: (error: any) => {
      toast.error(
        `Failed to cancel job: ${error.response?.data?.detail || error.message}`
      )
    },
  })

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: (jobId: number) => backfillApi.deleteJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backfill-jobs', pipelineId] })
      toast.success('Backfill job deleted')
    },
    onError: (error: any) => {
      toast.error(
        `Failed to delete job: ${error.response?.data?.detail || error.message}`
      )
    },
  })

  const formatNumber = (num: number) => new Intl.NumberFormat().format(num)

  return (
    <div className='space-y-4'>
      <div className='flex items-center justify-between'>
        <div>
          <h3 className='text-lg font-semibold'>Backfill Jobs</h3>
          <p className='text-sm text-muted-foreground'>
            Manage historical data backfill operations
          </p>
        </div>
        <CreateBackfillDialog pipelineId={pipelineId} sourceId={sourceId} />
      </div>

      {isLoading ? (
        <div className='flex h-64 items-center justify-center'>
          <Loader2 className='h-8 w-8 animate-spin text-muted-foreground' />
        </div>
      ) : jobsData?.items.length === 0 ? (
        <div className='flex h-64 flex-col items-center justify-center rounded-lg border'>
          <p className='mb-4 text-muted-foreground'>No backfill jobs yet</p>
          <CreateBackfillDialog pipelineId={pipelineId} sourceId={sourceId} />
        </div>
      ) : (
        <div className='rounded-lg border'>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Table Name</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Records</TableHead>
                <TableHead>Filters</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className='text-right'>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {jobsData?.items.map((job) => {
                const StatusIcon = STATUS_CONFIG[job.status].icon
                return (
                  <TableRow key={job.id}>
                    <TableCell className='font-medium'>
                      {job.table_name}
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant='outline'
                        className={STATUS_CONFIG[job.status].color}
                      >
                        <StatusIcon className='mr-1 h-3 w-3' />
                        {STATUS_CONFIG[job.status].label}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      {job.total_record > 0
                        ? `${formatNumber(job.count_record)}/${formatNumber(job.total_record)}`
                        : formatNumber(job.count_record)}
                    </TableCell>
                    <TableCell>
                      {job.filter_sql ? (
                        <span className='text-xs text-muted-foreground'>
                          {job.filter_sql.split(';').length} filter(s)
                        </span>
                      ) : (
                        <span className='text-xs text-muted-foreground'>
                          None
                        </span>
                      )}
                    </TableCell>
                    <TableCell className='text-sm text-muted-foreground'>
                      {formatDistanceToNow(new Date(job.created_at), {
                        addSuffix: true,
                      })}
                    </TableCell>
                    <TableCell className='text-right'>
                      <div className='flex justify-end gap-2'>
                        {(job.status === 'PENDING' ||
                          job.status === 'EXECUTING') && (
                          <Button
                            variant='outline'
                            size='sm'
                            onClick={() => cancelMutation.mutate(job.id)}
                            disabled={cancelMutation.isPending}
                          >
                            Cancel
                          </Button>
                        )}
                        {(job.status === 'COMPLETED' ||
                          job.status === 'FAILED' ||
                          job.status === 'CANCELLED') && (
                          <Button
                            variant='ghost'
                            size='sm'
                            onClick={() => deleteMutation.mutate(job.id)}
                            disabled={deleteMutation.isPending}
                          >
                            Delete
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  )
}
