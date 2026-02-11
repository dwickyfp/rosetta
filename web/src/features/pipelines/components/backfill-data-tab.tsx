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
import { Progress } from '@/components/ui/progress'
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
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { Check, ChevronsUpDown } from 'lucide-react'
import { cn } from '@/lib/utils'

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
  const [openCombobox, setOpenCombobox] = useState(false)
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
        `Failed to create backfill job: ${error.response?.data?.message || error.response?.data?.detail || error.message}`
      )
    },
  })

  // Handle dialog open state change
  const handleOpenChange = (newOpen: boolean) => {
    if (!newOpen) {
      resetForm()
    }
    setOpen(newOpen)
  }

  const resetForm = () => {
    setTableName('')
    setFilters([])
  }

  const handleTableChange = (value: string) => {
    setTableName(value)
    // Reset filters when table changes
    setFilters([])
    setOpenCombobox(false)
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
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button>
          <Plus className='mr-2 h-4 w-4' />
          Create Backfill
        </Button>
      </DialogTrigger>
      <DialogContent className='sm:max-w-[600px]'>
        <DialogHeader>
          <DialogTitle>Create Backfill Job</DialogTitle>
          <DialogDescription>
            Backfill historical data from source to destination. Add up to 5
            filters to limit the scope.
          </DialogDescription>
        </DialogHeader>

        <div className='grid gap-6 py-4'>
          {/* Table Selection */}
          <div className='grid gap-2'>
            <Label htmlFor='table'>Table Name</Label>
            <Popover open={openCombobox} onOpenChange={setOpenCombobox}>
              <PopoverTrigger asChild>
                <Button
                  variant='outline'
                  role='combobox'
                  aria-expanded={openCombobox}
                  className='w-full justify-between'
                >
                  {tableName || 'Select a table...'}
                  <ChevronsUpDown className='ml-2 h-4 w-4 shrink-0 opacity-50' />
                </Button>
              </PopoverTrigger>
              <PopoverContent className='w-[400px] p-0'>
                <Command>
                  <CommandInput placeholder='Search table...' />
                  <CommandList>
                    <CommandEmpty>No table found.</CommandEmpty>
                    <CommandGroup>
                      {sourceDetails?.tables?.map((table) => (
                        <CommandItem
                          key={table.table_name}
                          value={table.table_name}
                          onSelect={handleTableChange}
                        >
                          <Check
                            className={cn(
                              'mr-2 h-4 w-4',
                              tableName === table.table_name
                                ? 'opacity-100'
                                : 'opacity-0'
                            )}
                          />
                          {table.table_name}
                        </CommandItem>
                      ))}
                    </CommandGroup>
                  </CommandList>
                </Command>
              </PopoverContent>
            </Popover>
          </div>

          {/* Filters */}
          <div className='grid gap-4'>
            <div className='flex items-center justify-between'>
              <Label>Filters (Optional)</Label>
              <Button
                type='button'
                variant='outline'
                size='sm'
                onClick={addFilter}
                disabled={filters.length >= 5 || !tableName}
              >
                <Plus className='mr-2 h-3 w-3' />
                Add Filter
              </Button>
            </div>

            {!tableName && filters.length === 0 && (
              <div className='rounded-lg border border-dashed p-8 text-center'>
                <p className='text-sm text-muted-foreground'>
                  Select a table above to enable filters.
                </p>
              </div>
            )}

            {tableName && filters.length === 0 && (
              <div className='rounded-lg border border-dashed p-8 text-center'>
                <p className='text-sm text-muted-foreground'>
                  No filters active. All records from <span className="font-mono text-foreground">{tableName}</span> will be backfilled.
                </p>
              </div>
            )}

            {filters.length > 0 && (
              <div className='grid gap-3'>
                {filters.map((filter, index) => (
                  <div
                    key={index}
                    className='grid grid-cols-[1fr_140px_1fr_auto] items-end gap-2 rounded-lg border bg-muted/40 p-3'
                  >
                    <div className='space-y-1.5'>
                      <Label className='text-xs text-muted-foreground'>Column</Label>
                      <Select
                        value={filter.column}
                        onValueChange={(value) =>
                          updateFilter(index, 'column', value)
                        }
                        disabled={!tableName}
                      >
                        <SelectTrigger className="h-8">
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

                    <div className='space-y-1.5'>
                      <Label className='text-xs text-muted-foreground'>Operator</Label>
                      <Select
                        value={filter.operator}
                        onValueChange={(value) =>
                          updateFilter(index, 'operator', value)
                        }
                      >
                        <SelectTrigger className="h-8">
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

                    <div className='space-y-1.5'>
                      <Label className='text-xs text-muted-foreground'>Value</Label>
                      <Input
                        className="h-8"
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
                      className="h-8 w-8 text-muted-foreground hover:text-destructive"
                      onClick={() => removeFilter(index)}
                    >
                      <X className='h-4 w-4' />
                    </Button>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <DialogFooter>
          <Button variant='outline' onClick={() => handleOpenChange(false)}>
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
        `Failed to cancel job: ${error.response?.data?.message || error.response?.data?.detail || error.message}`
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
        `Failed to delete job: ${error.response?.data?.message || error.response?.data?.detail || error.message}`
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
                <TableHead>Progress</TableHead>
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
                        <StatusIcon
                          className={`mr-1 h-3 w-3 ${job.status === 'EXECUTING' ? 'animate-spin' : ''}`}
                        />
                        {STATUS_CONFIG[job.status].label}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      {job.total_record > 0
                        ? `${formatNumber(job.count_record)}/${formatNumber(job.total_record)}`
                        : formatNumber(job.count_record)}
                    </TableCell>
                    <TableCell>
                      <div className='flex items-center gap-2'>
                        <Progress
                          value={
                            job.total_record > 0
                              ? (job.count_record / job.total_record) * 100
                              : 0
                          }
                          className='h-2 w-24'
                        />
                        <span className='text-xs whitespace-nowrap text-muted-foreground'>
                          {job.total_record > 0
                            ? `${Math.round((job.count_record / job.total_record) * 100)}%`
                            : '0%'}
                        </span>
                      </div>
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
