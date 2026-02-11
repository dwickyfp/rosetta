import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams, Link } from '@tanstack/react-router'
import { pipelinesRepo, Pipeline } from '@/repo/pipelines'
import { sourcesRepo } from '@/repo/sources'
import {
  RefreshCcw,
  GitBranch,
  Table2,
  Database,
  ArrowRight,
  RotateCcw,
} from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'
import { Button } from '@/components/ui/button'
// import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs' // Replaced with CustomTabs
import {
  CustomTabs,
  CustomTabsContent,
  CustomTabsList,
  CustomTabsTrigger,
} from '@/components/ui/custom-tabs'
import { Skeleton } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { ConfigDrawer } from '@/components/config-drawer'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { BackfillDataTab } from '@/features/pipelines/components/backfill-data-tab'
import { PipelineDataFlow } from '@/features/pipelines/components/pipeline-data-flow'
import { PipelineFlowTab } from '@/features/pipelines/components/pipeline-flow-tab'

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
      queryClient.invalidateQueries({ queryKey: ['pipeline', pipeline.id] })
      toast.success('Pipeline status updated')
    },
    onError: (error) => {
      toast.error(`Failed to update status: ${error}`)
    },
  })

  return (
    <div className='flex items-center space-x-2'>
      <span className='text-sm font-medium'>
        {isRunning ? 'Running' : 'Paused'}
      </span>
      <Switch
        checked={isRunning}
        onCheckedChange={(checked) => mutate(checked)}
        disabled={isPending}
      />
    </div>
  )
}

export default function PipelineDetailsPage() {
  const { pipelineId } = useParams({
    from: '/_authenticated/pipelines/$pipelineId',
  })
  const id = parseInt(pipelineId)
  const [isRefreshing, setIsRefreshing] = useState(false)

  // 1. Fetch Pipeline
  const {
    data: pipeline,
    isLoading: isPipelineLoading,
    error: pipelineError,
  } = useQuery({
    queryKey: ['pipeline', id],
    queryFn: async () => {
      return await pipelinesRepo.get(id)
    },
    retry: false,
  })

  // 2. Fetch Source Details using pipeline.source_id
  const { data: sourceDetails, isLoading: isSourceLoading } = useQuery({
    queryKey: ['source-details', pipeline?.source_id],
    queryFn: () => sourcesRepo.getDetails(pipeline!.source_id),
    enabled: !!pipeline?.source_id,
  })

  const handleRefresh = async () => {
    if (!pipeline) return
    setIsRefreshing(true)
    try {
      await pipelinesRepo.refresh(id)
      await sourcesRepo.refreshSource(pipeline.source_id)
      toast.success('Pipeline and Source refreshed')
    } catch (e) {
      console.error(e)
      toast.error('Failed to refresh')
    } finally {
      setIsRefreshing(false)
    }
  }

  if (pipelineError) {
    return (
      <div className='p-8 text-center text-red-500'>
        Failed to load pipeline details.
      </div>
    )
  }

  const isLoading = isPipelineLoading || (!!pipeline && isSourceLoading)

  // Build destinations summary for header
  const destinationNames =
    pipeline?.destinations?.map((d) => d.destination?.name).filter(Boolean) ||
    []
  const destinationsSummary =
    destinationNames.length > 0
      ? destinationNames.length === 1
        ? destinationNames[0]
        : `${destinationNames.length} destinations`
      : 'No destinations'

  return (
    <>
      <Header fixed>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
          <ConfigDrawer />
        </div>
      </Header>

      <Main className='flex flex-1 flex-col gap-4'>
        {/* Redesigned Compact Header */}
        <div className='flex flex-col gap-1'>
          <Breadcrumb className='mb-1'>
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink asChild>
                  <Link to='/pipelines'>Pipelines</Link>
                </BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>
                  {pipeline?.name || 'Loading...'}
                </BreadcrumbPage>
              </BreadcrumbItem>
            </BreadcrumbList>
          </Breadcrumb>

          <div className='flex items-start justify-between gap-4'>
            <div className='space-y-1'>
              <h2 className='text-3xl font-bold tracking-tight dark:text-[#d5dae4]'>
                {isPipelineLoading ? (
                  <Skeleton className='h-9 w-64' />
                ) : (
                  pipeline?.name
                )}
              </h2>
              <div className='flex items-center gap-2 text-sm text-muted-foreground'>
                {isPipelineLoading ? (
                  <Skeleton className='h-4 w-48' />
                ) : (
                  <div className='inline-flex items-center gap-2 rounded-sm bg-secondary/50 px-3 py-1.5 text-xs font-medium text-[#7b828f] ring-1 ring-gray-500/10 ring-inset dark:bg-[#0f161d] dark:text-[#7b828f]'>
                    <div className='flex items-center gap-1.5 opacity-90 transition-opacity hover:opacity-100'>
                      <Database className='h-3.5 w-3.5' />
                      <span>{pipeline?.source?.name}</span>
                    </div>
                    <ArrowRight className='h-3 w-3 opacity-40' />
                    <div className='flex items-center gap-1.5 opacity-90 transition-opacity hover:opacity-100'>
                      <Database className='h-3.5 w-3.5' />
                      <span>{destinationsSummary}</span>
                    </div>
                  </div>
                )}
              </div>
            </div>
            <div className='flex items-center gap-3 pt-1'>
              {pipeline && <PipelineStatusSwitch pipeline={pipeline} />}
              <Button
                variant='outline'
                size='sm'
                onClick={handleRefresh}
                disabled={isRefreshing || isLoading}
                className='h-9'
              >
                <RefreshCcw
                  className={cn('mr-2 h-4 w-4', isRefreshing && 'animate-spin')}
                />
                Refresh
              </Button>
            </div>
          </div>
        </div>

        {/* Tabbed Content */}
        <CustomTabs defaultValue='flow-destination' className='w-full flex-1'>
          <CustomTabsList className='mb-4 w-full justify-start border-b'>
            <CustomTabsTrigger value='flow-destination'>
              <GitBranch className='mr-2 h-4 w-4' />
              Flow Destination
            </CustomTabsTrigger>
            <CustomTabsTrigger value='flow-data'>
              <Table2 className='mr-2 h-4 w-4' />
              Flow Data
            </CustomTabsTrigger>
            <CustomTabsTrigger value='backfill'>
              <RotateCcw className='mr-2 h-4 w-4' />
              Backfill Data
            </CustomTabsTrigger>
          </CustomTabsList>

          {/* Flow Destination Tab */}
          <CustomTabsContent value='flow-destination' className='mt-0'>
            {isPipelineLoading ? (
              <div className='flex h-125 items-center justify-center'>
                <Skeleton className='h-full w-full rounded-lg' />
              </div>
            ) : pipeline ? (
              <PipelineFlowTab pipeline={pipeline} />
            ) : (
              <div className='p-4 text-muted-foreground'>
                Pipeline not found.
              </div>
            )}
          </CustomTabsContent>

          {/* Flow Data Tab */}
          <CustomTabsContent value='flow-data' className='mt-0'>
            {isLoading ? (
              <div className='space-y-2'>
                <Skeleton className='h-10 w-full' />
                <Skeleton className='h-10 w-full' />
                <Skeleton className='h-10 w-full' />
              </div>
            ) : sourceDetails && pipeline ? (
              <PipelineDataFlow pipeline={pipeline} />
            ) : (
              <div className='p-4 text-muted-foreground'>
                No source details available.
              </div>
            )}
          </CustomTabsContent>

          {/* Backfill Data Tab */}
          <CustomTabsContent value='backfill' className='mt-0'>
            {isLoading ? (
              <div className='space-y-2'>
                <Skeleton className='h-10 w-full' />
                <Skeleton className='h-10 w-full' />
                <Skeleton className='h-10 w-full' />
              </div>
            ) : pipeline ? (
              <BackfillDataTab
                pipelineId={pipeline.id}
                sourceId={pipeline.source_id}
              />
            ) : (
              <div className='p-4 text-muted-foreground'>
                Pipeline not found.
              </div>
            )}
          </CustomTabsContent>
        </CustomTabs>
      </Main>
    </>
  )
}
