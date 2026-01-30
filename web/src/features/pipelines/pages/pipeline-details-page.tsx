import { useParams, Link } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { pipelinesRepo } from '@/repo/pipelines'
import { sourcesRepo } from '@/repo/sources'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ConfigDrawer } from '@/components/config-drawer'
import { Skeleton } from '@/components/ui/skeleton'
import { PipelineFlowTab } from '@/features/pipelines/components/pipeline-flow-tab'
import { PipelineDataFlow } from '@/features/pipelines/components/pipeline-data-flow'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { RefreshCcw, GitBranch, Table2 } from 'lucide-react'
import { toast } from 'sonner'
import { useState } from 'react'
import { cn } from '@/lib/utils'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'

export default function PipelineDetailsPage() {
    const { pipelineId } = useParams({ from: '/_authenticated/pipelines/$pipelineId' })
    const id = parseInt(pipelineId)
    const [isRefreshing, setIsRefreshing] = useState(false)

    // 1. Fetch Pipeline
    const { data: pipeline, isLoading: isPipelineLoading, error: pipelineError } = useQuery({
        queryKey: ['pipeline', id],
        queryFn: async () => {
            return (await pipelinesRepo.get(id))
        },
        retry: false
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
            toast.success("Pipeline and Source refreshed")
        } catch (e) {
            console.error(e)
            toast.error("Failed to refresh")
        } finally {
            setIsRefreshing(false)
        }
    }

    if (pipelineError) {
        return <div className="p-8 text-center text-red-500">Failed to load pipeline details.</div>
    }

    const isLoading = isPipelineLoading || (!!pipeline && isSourceLoading)

    // Build destinations summary for header
    const destinationNames = pipeline?.destinations?.map(d => d.destination?.name).filter(Boolean) || []
    const destinationsSummary = destinationNames.length > 0 
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

            <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
                <Breadcrumb>
                  <BreadcrumbList>
                    <BreadcrumbItem>
                      <BreadcrumbLink asChild>
                        <Link to="/pipelines">Pipelines</Link>
                      </BreadcrumbLink>
                    </BreadcrumbItem>
                    <BreadcrumbSeparator />
                    <BreadcrumbItem>
                      <BreadcrumbPage>{pipeline?.name || 'Loading...'}</BreadcrumbPage>
                    </BreadcrumbItem>
                  </BreadcrumbList>
                </Breadcrumb>

                {/* Header Section */}
                <div className="flex items-center gap-4">
                    <div className="space-y-1">
                        <h2 className='text-2xl font-bold tracking-tight'>
                            {isPipelineLoading ? <Skeleton className="h-8 w-48" /> : pipeline?.name}
                        </h2>
                        <div className="flex items-center gap-2 text-muted-foreground text-sm">
                            {isPipelineLoading ? <Skeleton className="h-4 w-32" /> : (
                                <>
                                    <span>{pipeline?.source?.name}</span>
                                    <span>→</span>
                                    <span>{destinationsSummary}</span>
                                </>
                            )}
                        </div>
                    </div>
                    <div className="ml-auto">
                        <Button
                            variant="outline"
                            size="sm"
                            onClick={handleRefresh}
                            disabled={isRefreshing || isLoading}
                        >
                            <RefreshCcw className={cn("h-4 w-4 mr-2", isRefreshing && "animate-spin")} />
                            Refresh
                        </Button>
                    </div>
                </div>

                {/* Tabbed Content */}
                <Tabs defaultValue="flow-destination" className="flex-1">
                    <TabsList>
                        <TabsTrigger value="flow-destination">
                            <GitBranch className="h-4 w-4 mr-2" />
                            Flow Destination
                        </TabsTrigger>
                        <TabsTrigger value="flow-data">
                            <Table2 className="h-4 w-4 mr-2" />
                            Flow Data
                        </TabsTrigger>
                    </TabsList>

                    {/* Flow Destination Tab */}
                    <TabsContent value="flow-destination" className="mt-4">
                        {isPipelineLoading ? (
                            <div className="h-[500px] flex items-center justify-center">
                                <Skeleton className="h-full w-full rounded-lg" />
                            </div>
                        ) : pipeline ? (
                            <PipelineFlowTab pipeline={pipeline} />
                        ) : (
                            <div className="p-4 text-muted-foreground">Pipeline not found.</div>
                        )}
                    </TabsContent>

                    {/* Flow Data Tab */}
                    <TabsContent value="flow-data" className="mt-4">
                        {isLoading ? (
                            <div className="space-y-2">
                                <Skeleton className="h-10 w-full" />
                                <Skeleton className="h-10 w-full" />
                                <Skeleton className="h-10 w-full" />
                            </div>
                        ) : sourceDetails && pipeline ? (
                            <PipelineDataFlow
                                pipeline={pipeline}
                                sourceDetails={sourceDetails}
                            />
                        ) : (
                            <div className="p-4 text-muted-foreground">No source details available.</div>
                        )}
                    </TabsContent>
                </Tabs>
            </Main>
        </>
    )
}
