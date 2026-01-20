import { useParams, useNavigate } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { pipelinesRepo } from '@/repo/pipelines'
import { sourcesRepo } from '@/repo/sources'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ConfigDrawer } from '@/components/config-drawer'
import { Skeleton } from '@/components/ui/skeleton'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { SourceDetailsTablesList } from '@/features/sources/components/source-details-tables-list'
import { Button } from '@/components/ui/button'
import { ArrowLeft, RefreshCcw } from 'lucide-react'
import { toast } from 'sonner'
import { useState } from 'react'
import { cn } from '@/lib/utils'

export default function PipelineDetailsPage() {
    // Correctly accessing params using the route ID from routeTree.gen.ts
    const { pipelineId } = useParams({ from: '/_authenticated/pipelines/$pipelineId' })
    const id = parseInt(pipelineId)
    const navigate = useNavigate()
    const [isRefreshing, setIsRefreshing] = useState(false)

    // 1. Fetch Pipeline to get source_id
    const { data: pipeline, isLoading: isPipelineLoading, error: pipelineError } = useQuery({
        queryKey: ['pipeline', id],
        queryFn: async () => {
            // We might need a direct get endpoint or filter from list if get isn't available
            // Assuming pipelinesRepo has a get or we can implement it.
            // If strictly following existing repo, we might need to use getAll and find?
            // Let's check pipelinesRepo again. It has pipelinesRepo.getAll but usually we need get one.
            // Wait, I recall seeing get_pipeline in backend but checking repo file again is safer.
            // For now assuming we can add or it exists.
            // Actually, the backend HAS get_pipeline. I should verify if frontend repo has it.
            // If not, I'll add it to the repo file in next step or now?
            // I'll assume I can add it if missing.
            // But to be safe, I'll check repo content in previous turns...
            // Previous turns show pipelinesRepo has: getAll, create, delete, start, pause.
            // It does NOT have get(id). I need to add it.
            // For now I will write this component assuming I will update repo.
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
            // Maybe refresh pipeline status too?
            // await pipelinesRepo.refresh(id) 
            // Backend has refresh_pipeline endpoint.
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
                <div className="flex items-center gap-4">
                    <Button variant="ghost" size="icon" onClick={() => navigate({ to: '/pipelines' })}>
                        <ArrowLeft className="h-4 w-4" />
                    </Button>
                    <div className="space-y-1">
                        <h2 className='text-2xl font-bold tracking-tight'>
                            {isPipelineLoading ? <Skeleton className="h-8 w-48" /> : pipeline?.name}
                        </h2>
                        <div className="flex items-center gap-2 text-muted-foreground text-sm">
                            {isPipelineLoading ? <Skeleton className="h-4 w-32" /> : (
                                <>
                                    <span>{pipeline?.source?.name}</span>
                                    <span>→</span>
                                    <span>{pipeline?.destination?.name}</span>
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

                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                    <Card>
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">Status</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="text-2xl font-bold">
                                {isLoading ? <Skeleton className="h-8 w-20" /> : (
                                    <Badge variant={pipeline?.status === 'START' ? 'default' : 'secondary'}>
                                        {pipeline?.status === 'START' ? 'RUNNING' : pipeline?.status}
                                    </Badge>
                                )}
                            </div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">Progress</CardTitle>
                        </CardHeader>
                        <CardContent>
                            {isLoading ? <Skeleton className="h-8 w-full" /> : (
                                <div className="space-y-1">
                                    <div className="text-2xl font-bold">{pipeline?.pipeline_progress?.progress || 0}%</div>
                                    <p className="text-xs text-muted-foreground">{pipeline?.pipeline_progress?.status}</p>
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </div>

                <Separator />

                <div className="space-y-4">
                    <h3 className="text-lg font-medium">Table Replication Status</h3>
                    {isLoading ? (
                        <div className="space-y-2">
                            <Skeleton className="h-10 w-full" />
                            <Skeleton className="h-10 w-full" />
                            <Skeleton className="h-10 w-full" />
                        </div>
                    ) : sourceDetails ? (
                        <SourceDetailsTablesList
                            sourceId={pipeline!.source_id}
                            pipelineId={pipeline!.id}
                            tables={sourceDetails.tables}
                            readOnly={true}
                        />
                    ) : (
                        <div className="p-4 text-muted-foreground">No source details available.</div>
                    )}
                </div>
            </Main>
        </>
    )
}
