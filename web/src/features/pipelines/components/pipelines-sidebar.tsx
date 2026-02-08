import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
} from "@/components/ui/accordion"
import {
    HoverCard,
    HoverCardContent,
    HoverCardTrigger,
} from "@/components/ui/hover-card"
import { useQueries, useQuery, useQueryClient } from '@tanstack/react-query'
import { pipelinesRepo, Pipeline } from '@/repo/pipelines'
import { sourcesRepo, SourceDetailResponse } from '@/repo/sources'
import { Link, useParams, useNavigate } from '@tanstack/react-router'
import { Database, Table, Layers, Workflow, Loader2, Search, RefreshCw, X, FolderInput, FolderSync } from "lucide-react"
import { cn } from "@/lib/utils"
import { useState, useMemo, useEffect } from "react"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"

function HighlightedText({ text, highlight }: { text: string, highlight: string }) {
    if (!highlight.trim()) {
        return <span className="truncate">{text}</span>
    }

    const parts = text.split(new RegExp(`(${highlight})`, 'gi'))
    return (
        <span className="truncate">
            {parts.map((part, i) => (
                part.toLowerCase() === highlight.toLowerCase() ? (
                    <span key={i} className="bg-[#003e9b] text-white px-0.5 font-medium">
                        {part}
                    </span>
                ) : (
                    <span key={i}>{part}</span>
                )
            ))}
        </span>
    )
}

// -- Sub-components for clean recursion

function TableItem({ name, isActive, highlight, type, sourceTable }: {
    name: string,
    isActive?: boolean,
    highlight: string,
    database?: string,
    type?: 'source' | 'destination',
    sourceTable?: string
}) {
    return (
        <div className="relative group/table">
            <div className={cn(
                "absolute left-[-4px] top-1/2 w-4 h-px bg-border -translate-y-1/2",
                // "group-hover/table:bg-accent-foreground/50 transition-colors"
            )} />
            <HoverCard openDelay={100} closeDelay={200}>
                <HoverCardTrigger asChild>
                    <div className={cn(
                        "flex items-center gap-2 py-1 px-2 rounded-md text-sm text-muted-foreground dark:text-[#bec4d6] hover:bg-accent hover:text-accent-foreground cursor-pointer ml-3",
                        isActive && "bg-accent text-accent-foreground font-medium"
                    )}>
                        <Table className="h-3 w-3 shrink-0" />
                        <HighlightedText text={name.toUpperCase()} highlight={highlight} />
                    </div>
                </HoverCardTrigger>
                <HoverCardContent className="w-80" side="right" align="start">
                    <div className="space-y-2">
                        <div>
                            <h4 className="text-sm font-semibold mb-1">{type === 'source' ? 'Source Table' : 'Destination Table'}</h4>
                            <div className="space-y-1 text-xs">
                                <div className="flex items-start gap-2">
                                    <span className="text-muted-foreground min-w-[80px]">Table Name:</span>
                                    <span className="font-mono font-medium break-all">{name}</span>
                                </div>
                                {sourceTable && type === 'destination' && (
                                    <div className="flex items-start gap-2">
                                        <span className="text-muted-foreground min-w-[80px]">Source Table:</span>
                                        <span className="font-mono font-medium break-all">{sourceTable}</span>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                </HoverCardContent>
            </HoverCard>
        </div >
    )
}

function SourceTables({ tables, searchQuery }: { tables: any[], searchQuery: string }) {
    // Filter tables here
    const filteredTables = useMemo(() => {
        if (!searchQuery.trim()) return tables
        return tables.filter(t => t.table_name.toLowerCase().includes(searchQuery.toLowerCase()))
    }, [tables, searchQuery])

    if (!filteredTables?.length) {
        if (searchQuery) return null // Hide if no matches during search
        return <div className="ml-6 py-1 text-xs text-muted-foreground">No tables found</div>
    }

    return (
        <div className="flex flex-col gap-0.5 mt-1 border-l border-border ml-[22px] pl-1">
            {filteredTables.map((table) => (
                <TableItem
                    key={table.id}
                    name={table.table_name}
                    highlight={searchQuery}
                    type="source"
                />
            ))}
        </div>
    )
}

function PipelineItem({ pipeline, sourceDetails, checkExpanded, searchQuery }: { pipeline: Pipeline, sourceDetails?: SourceDetailResponse | null, checkExpanded?: string[], searchQuery: string }) {
    const sourceName = pipeline.source?.name || 'Source'
    const destinations = pipeline.destinations || []

    // Use passed source details or empty if not loaded yet
    const sourceTables = sourceDetails?.tables || []

    const [openItems, setOpenItems] = useState<string[]>([])

    useEffect(() => {
        if (checkExpanded && checkExpanded.length > 0) {
            setOpenItems(prev => Array.from(new Set([...prev, ...checkExpanded])))
        }
    }, [checkExpanded])

    // Filter destinations logic
    const filteredDestinations = useMemo(() => {
        if (!searchQuery.trim()) return destinations

        return destinations.filter(d => {
            // If dest name matches, keep it
            if (d.destination.name.toLowerCase().includes(searchQuery.toLowerCase())) return true

            // If any table matches, keep it
            if (d.table_syncs?.some(s => (s.table_name_target || s.table_name).toLowerCase().includes(searchQuery.toLowerCase()))) return true

            return false
        })
    }, [destinations, searchQuery])


    return (
        <div className="flex flex-col gap-1 pb-2">
            <Accordion type="multiple" className="w-full" value={openItems} onValueChange={setOpenItems}>
                {/* SOURCES FOLDER */}
                <AccordionItem value="sources" className="border-none">
                    <AccordionTrigger chevronPosition="left" className="justify-start py-1 px-2 gap-1.5 hover:bg-muted/50 hover:no-underline rounded-md text-sm font-medium dark:text-[#bec4d6]">
                        <div className="flex items-center gap-2">
                            <FolderInput className="h-4 w-4" />
                            <span>Sources</span>
                        </div>
                    </AccordionTrigger>
                    <AccordionContent className="pb-0 pt-0.5">
                        <Accordion type="multiple" className="w-full ml-2 border-l border-border/50 pl-2" value={openItems} onValueChange={setOpenItems}>
                            <AccordionItem value={`src-${pipeline.source_id}`} className="border-none">
                                <AccordionTrigger chevronPosition="left" className="justify-start py-1 px-2 gap-1.5 hover:bg-muted/50 hover:no-underline rounded-md text-sm dark:text-[#bec4d6]">
                                    <div className="flex items-center gap-2">
                                        <Database className="h-3.5 w-3.5" />
                                        <HighlightedText text={sourceName} highlight={searchQuery} />
                                    </div>
                                </AccordionTrigger>
                                <AccordionContent className="pb-0">
                                    <SourceTables
                                        tables={sourceTables}
                                        searchQuery={searchQuery}
                                    />
                                </AccordionContent>
                            </AccordionItem>
                        </Accordion>
                    </AccordionContent>
                </AccordionItem>

                {/* DESTINATIONS FOLDER */}
                <AccordionItem value="destinations" className="border-none">
                    <AccordionTrigger chevronPosition="left" className="justify-start py-1 px-2 gap-1.5 hover:bg-muted/50 hover:no-underline rounded-md text-sm font-medium dark:text-[#bec4d6]">
                        <div className="flex items-center gap-2">
                            <FolderSync className="h-4 w-4" />
                            <span>Destinations</span>
                        </div>
                    </AccordionTrigger>
                    <AccordionContent className="pb-0 pt-0.5">
                        <div className="flex flex-col gap-1 ml-2 border-l border-border/50 pl-2">
                            {filteredDestinations.length === 0 && (
                                <div className="text-xs text-muted-foreground px-2 py-1">No destinations found</div>
                            )}
                            {filteredDestinations.map(d => (
                                <Accordion key={d.id} type="multiple" className="w-full" value={openItems} onValueChange={setOpenItems}>
                                    <AccordionItem value={`dest-${d.id}`} className="border-none">
                                        <AccordionTrigger chevronPosition="left" className="justify-start py-1 px-2 gap-1.5 hover:bg-muted/50 hover:no-underline rounded-md text-sm dark:text-[#bec4d6]">
                                            <div className="flex items-center gap-2">
                                                <Layers className="h-3.5 w-3.5" />
                                                <HighlightedText text={d.destination.name} highlight={searchQuery} />
                                            </div>
                                        </AccordionTrigger>
                                        <AccordionContent className="pb-0">
                                            <div className="flex flex-col gap-0.5 mt-1 border-l border-border ml-[22px] pl-1">
                                                {d.table_syncs
                                                    ?.filter(sync => !searchQuery.trim() || (sync.table_name_target || sync.table_name).toLowerCase().includes(searchQuery.toLowerCase()))
                                                    ?.map(sync => (
                                                        <TableItem
                                                            key={sync.id}
                                                            name={sync.table_name_target || sync.table_name}
                                                            highlight={searchQuery}
                                                            type="destination"
                                                            sourceTable={sync.table_name}
                                                        />
                                                    ))}
                                                {(!d.table_syncs || d.table_syncs.length === 0) && (
                                                    <div className="ml-5 py-1 text-xs text-muted-foreground">No synced tables</div>
                                                )}
                                            </div>
                                        </AccordionContent>
                                    </AccordionItem>
                                </Accordion>
                            ))}
                        </div>
                    </AccordionContent>
                </AccordionItem>
            </Accordion>
        </div>
    )
}

export function PipelinesSidebar() {
    const { pipelineId } = useParams({ strict: false }) as { pipelineId?: string }
    const currentId = pipelineId ? parseInt(pipelineId) : null
    const navigate = useNavigate()
    const [searchQuery, setSearchQuery] = useState("")
    const [expandedItems, setExpandedItems] = useState<string[]>([])
    const [isManualRefreshing, setIsManualRefreshing] = useState(false)
    const queryClient = useQueryClient()

    // 1. Fetch Pipelines
    const { data: pipelinesData, isLoading: isLoadingPipelines, isError, isFetching } = useQuery({
        queryKey: ['pipelines'],
        queryFn: pipelinesRepo.getAll,
    })

    const pipelines = pipelinesData?.pipelines || []

    // 2. Fetch Source Details (Eager Loading)
    // Extract unique source IDs
    const sourceIds = useMemo(() => {
        return Array.from(new Set(pipelines.map(p => p.source_id))).filter(id => typeof id === 'number' && !isNaN(id));
    }, [pipelines])

    const sourceQueries = useQueries({
        queries: sourceIds.map(id => ({
            queryKey: ['source-details', id],
            queryFn: () => sourcesRepo.getDetails(id),
            staleTime: 1000 * 60 * 5, // Cache for 5 minutes
            retry: false, // Don't retry on failure to avoid spamming the server
        }))
    })

    // Create a map for easy access: sourceId -> details
    const sourceDetailsMap = useMemo(() => {
        const map = new Map<number, SourceDetailResponse>()
        sourceQueries.forEach((query, index) => {
            if (query.data) {
                map.set(sourceIds[index], query.data)
            }
        })
        return map
    }, [sourceQueries, sourceIds])


    // 3. Search Logic
    // We modify this to also return "internal expansion" maps per pipeline
    const { filteredPipelines, itemsToExpand, internalExpansionMap } = useMemo(() => {
        if (!searchQuery.trim()) {
            return { filteredPipelines: pipelines, itemsToExpand: [], internalExpansionMap: new Map<number, string[]>() }
        }

        const lowerQuery = searchQuery.toLowerCase()
        const expanded = new Set<string>()
        const internalMap = new Map<number, string[]>()

        const filtered = pipelines.filter(pipeline => {
            let isMatch = false
            const pId = `pipeline-${pipeline.id}`
            const internalIds = new Set<string>()

            // Check Pipeline Name
            if (pipeline.name.toLowerCase().includes(lowerQuery)) {
                isMatch = true
                // Pipeline name match doesn't force expansion of internals
            }

            // Check Source Name
            if (pipeline.source?.name.toLowerCase().includes(lowerQuery)) {
                isMatch = true
                expanded.add(pId)
                internalIds.add('sources')
            }

            // Check Source Tables
            const sourceDetails = sourceDetailsMap.get(pipeline.source_id)
            if (sourceDetails?.tables) {
                const matchingTables = sourceDetails.tables.filter(t => t.table_name.toLowerCase().includes(lowerQuery))
                if (matchingTables.length > 0) {
                    isMatch = true
                    expanded.add(pId)
                    internalIds.add('sources')
                    internalIds.add(`src-${pipeline.source_id}`)
                }
            }

            // Check Destinations & Destination Tables
            pipeline.destinations?.forEach(d => {
                const dId = d.id

                if (d.destination.name.toLowerCase().includes(lowerQuery)) {
                    isMatch = true
                    expanded.add(pId)
                    internalIds.add('destinations')
                    // internalIds.add(`dest-${dId}`) // Optional: expand destination itself if name matches? Maybe just folder.
                }

                // Check Destination Tables
                const matchingSyncs = d.table_syncs?.filter(s =>
                    (s.table_name_target || s.table_name).toLowerCase().includes(lowerQuery)
                )
                if (matchingSyncs && matchingSyncs.length > 0) {
                    isMatch = true
                    expanded.add(pId)
                    internalIds.add('destinations')
                    internalIds.add(`dest-${dId}`)
                }
            })

            if (isMatch) {
                internalMap.set(pipeline.id, Array.from(internalIds))
            }

            return isMatch
        })

        return {
            filteredPipelines: filtered,
            itemsToExpand: Array.from(expanded),
            internalExpansionMap: internalMap
        }
    }, [pipelines, searchQuery, sourceDetailsMap])

    // Update expanded items when search results change
    useEffect(() => {
        if (searchQuery.trim() && itemsToExpand.length > 0) {
            setExpandedItems(prev => {
                const prevSet = new Set(prev)
                const newItems = itemsToExpand.filter(id => !prevSet.has(id))

                if (newItems.length === 0) {
                    return prev
                }

                return [...prev, ...newItems]
            })
        }
    }, [itemsToExpand, searchQuery])

    // Auto-expand current pipeline
    useEffect(() => {
        if (currentId && !searchQuery) {
            setExpandedItems(prev => Array.from(new Set([...prev, `pipeline-${currentId}`])))
        }
    }, [currentId, searchQuery])


    if (isError) {
        return (
            <div className="p-4 text-sm text-destructive">
                Failed to load pipelines.
            </div>
        )
    }

    if (isLoadingPipelines) {
        return (
            <div className="p-4 flex items-center justify-center">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
            </div>
        )
    }

    return (
        <div className="h-full flex flex-col bg-sidebar border-r border-sidebar-border">
            {/* Header: Title & Branding */}
            <div className="px-4 pt-4 pb-0">
                <h1 className="text-xl font-bold text-foreground dark:text-[#bec4d6] mb-2">Pipelines Explorer</h1>
                <div className="flex items-center gap-2 mb-4">
                    <Workflow className="h-4 w-4 text-cyan-500" />
                    <span className="text-sm font-semibold text-cyan-500">
                        ROSETTA CATALOG
                    </span>
                </div>
            </div>
            {/* Pipelines Tab with Search */}
            <div className="border-border">
                <div className="px-4 pb-2">
                    <h2 className="text-sm font-semibold text-[#3581f2] inline-block relative pb-2">
                        Pipelines
                        <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-[#3581f2]" />
                    </h2>
                </div>
                <div className="p-3 pt-2">
                    <div className="flex items-center gap-2">
                        <div className="relative flex-1">
                            <Search className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
                            <Input
                                placeholder="Search"
                                className="h-8 pl-8 pr-8 text-xs bg-sidebar-accent/50 border-sidebar-border focus-visible:!border-[#3581f2] focus-visible:!ring-[#3581f2] focus-visible:!ring-1"
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                            />
                            {searchQuery && (
                                <button
                                    onClick={() => setSearchQuery("")}
                                    className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors z-10"
                                    title="Clear search"
                                    type="button"
                                >
                                    <X className="h-3.5 w-3.5" />
                                </button>
                            )}
                        </div>
                        <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 text-muted-foreground hover:text-foreground"
                            onClick={() => {
                                setIsManualRefreshing(true)
                                queryClient.invalidateQueries({ queryKey: ['pipelines'] })
                                queryClient.invalidateQueries({ queryKey: ['source-details'] })
                                setTimeout(() => setIsManualRefreshing(false), 800)
                            }}
                            title="Refresh pipelines"
                            disabled={isFetching || isManualRefreshing}
                        >
                            {isFetching || isManualRefreshing ? (
                                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                            ) : (
                                <RefreshCw className="h-3.5 w-3.5" />
                            )}
                        </Button>
                    </div>
                </div>
            </div>

            <ScrollArea className="flex-1">
                <div className="p-2">
                    {filteredPipelines.length === 0 && (
                        <div className="p-4 text-center text-xs text-muted-foreground">
                            {searchQuery ? `No results for "${searchQuery}"` : "No pipelines found"}
                        </div>
                    )}
                    <Accordion
                        type="multiple"
                        value={expandedItems}
                        onValueChange={setExpandedItems}
                        className="w-full"
                    >
                        {filteredPipelines.map(pipeline => (
                            <AccordionItem key={pipeline.id} value={`pipeline-${pipeline.id}`} className="border-none mb-1">
                                <div className="group relative">
                                    <AccordionTrigger
                                        chevronPosition="left"
                                        className={cn(
                                            "justify-start py-2 px-2 pr-8 gap-1.5 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground hover:no-underline rounded-md text-sm font-semibold flex-1",
                                            currentId === pipeline.id && "bg-[#002c6e] text-[#5999f7] hover:bg-[#002c6e] hover:text-[#5999f7]"
                                        )}
                                        onClick={(e) => {
                                            // If clicking on a non-active pipeline, navigate to it instead of expanding
                                            if (currentId !== pipeline.id) {
                                                e.preventDefault()
                                                e.stopPropagation()
                                                navigate({
                                                    to: '/pipelines/$pipelineId',
                                                    params: { pipelineId: pipeline.id.toString() }
                                                })
                                            }
                                            // If it's already active, let the accordion expand/collapse normally
                                        }}
                                    >
                                        <div className="flex items-center gap-2 overflow-hidden flex-1">
                                            <Workflow className={cn("h-4 w-4 shrink-0", currentId === pipeline.id ? "text-[#5999f7]" : "text-primary")} />
                                            <HighlightedText text={pipeline.name} highlight={searchQuery} />
                                        </div>
                                    </AccordionTrigger>
                                    <Link
                                        to="/pipelines/$pipelineId"
                                        params={{ pipelineId: pipeline.id.toString() }}
                                        className={cn(
                                            "absolute right-2 top-1/2 -translate-y-1/2 opacity-0 group-hover:opacity-100 p-1 rounded-md transition-opacity",
                                            currentId === pipeline.id
                                                ? "text-[#5999f7] hover:bg-white/20"
                                                : "text-muted-foreground hover:text-foreground hover:bg-sidebar-accent-foreground/5"
                                        )}
                                        title="Go to details"
                                    >
                                        <Workflow className="h-3 w-3" />
                                    </Link>
                                </div>
                                <AccordionContent className="pb-0 pt-1 pl-2">
                                    <div className="border-l border-border/40 pl-2">
                                        <PipelineItem
                                            pipeline={pipeline}
                                            sourceDetails={sourceDetailsMap.get(pipeline.source_id)}
                                            checkExpanded={internalExpansionMap.get(pipeline.id)}
                                            searchQuery={searchQuery}
                                        />
                                    </div>
                                </AccordionContent>
                            </AccordionItem>
                        ))}
                    </Accordion>
                </div>
            </ScrollArea>
        </div>
    )
}
