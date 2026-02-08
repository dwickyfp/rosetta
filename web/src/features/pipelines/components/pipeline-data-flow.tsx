import { Pipeline, pipelinesRepo } from '@/repo/pipelines'
import { ReactFlow, Background, Controls, Node, Edge, Position, MarkerType, Handle } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useQuery } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { useTheme } from '@/context/theme-provider'
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription } from '@/components/ui/sheet'
import { PipelineDetailsTable } from './pipeline-details-table'
import { SourceTableInfo } from '@/repo/sources'
import { Database, Layers } from 'lucide-react'
import { Card, CardContent, CardHeader } from '@/components/ui/card'

// Custom Node Component for consistent styling
const CustomNode = ({ data }: { data: any }) => {
    const isSource = data.isSource;
    const isDestGroup = data.isDestGroup;

    if (isSource) {
        return (
            <div className="relative">
                <Handle type="target" position={Position.Left} className="!bg-slate-400 !w-3 !h-3" />
                <Card className="min-w-[280px] max-w-[400px] shadow-sm border-l-4 border-l-blue-500">
                    <CardHeader className="p-3 pb-2">
                        <div className="flex items-center gap-2">
                            <Database className="w-4 h-4 text-blue-500 shrink-0" />
                            <div className="font-semibold text-sm leading-tight truncate" title={data.label}>
                                {data.label}
                            </div>
                        </div>
                    </CardHeader>
                </Card>
                <Handle type="source" position={Position.Right} className="!bg-slate-400 !w-3 !h-3" />
            </div>
        )
    }

    if (isDestGroup) {
        return (
            <div className="relative">
                <Handle type="target" position={Position.Left} className="!bg-slate-400 !w-2 !h-2" />
                <Card className="min-w-[200px] max-w-[300px] shadow-sm hover:shadow-md transition-shadow border-l-4 border-l-purple-500 bg-card">
                    <CardHeader className="p-3 pb-2">
                        <div className="flex items-center gap-2">
                            <Layers className="w-4 h-4 text-purple-500 shrink-0" />
                            <div className="font-semibold text-sm leading-tight truncate" title={data.label}>
                                {data.label}
                            </div>
                        </div>
                    </CardHeader>
                </Card>
                <Handle type="source" position={Position.Right} className="!bg-slate-400 !w-2 !h-2" />
            </div>
        )
    }

    // Compact Target Node
    return (
        <div className="relative">
            <Handle type="target" position={Position.Left} className="!bg-slate-400 !w-2 !h-2" />

            <Card className="min-w-[180px] max-w-[250px] shadow-sm hover:shadow-md transition-shadow border-l-2 border-l-emerald-500">
                <CardContent className="p-2 flex flex-col gap-1">
                    <div className="flex items-center gap-2">
                        <Database className="w-3 h-3 text-emerald-500 shrink-0" />
                        <div className="font-semibold text-xs leading-tight truncate" title={data.label}>
                            {data.label}
                        </div>
                    </div>
                    <div className="flex justify-between items-center mt-1 pt-1 border-t border-border">
                        <span className="text-[10px] text-muted-foreground">Records</span>
                        <span className="font-mono text-xs font-bold text-emerald-600">{data.totalCount?.toLocaleString()}</span>
                    </div>
                </CardContent>
            </Card>

            <Handle type="source" position={Position.Right} className="!bg-slate-400 !w-2 !h-2" />
        </div>
    )
}

const nodeTypes = {
    custom: CustomNode
}

interface PipelineDataFlowProps {
    pipeline: Pipeline
    sourceDetails?: { tables: SourceTableInfo[] }
}

export function PipelineDataFlow({ pipeline, sourceDetails }: PipelineDataFlowProps) {
    const { theme } = useTheme()
    const [selectedDestId, setSelectedDestId] = useState<number | null>(null)
    const [isSheetOpen, setIsSheetOpen] = useState(false)

    // Fetch stats to calculate edge labels
    const { data: stats } = useQuery({
        queryKey: ['pipeline-stats', pipeline.id],
        queryFn: () => pipelinesRepo.getStats(pipeline.id),
        refetchInterval: 5000
    })

    // Helper to sum today's stats
    const calcTotal = (dailyStats: any[]) => {
        if (!dailyStats) return 0
        const today = new Date().toLocaleDateString('en-CA')
        const entry = dailyStats.find((d: any) => d.date.startsWith(today))
        return entry ? entry.count : 0
    }

    // Calculate totals & build lineage graph
    const { nodes, edges } = useMemo(() => {
        const nodes: Node[] = []
        const edges: Edge[] = []

        if (!pipeline || !stats) return { nodes, edges }

        // Constants
        const X_ROOT = 50
        const X_SOURCE = 400
        const X_DEST_GROUP = 800
        const X_TARGET = 1200
        const ROW_HEIGHT = 150

        // Group stats by Source Table
        const flowMap = new Map<string, typeof stats>()
        stats.forEach(s => {
            const list = flowMap.get(s.table_name) || []
            list.push(s)
            flowMap.set(s.table_name, list)
        })

        let currentY = 50

        // 1. Source DB Node (Root) - Position usually centered relative to everything, 
        // but for simplicity, let's just place it top-left or centered if we calculate total height.
        // We'll fix it at the top for now, or maybe vertically center it later.
        // Let's place it at top-left.
        nodes.push({
            id: 'source-root',
            type: 'custom',
            position: { x: X_ROOT, y: currentY },
            data: {
                label: pipeline.source?.name || 'Source DB',
                isSource: true
            },
            sourcePosition: Position.Right,
            selectable: false, // Disable selection
        })

        // Track Root Y center for "Source DB" edge connections if we wanted to center root.
        // But let's just flow downwards.

        // Iterate through each Source Table Group
        flowMap.forEach((targets, sourceTableName) => {
            // Group targets by Destination ID
            const destGroups = new Map<number, typeof targets>()
            targets.forEach(t => {
                if (!t.pipeline_destination_id) return
                const list = destGroups.get(t.pipeline_destination_id) || []
                list.push(t)
                destGroups.set(t.pipeline_destination_id, list)
            })

            // Calculate total height for this Source Group based on all destinations
            let groupTotalRows = 0
            destGroups.forEach(groupTargets => {
                groupTotalRows += Math.max(1, groupTargets.length)
            })
            const sourceGroupHeight = groupTotalRows * ROW_HEIGHT

            // Source Node Position (Vertically centered for this entire group)
            const sourceY = currentY + (sourceGroupHeight / 2) - (ROW_HEIGHT / 2)

            const sourceNodeId = `src-tbl-${sourceTableName}`
            nodes.push({
                id: sourceNodeId,
                type: 'custom',
                position: { x: X_SOURCE, y: sourceY },
                data: {
                    label: sourceTableName,
                    isSource: true
                },
                sourcePosition: Position.Right,
                targetPosition: Position.Left,
                selectable: false, // Disable selection
            })

            // Edge Root -> Source
            edges.push({
                id: `e-root-${sourceTableName}`,
                source: 'source-root',
                target: sourceNodeId,
                style: { stroke: '#cbd5e1', strokeWidth: 2, strokeDasharray: '5,5' },
                type: 'smoothstep',
                animated: true,
            })

            // Iterate through Destination Groups to place Dest Nodes & Target Nodes
            let currentDestY = currentY
            destGroups.forEach((groupTargets, destId) => {
                const destRowCount = Math.max(1, groupTargets.length)
                const destGroupHeight = destRowCount * ROW_HEIGHT

                // Destination Node Position (Centered for its targets)
                const destY = currentDestY + (destGroupHeight / 2) - (ROW_HEIGHT / 2)
                const destName = groupTargets[0].destination_name || `Dest ${destId}`
                const destNodeId = `dst-group-${sourceTableName}-${destId}`

                nodes.push({
                    id: destNodeId,
                    type: 'custom',
                    position: { x: X_DEST_GROUP, y: destY },
                    data: {
                        label: destName,
                        isDestGroup: true,
                        isSource: false
                    },
                    sourcePosition: Position.Right,
                    targetPosition: Position.Left
                })

                // Edge Source -> Destination
                edges.push({
                    id: `e-${sourceTableName}-${destNodeId}`,
                    source: sourceNodeId,
                    target: destNodeId,
                    type: 'smoothstep',
                    animated: true,
                    style: { stroke: '#94a3b8', strokeWidth: 1.5, strokeDasharray: '4,4' },
                    markerEnd: { type: MarkerType.ArrowClosed, color: '#94a3b8' },
                })

                // Place Target Nodes for this Destination
                groupTargets.forEach((stat, idx) => {
                    const targetTableName = stat.target_table_name || stat.table_name
                    // Unique ID for target node
                    const uniqueIdSuffix = stat.pipeline_destination_table_sync_id
                        ? `sync-${stat.pipeline_destination_table_sync_id}`
                        : `${stat.pipeline_destination_id}-${targetTableName}`

                    const targetNodeId = `dst-tbl-${uniqueIdSuffix}`

                    // Target Y position 
                    const targetY = currentDestY + (idx * ROW_HEIGHT)

                    nodes.push({
                        id: targetNodeId,
                        type: 'custom',
                        position: { x: X_TARGET, y: targetY },
                        data: {
                            label: targetTableName,
                            subLabel: destName,
                            isSource: false,
                            totalCount: calcTotal(stat.daily_stats),
                            destId: stat.pipeline_destination_id
                        },
                        targetPosition: Position.Left,
                    })

                    // Edge Destination -> Target
                    edges.push({
                        id: `e-${destNodeId}-${targetNodeId}`,
                        source: destNodeId,
                        target: targetNodeId,
                        type: 'smoothstep',
                        animated: true,
                        style: { stroke: '#64748b', strokeWidth: 1.5 },
                        label: `${calcTotal(stat.daily_stats).toLocaleString()}`,
                        labelStyle: { fill: 'var(--foreground)', fontWeight: 600, fontSize: 10 },
                        labelBgStyle: { fill: 'var(--card)', fillOpacity: 0.9 },
                        markerEnd: { type: MarkerType.ArrowClosed, color: '#64748b', width: 15, height: 15 },
                    })
                })

                // Move Y cursor for next destination group
                currentDestY += destGroupHeight
            })

            // Advance Main Y for next Source Group + padding
            currentY += sourceGroupHeight + 40
        })

        return { nodes, edges }
    }, [pipeline, stats])

    const onNodeClick = (_: any, node: Node) => {
        if (node.id.startsWith('dst-tbl-')) {
            const destId = node.data.destId as number
            setSelectedDestId(destId)
            setIsSheetOpen(true)
        }
    }

    const selectedDestName = useMemo(() => {
        return pipeline.destinations?.find(d => d.id === selectedDestId)?.destination.name
    }, [selectedDestId, pipeline])

    return (
        <div className="h-[700px] border rounded-lg bg-background relative [&_.react-flow__controls]:bg-background [&_.react-flow__controls]:border-border [&_.react-flow__controls]:shadow-md [&_.react-flow__controls-button]:bg-background [&_.react-flow__controls-button]:border-border [&_.react-flow__controls-button]:fill-foreground [&_.react-flow__controls-button:hover]:bg-muted">
            <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                colorMode={theme}
                onNodeClick={onNodeClick}
                fitView
            >
                <Background className="!bg-background" color="var(--border)" />
                <Controls />
            </ReactFlow>

            <Sheet open={isSheetOpen} onOpenChange={setIsSheetOpen}>
                <SheetContent side="right" className="min-w-[800px] sm:w-[800px] overflow-y-auto">
                    <SheetHeader className="mb-6">
                        <SheetTitle>Lineage Details: {selectedDestName}</SheetTitle>
                        <SheetDescription>
                            Records flowing to {selectedDestName}.
                        </SheetDescription>
                    </SheetHeader>

                    {selectedDestId && sourceDetails && (
                        <PipelineDetailsTable
                            pipelineId={pipeline.id}
                            tables={sourceDetails.tables}
                            destinationId={selectedDestId}
                        />
                    )}
                </SheetContent>
            </Sheet>
        </div>
    )
}
