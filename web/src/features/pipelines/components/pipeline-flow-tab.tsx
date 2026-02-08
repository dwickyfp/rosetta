import { Button } from '@/components/ui/button'
import { Pipeline } from '@/repo/pipelines'
import { ReactFlow, Background, Controls, Node, Edge, Position } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Plus } from 'lucide-react'
import { useState, useMemo, useCallback } from 'react'
import { useTheme } from '@/context/theme-provider'
import { AddDestinationModal } from './add-destination-modal'
import { PipelineNode, PipelineNodeData } from './pipeline-node'
import { SourceTableDrawer } from './source-table-drawer'

interface PipelineFlowTabProps {
  pipeline: Pipeline
}

// Register custom node types
const nodeTypes = {
  pipelineNode: PipelineNode,
}

export function PipelineFlowTab({ pipeline }: PipelineFlowTabProps) {
  const { theme } = useTheme()
  const [openAddDest, setOpenAddDest] = useState(false)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedDestId, setSelectedDestId] = useState<number | null>(null)

  // Determine existing destination IDs to exclude from add modal
  const existingDestinationIds = useMemo(() => {
    const ids = new Set<number>()
    pipeline?.destinations?.forEach((d) => ids.add(d.destination.id))
    return ids
  }, [pipeline])

  // Handle node click - open drawer when destination node is clicked
  const onNodeClick = useCallback((_event: React.MouseEvent, node: Node<PipelineNodeData>) => {
    if (node.data.isSource) {
      // Disabled click on source node
      return
    } else if (node.id.startsWith('dest-')) {
      const destId = parseInt(node.id.replace('dest-', ''))
      // Find the pipeline_destination ID from the destination ID
      const pipelineDest = pipeline?.destinations?.find((d) => d.destination.id === destId)

      if (pipelineDest) {
        setSelectedDestId(pipelineDest.id)
        setDrawerOpen(true)
      }
    }
  }, [pipeline])

  // Build nodes and edges for React Flow
  const { nodes, edges } = useMemo(() => {
    if (!pipeline) return { nodes: [], edges: [] }

    const nodes: Node<PipelineNodeData>[] = []
    const edges: Edge[] = []

    // Source Node
    if (pipeline.source) {
      nodes.push({
        id: 'source',
        type: 'pipelineNode', // Use custom type
        position: { x: 50, y: 150 },
        data: {
          label: pipeline.source.name,
          type: 'POSTGRESQL', // Assuming source is always Postgres for now
          isSource: true,
          status: pipeline.status
        },
        sourcePosition: Position.Right,
        selectable: false, // Disable selection visual
      })
    }

    // Destination Nodes
    const destinationCount = pipeline.destinations?.length || 0
    // Center destinations vertically relative to source
    const totalHeight = (destinationCount * 120) // approx height per node
    const startY = 150 - (totalHeight / 2) + 60 // Adjust to center

    pipeline.destinations?.forEach((d, index) => {
      const destId = `dest-${d.destination.id}`
      nodes.push({
        id: destId,
        type: 'pipelineNode', // Use custom type
        position: { x: 500, y: (destinationCount > 1 ? startY + index * 140 : 150) },
        data: {
          label: d.destination.name,
          type: d.destination.type,
          isSource: false,
          pipelineId: pipeline.id,
          destinationId: d.destination.id,
          isError: d.is_error,
          errorMessage: d.error_message ?? undefined,
          errorCount: d.table_syncs?.filter(t => t.is_error).length || 0
        },
        targetPosition: Position.Left,
      })

      // Edge from source to destination
      edges.push({
        id: `e-source-${d.destination.id}`,
        source: 'source',
        target: destId,
        animated: true,
        style: {
          stroke: '#6366f1',
          strokeWidth: 2,
        },
      })
    })

    return { nodes, edges }
  }, [pipeline])

  return (
    <div className="flex h-[500px] flex-col rounded-lg border bg-background">
      {/* Toolbar */}
      <div className="flex items-center justify-between border-b px-4 py-3">
        <div className="text-sm text-muted-foreground">
          Source to destination data flow
        </div>
        <Button size="sm" onClick={() => setOpenAddDest(true)}>
          <Plus className="mr-2 h-4 w-4" />
          Add Destination
        </Button>
      </div>

      {/* React Flow Canvas */}
      <div className="flex-1 [&_.react-flow__controls]:bg-background [&_.react-flow__controls]:border-border [&_.react-flow__controls]:shadow-md [&_.react-flow__controls-button]:bg-background [&_.react-flow__controls-button]:border-border [&_.react-flow__controls-button]:fill-foreground [&_.react-flow__controls-button:hover]:bg-muted">
        <ReactFlow<Node<PipelineNodeData>>
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes} // Register custom types
          onNodeClick={onNodeClick}
          colorMode={theme}
          fitView
          fitViewOptions={{ padding: 0.3 }}
          attributionPosition="bottom-right"
          proOptions={{ hideAttribution: true }}
        >
          <Background className="!bg-background" color="var(--border)" gap={16} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>

      {/* Add Destination Modal */}
      <AddDestinationModal
        open={openAddDest}
        setOpen={setOpenAddDest}
        pipelineId={pipeline.id}
        existingDestinationIds={existingDestinationIds}
      />

      {/* Source Table Drawer */}
      <SourceTableDrawer
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        pipeline={pipeline}
        initialDestinationId={selectedDestId}
      />
    </div>
  )
}
