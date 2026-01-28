import { Handle, Position, NodeProps, type Node } from '@xyflow/react'
import { Database, Server } from 'lucide-react'
import { cn } from '@/lib/utils'

export interface PipelineNodeData extends Record<string, unknown> {
  label: string
  type: string
  isSource?: boolean
  status?: string
}

export function PipelineNode({ data }: NodeProps<Node<PipelineNodeData>>) {
  const isSource = data.isSource
  
  return (
    <div className={cn(
      "relative min-w-[200px] rounded-xl border-2 bg-background transition-all hover:shadow-lg",
      isSource 
        ? "border-blue-500/50 shadow-blue-500/20" 
        : "border-emerald-500/50 shadow-emerald-500/20"
    )}>
      {/* Header with Gradient */}
      <div className={cn(
        "flex items-center gap-2 rounded-t-[10px] px-4 py-3 text-white",
        isSource
          ? "bg-gradient-to-r from-blue-600 to-indigo-600"
          : "bg-gradient-to-r from-emerald-600 to-teal-600"
      )}>
        <Database className="h-4 w-4 shrink-0" />
        <span className="font-semibold text-sm truncate" title={data.label}>
          {data.label}
        </span>
      </div>

      {/* Body */}
      <div className="p-4">
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <div className="flex items-center gap-1.5">
            <Server className="h-3 w-3" />
            <span className="uppercase tracking-wider font-medium">
              {data.type}
            </span>
          </div>
          {data.status && (
            <div className="flex items-center gap-1.5">
               <span className={cn(
                 "h-2 w-2 rounded-full",
                 data.status === 'START' ? "bg-green-500" : "bg-yellow-500"
               )} />
               <span>{data.status}</span>
            </div>
          )}
        </div>
      </div>

      {/* Handles */}
      {isSource ? (
        <Handle
          type="source"
          position={Position.Right}
          className="!bg-blue-600 !h-3 !w-3 !-right-2 border-2 border-white"
        />
      ) : (
        <Handle
          type="target"
          position={Position.Left}
          className="!bg-emerald-600 !h-3 !w-3 !-left-2 border-2 border-white"
        />
      )}
    </div>
  )
}
