import { Handle, Position, NodeProps, type Node } from '@xyflow/react'
import { Database, Server, Trash, Info } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { pipelinesRepo } from '@/repo/pipelines'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'

export interface PipelineNodeData extends Record<string, unknown> {
  label: string
  type: string
  isSource?: boolean
  status?: string
  pipelineId?: number
  destinationId?: number
  isError?: boolean
  errorMessage?: string
  errorCount?: number
}

export function PipelineNode({ data }: NodeProps<Node<PipelineNodeData>>) {
  const isSource = data.isSource
  const isError = data.isError
  const queryClient = useQueryClient()

  const { mutate: deleteDestination, isPending: isDeleting } = useMutation({
    mutationFn: async () => {
      if (!data.pipelineId || !data.destinationId) return
      return pipelinesRepo.removeDestination(data.pipelineId, data.destinationId)
    },
    onSuccess: () => {
      toast.success('Destination removed')
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      queryClient.invalidateQueries({ queryKey: ['pipeline', data.pipelineId] })
    },
    onError: (error) => {
      toast.error(`Failed to remove destination: ${error}`)
    }
  })


  return (
    <div className={cn(
      "relative min-w-[200px] rounded-xl border-2 bg-background dark:bg-[#1d252f] transition-all hover:shadow-lg group",
      isSource
        ? "border-blue-500/50 shadow-blue-500/20"
        : isError
          ? "border-red-500 shadow-red-500/30 animate-error-pulse"
          : "border-emerald-500/50 shadow-emerald-500/20"
    )}>
      {/* Error glow effect */}
      {isError && !isSource && (
        <div className="absolute inset-0 rounded-xl bg-red-500/10 animate-pulse pointer-events-none" />
      )}

      {/* Header with Gradient */}
      <div className={cn(
        "flex items-center justify-between rounded-t-[10px] px-4 py-3 text-white",
        isSource
          ? "bg-gradient-to-r from-blue-600 to-indigo-600"
          : isError
            ? "bg-gradient-to-r from-red-600 to-rose-600"
            : "bg-gradient-to-r from-emerald-600 to-teal-600"
      )}>
        <div className="flex items-center gap-2 overflow-hidden">
          <Database className="h-4 w-4 shrink-0" />
          <span className="font-semibold text-sm truncate" title={data.label}>
            {data.label}
          </span>
        </div>

        {!isSource && (
          <div className="flex items-center gap-1">
            {/* Error Info Popover */}
            {isError && data.errorMessage && (
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 text-white/70 hover:text-white hover:bg-white/20"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <Info className="h-3 w-3" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent
                  className="w-80 max-h-60 overflow-auto"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="space-y-2">
                    <h4 className="font-medium text-destructive flex items-center gap-2">
                      <Info className="h-4 w-4" />
                      Error Details
                    </h4>
                    <p className="text-sm text-muted-foreground whitespace-pre-wrap break-words">
                      {data.errorMessage}
                    </p>
                  </div>
                </PopoverContent>
              </Popover>
            )}

            {/* Delete Button */}
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6 text-white/70 hover:text-white hover:bg-white/20 -mr-2"
                  onClick={(e) => e.stopPropagation()}
                  disabled={isDeleting}
                >
                  <Trash className="h-3 w-3" />
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent onClick={(e) => e.stopPropagation()}>
                <AlertDialogHeader>
                  <AlertDialogTitle>Delete Destination</AlertDialogTitle>
                  <AlertDialogDescription>
                    Are you sure you want to remove this destination from the pipeline? This action cannot be undone.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel onClick={(e) => e.stopPropagation()}>Cancel</AlertDialogCancel>
                  <AlertDialogAction
                    className="bg-destructive text-white hover:bg-destructive/90"
                    onClick={(e) => {
                      e.stopPropagation()
                      deleteDestination()
                    }}
                  >
                    Delete
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        )}
      </div>

      {/* Body */}
      <div className="p-4">
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <div className="flex items-center gap-1.5">
            <Server className="h-3 w-3" />
            <span className="uppercase tracking-wider font-medium">
              {data.type}
            </span>
            {data.errorCount !== undefined && data.errorCount > 0 && (
              <div className="flex items-center gap-1.5 ml-2 bg-red-100 text-red-600 px-2 py-0.5 rounded-full border border-red-200">
                <span className="font-bold text-[10px]">{data.errorCount}</span>
                <span className="text-[10px] font-medium">Error</span>
              </div>
            )}
          </div>
          {data.status && (
            <div className="flex items-center gap-1.5">
              <span className={cn(
                "h-2 w-2 rounded-full",
                isError ? "bg-red-500 animate-pulse" : data.status === 'START' ? "bg-green-500" : "bg-yellow-500"
              )} />
              <span>{isError ? 'ERROR' : data.status}</span>
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
          className={cn(
            "!h-3 !w-3 !-left-2 border-2 border-white",
            isError ? "!bg-red-600" : "!bg-emerald-600"
          )}
        />
      )}
    </div>
  )
}
