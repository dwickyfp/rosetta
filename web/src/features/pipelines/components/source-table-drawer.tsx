import { useState, useEffect } from 'react'
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from '@/components/ui/sheet'
import { Pipeline, TableWithSyncInfo, tableSyncRepo } from '@/repo/pipelines'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { PostgresTableConfig } from '@/features/pipelines/components/postgres-table-config'
import { SnowflakeTableConfig } from '@/features/pipelines/components/snowflake-table-config'
import { Loader2 } from 'lucide-react'

interface SourceTableDrawerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  pipeline: Pipeline
  initialDestinationId?: number | null
}

export function SourceTableDrawer({
  open,
  onOpenChange,
  pipeline,
  initialDestinationId,
}: SourceTableDrawerProps) {
  const [selectedDestinationId, setSelectedDestinationId] = useState<number | null>(null)
  const [tables, setTables] = useState<TableWithSyncInfo[]>([])
  const [loading, setLoading] = useState(false)

  // Get destinations
  const destinations = pipeline.destinations || []

  // Set first destination as default or use initialDestinationId
  useEffect(() => {
    if (open) {
      if (initialDestinationId) {
        setSelectedDestinationId(initialDestinationId)
      } else if (destinations.length > 0 && !selectedDestinationId) {
        setSelectedDestinationId(destinations[0].id)
      }
    }
  }, [open, destinations, selectedDestinationId, initialDestinationId])

  // Load tables when destination is selected
  useEffect(() => {
    if (open && selectedDestinationId) {
      loadTables()
    }
  }, [open, selectedDestinationId])

  const loadTables = async () => {
    if (!selectedDestinationId) return
    
    setLoading(true)
    try {
      const data = await tableSyncRepo.getDestinationTables(
        pipeline.id,
        selectedDestinationId
      )
      setTables(data)
    } catch (error) {
      console.error('Failed to load tables:', error)
    } finally {
      setLoading(false)
    }
  }

  const getCurrentDestination = () => {
    return destinations.find((d) => d.id === selectedDestinationId)
  }

  const currentDestination = getCurrentDestination()
  const destinationType = currentDestination?.destination?.type || 'POSTGRESQL'

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="left"
        className="w-[500px] sm:max-w-[500px] overflow-y-auto"
      >
        <SheetHeader>
          <SheetTitle>Source Tables Configuration</SheetTitle>
          <SheetDescription>
            Configure table synchronization for{' '}
            <span className="font-medium">{pipeline.source?.name}</span>
          </SheetDescription>
        </SheetHeader>

        <div className="mt-6">
          {/* Destination Tabs */}
          {destinations.length > 1 ? (
            <Tabs
              value={String(selectedDestinationId || '')}
              onValueChange={(val) => setSelectedDestinationId(Number(val))}
            >
              <TabsList className="w-full">
                {destinations.map((d) => (
                  <TabsTrigger key={d.id} value={String(d.id)} className="flex-1">
                    {d.destination.name}
                  </TabsTrigger>
                ))}
              </TabsList>
            </Tabs>
          ) : destinations.length === 1 ? (
            <div className="text-sm text-muted-foreground mb-4">
              Destination:{' '}
              <span className="font-medium text-foreground">
                {destinations[0].destination.name}
              </span>{' '}
              ({destinations[0].destination.type})
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">
              No destinations configured
            </div>
          )}

          {/* Table List */}
          <div className="mt-4">
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : destinationType === 'SNOWFLAKE' ? (
              <SnowflakeTableConfig
                tables={tables}
                pipelineId={pipeline.id}
                pipelineDestinationId={selectedDestinationId!}
                onRefresh={loadTables}
              />
            ) : (
              <PostgresTableConfig
                tables={tables}
                pipelineId={pipeline.id}
                pipelineDestinationId={selectedDestinationId!}
                onRefresh={loadTables}
              />
            )}
          </div>
        </div>
      </SheetContent>
    </Sheet>
  )
}
