import { useState } from 'react'
import { TableWithSyncInfo, tableSyncRepo } from '@/repo/pipelines'
import { Switch } from '@/components/ui/switch'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { CheckCircle2, XCircle, Loader2, Play } from 'lucide-react'
import { toast } from 'sonner'

interface SnowflakeTableConfigProps {
  tables: TableWithSyncInfo[]
  pipelineId: number
  pipelineDestinationId: number
  onRefresh: () => void
}

export function SnowflakeTableConfig({
  tables,
  pipelineId,
  pipelineDestinationId,
  onRefresh,
}: SnowflakeTableConfigProps) {
  const [initializingTable, setInitializingTable] = useState<string | null>(null)
  const [savingTable, setSavingTable] = useState<string | null>(null)

  const handleToggleSync = async (table: TableWithSyncInfo) => {
    setSavingTable(table.table_name)
    try {
      if (table.sync_config) {
        await tableSyncRepo.deleteTableSync(
          pipelineId,
          pipelineDestinationId,
          table.table_name
        )
        toast.success(`Removed ${table.table_name} from sync`)
      } else {
        await tableSyncRepo.saveTableSync(pipelineId, pipelineDestinationId, {
          table_name: table.table_name,
        })
        toast.success(`Added ${table.table_name} to sync`)
      }
      onRefresh()
    } catch (error) {
      toast.error('Failed to update table sync')
    } finally {
      setSavingTable(null)
    }
  }

  const handleInitTable = async (table: TableWithSyncInfo) => {
    setInitializingTable(table.table_name)
    try {
      const result = await tableSyncRepo.initSnowflakeTable(
        pipelineId,
        pipelineDestinationId,
        table.table_name
      )
      if (result.status === 'success') {
        toast.success(result.message)
      } else if (result.status === 'skipped') {
        toast.info(result.message)
      } else {
        toast.error(result.message)
      }
      onRefresh()
    } catch (error) {
      toast.error('Failed to initialize table')
    } finally {
      setInitializingTable(null)
    }
  }

  const StatusBadge = ({
    exists,
    label,
  }: {
    exists: boolean
    label: string
  }) => (
    <Badge
      variant={exists ? 'default' : 'secondary'}
      className={`text-xs ${
        exists
          ? 'bg-emerald-100 text-emerald-700 hover:bg-emerald-100'
          : 'bg-gray-100 text-gray-500'
      }`}
    >
      {exists ? (
        <CheckCircle2 className="h-3 w-3 mr-1" />
      ) : (
        <XCircle className="h-3 w-3 mr-1" />
      )}
      {label}
    </Badge>
  )

  return (
    <div className="space-y-2">
      <div className="text-sm font-medium mb-3">Tables ({tables.length})</div>

      <div className="border rounded-lg divide-y">
        {tables.map((table) => (
          <div
            key={table.table_name}
            className="px-3 py-3 hover:bg-muted/50 transition-colors"
          >
            {/* Top row: Switch + Name + Init Button */}
            <div className="flex items-center gap-3 mb-2">
              {/* Switch */}
              <div className="flex items-center">
                {savingTable === table.table_name ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Switch
                    checked={!!table.sync_config}
                    onCheckedChange={() => handleToggleSync(table)}
                  />
                )}
              </div>

              {/* Table Name */}
              <div className="flex-1 min-w-0">
                <span className="text-sm font-medium truncate block">
                  {table.table_name}
                </span>
              </div>

              {/* Init Button */}
              <Button
                variant="outline"
                size="sm"
                onClick={() => handleInitTable(table)}
                disabled={initializingTable === table.table_name}
                className="h-7 px-2 text-xs"
              >
                {initializingTable === table.table_name ? (
                  <Loader2 className="h-3 w-3 animate-spin mr-1" />
                ) : (
                  <Play className="h-3 w-3 mr-1" />
                )}
                Init Table
              </Button>
            </div>

            {/* Bottom row: Status badges */}
            <div className="flex flex-wrap gap-1.5 ml-10">
              <StatusBadge
                exists={table.is_exists_table_landing}
                label="Landing"
              />
              <StatusBadge exists={table.is_exists_stream} label="Stream" />
              <StatusBadge exists={table.is_exists_task} label="Task" />
              <StatusBadge
                exists={table.is_exists_table_destination}
                label="Target"
              />
            </div>
          </div>
        ))}

        {tables.length === 0 && (
          <div className="px-3 py-8 text-center text-sm text-muted-foreground">
            No tables available
          </div>
        )}
      </div>
    </div>
  )
}
