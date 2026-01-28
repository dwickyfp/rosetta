import { useState } from 'react'
import { TableWithSyncInfo, tableSyncRepo } from '@/repo/pipelines'
import { Switch } from '@/components/ui/switch'
import { Button } from '@/components/ui/button'
import { Filter, Code2, Loader2, AlertCircle } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'

interface PostgresTableConfigProps {
  tables: TableWithSyncInfo[]
  pipelineId: number
  pipelineDestinationId: number
  onRefresh: () => void
  onEditFilter: (table: TableWithSyncInfo) => void
  onEditCustomSql: (table: TableWithSyncInfo) => void
}

export function PostgresTableConfig({
  tables,
  pipelineId,
  pipelineDestinationId,
  onRefresh,
  onEditFilter,
  onEditCustomSql
}: PostgresTableConfigProps) {
  const [savingTable, setSavingTable] = useState<string | null>(null)

  const handleToggleSync = async (table: TableWithSyncInfo) => {
    setSavingTable(table.table_name)
    try {
      if (table.sync_config) {
        // Remove from sync
        await tableSyncRepo.deleteTableSync(
          pipelineId,
          pipelineDestinationId,
          table.table_name
        )
        toast.success(`Removed ${table.table_name} from sync`)
      } else {
        // Add to sync
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

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="text-sm font-medium text-muted-foreground">Tables ({tables.length})</div>
      </div>

      <div className="space-y-3">
        {tables.map((table) => (
          <div
            key={table.table_name}
            className="flex items-center justify-between gap-3 p-3 border rounded-lg bg-card hover:bg-accent/5 transition-colors group"
          >
            <div className="flex items-center gap-3 min-w-0">
              <div className="flex items-center pt-0.5">
                {savingTable === table.table_name ? (
                  <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                ) : (
                  <Switch
                    checked={!!table.sync_config}
                    onCheckedChange={() => handleToggleSync(table)}
                  />
                )}
              </div>

              <div className="min-w-0">
                <span className="text-sm font-medium truncate block" title={table.table_name}>
                  {table.table_name}
                </span>
                {(table.sync_config?.filter_sql || table.sync_config?.custom_sql) && (
                  <div className="flex gap-2 mt-0.5">
                    {table.sync_config?.filter_sql && <span className="text-[10px] bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded border border-blue-100">Filtered</span>}
                    {table.sync_config?.custom_sql && <span className="text-[10px] bg-purple-50 text-purple-700 px-1.5 py-0.5 rounded border border-purple-100">Custom SQL</span>}
                  </div>
                )}
              </div>
            </div>

            {/* Actions */}
            <div className="flex items-center gap-2 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
              <Button
                variant="outline"
                size="sm"
                onClick={(e) => {
                  e.preventDefault()
                  e.stopPropagation()
                  onEditFilter(table)
                }}
                disabled={!table.sync_config}
                className={cn("h-8 px-2.5 text-xs", table.sync_config?.filter_sql && "bg-blue-50 border-blue-200 text-blue-700 hover:bg-blue-100")}
              >
                <Filter className="h-3.5 w-3.5 mr-1.5" />
                Filter
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={(e) => {
                  e.preventDefault()
                  e.stopPropagation()
                  onEditCustomSql(table)
                }}
                disabled={!table.sync_config}
                className={cn("h-8 px-2.5 text-xs", table.sync_config?.custom_sql && "bg-purple-50 border-purple-200 text-purple-700 hover:bg-purple-100")}
              >
                <Code2 className="h-3.5 w-3.5 mr-1.5" />
                Custom
              </Button>
            </div>
          </div>
        ))}

        {tables.length === 0 && (
          <div className="flex flex-col items-center justify-center py-12 text-center border-2 border-dashed rounded-lg">
            <AlertCircle className="h-8 w-8 text-muted-foreground mb-2" />
            <p className="text-sm font-medium">No tables found</p>
            <p className="text-xs text-muted-foreground">This source has no tables available to configure.</p>
          </div>
        )}
      </div>
    </div>
  )
}
