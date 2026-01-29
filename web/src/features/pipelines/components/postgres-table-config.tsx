import { useState } from 'react'
import { TableWithSyncInfo, tableSyncRepo } from '@/repo/pipelines'
import { Switch } from '@/components/ui/switch'
import { Button } from '@/components/ui/button'
import { Filter, Code2, Loader2, AlertCircle, Database, ArrowRight } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'

interface PostgresTableConfigProps {
  tables: TableWithSyncInfo[]
  pipelineId: number
  pipelineDestinationId: number
  onRefresh: () => void
  onEditFilter: (table: TableWithSyncInfo) => void
  onEditCustomSql: (table: TableWithSyncInfo) => void
  onEditTargetName: (table: TableWithSyncInfo) => void
}

export function PostgresTableConfig({
  tables,
  pipelineId,
  pipelineDestinationId,
  onRefresh,
  onEditFilter,
  onEditCustomSql,
  onEditTargetName
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

  const hasCustomTarget = (table: TableWithSyncInfo) => {
    return table.sync_config?.table_name_target &&
      table.sync_config.table_name_target !== table.table_name
  }

  const getTargetName = (table: TableWithSyncInfo) => {
    return table.sync_config?.table_name_target || table.table_name
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
            className="p-3 border rounded-lg bg-card hover:bg-accent/5 transition-colors group"
          >
            {/* Row 1: Toggle + Table Name */}
            <div className="flex items-start gap-3">
              <div className="flex items-center pt-0.5 flex-shrink-0">
                {savingTable === table.table_name ? (
                  <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                ) : (
                  <Switch
                    checked={!!table.sync_config}
                    onCheckedChange={() => handleToggleSync(table)}
                  />
                )}
              </div>

              <div className="flex-1 min-w-0">
                {/* Table name with arrow to target */}
                <div className="flex items-center gap-2 min-w-0">
                  <span className="text-sm font-medium font-mono truncate" title={table.table_name}>
                    {table.table_name}
                  </span>
                  {table.sync_config && (
                    <>
                      <ArrowRight className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
                      <span
                        className={cn(
                          "text-sm font-mono truncate",
                          hasCustomTarget(table) ? "text-green-600 font-medium" : "text-muted-foreground"
                        )}
                        title={getTargetName(table)}
                      >
                        {getTargetName(table)}
                      </span>
                    </>
                  )}
                </div>

                {/* Badges */}
                {(table.sync_config?.filter_sql || table.sync_config?.custom_sql) && (
                  <div className="flex gap-2 mt-1.5 flex-wrap">
                    {table.sync_config?.filter_sql && (
                      <span className="text-[10px] bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded border border-blue-100">
                        Filtered
                      </span>
                    )}
                    {table.sync_config?.custom_sql && (
                      <span className="text-[10px] bg-purple-50 text-purple-700 px-1.5 py-0.5 rounded border border-purple-100">
                        Custom SQL
                      </span>
                    )}
                  </div>
                )}
              </div>
            </div>

            {/* Row 2: Action Buttons (only when synced) */}
            {table.sync_config && (
              <div className="flex items-center gap-2 mt-3 pl-10">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={(e) => {
                    e.preventDefault()
                    e.stopPropagation()
                    onEditTargetName(table)
                  }}
                  className={cn(
                    "h-7 px-2.5 text-xs",
                    hasCustomTarget(table) && "bg-green-50 border-green-200 text-green-700 hover:bg-green-100"
                  )}
                >
                  <Database className="h-3 w-3 mr-1.5" />
                  Target
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={(e) => {
                    e.preventDefault()
                    e.stopPropagation()
                    onEditFilter(table)
                  }}
                  className={cn(
                    "h-7 px-2.5 text-xs",
                    table.sync_config?.filter_sql && "bg-blue-50 border-blue-200 text-blue-700 hover:bg-blue-100"
                  )}
                >
                  <Filter className="h-3 w-3 mr-1.5" />
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
                  className={cn(
                    "h-7 px-2.5 text-xs",
                    table.sync_config?.custom_sql && "bg-purple-50 border-purple-200 text-purple-700 hover:bg-purple-100"
                  )}
                >
                  <Code2 className="h-3 w-3 mr-1.5" />
                  Custom SQL
                </Button>
              </div>
            )}
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


