import { useState } from 'react'
import { TableWithSyncInfo, tableSyncRepo } from '@/repo/pipelines'
import { Checkbox } from '@/components/ui/checkbox'
import { Button } from '@/components/ui/button'
import { Filter, Code2, Loader2 } from 'lucide-react'
import { FilterModal } from '@/features/pipelines/components/filter-modal'
import { SqlEditorModal } from '@/features/pipelines/components/sql-editor-modal'
import { toast } from 'sonner'

interface PostgresTableConfigProps {
  tables: TableWithSyncInfo[]
  pipelineId: number
  pipelineDestinationId: number
  onRefresh: () => void
}

export function PostgresTableConfig({
  tables,
  pipelineId,
  pipelineDestinationId,
  onRefresh,
}: PostgresTableConfigProps) {
  const [filterModalOpen, setFilterModalOpen] = useState(false)
  const [sqlEditorOpen, setSqlEditorOpen] = useState(false)
  const [selectedTable, setSelectedTable] = useState<TableWithSyncInfo | null>(null)
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

  const handleOpenFilter = (table: TableWithSyncInfo) => {
    setSelectedTable(table)
    setFilterModalOpen(true)
  }

  const handleOpenSqlEditor = (table: TableWithSyncInfo) => {
    setSelectedTable(table)
    setSqlEditorOpen(true)
  }

  const handleSaveFilter = async (filterSql: string) => {
    if (!selectedTable) return

    try {
      await tableSyncRepo.saveTableSync(pipelineId, pipelineDestinationId, {
        table_name: selectedTable.table_name,
        filter_sql: filterSql,
        custom_sql: selectedTable.sync_config?.custom_sql,
      })
      toast.success('Filter saved successfully')
      setFilterModalOpen(false)
      onRefresh()
    } catch (error) {
      toast.error('Failed to save filter')
    }
  }

  const handleSaveCustomSql = async (customSql: string) => {
    if (!selectedTable) return

    try {
      await tableSyncRepo.saveTableSync(pipelineId, pipelineDestinationId, {
        table_name: selectedTable.table_name,
        custom_sql: customSql,
        filter_sql: selectedTable.sync_config?.filter_sql,
      })
      toast.success('Custom SQL saved successfully')
      setSqlEditorOpen(false)
      onRefresh()
    } catch (error) {
      toast.error('Failed to save custom SQL')
    }
  }

  return (
    <div className="space-y-2">
      <div className="text-sm font-medium mb-3">
        Tables ({tables.length})
      </div>

      <div className="border rounded-lg divide-y">
        {tables.map((table) => (
          <div
            key={table.table_name}
            className="flex items-center gap-3 px-3 py-2.5 hover:bg-muted/50 transition-colors"
          >
            {/* Checkbox */}
            <div className="flex items-center">
              {savingTable === table.table_name ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Checkbox
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
              {table.sync_config?.filter_sql && (
                <span className="text-xs text-muted-foreground">
                  Has filter
                </span>
              )}
            </div>

            {/* Actions */}
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => handleOpenFilter(table)}
                disabled={!table.sync_config}
                className="h-8 px-2"
              >
                <Filter className="h-4 w-4" />
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => handleOpenSqlEditor(table)}
                disabled={!table.sync_config}
                className="h-8 px-2"
              >
                <Code2 className="h-4 w-4" />
              </Button>
            </div>
          </div>
        ))}

        {tables.length === 0 && (
          <div className="px-3 py-8 text-center text-sm text-muted-foreground">
            No tables available
          </div>
        )}
      </div>

      {/* Filter Modal */}
      <FilterModal
        open={filterModalOpen}
        onOpenChange={setFilterModalOpen}
        table={selectedTable}
        onSave={handleSaveFilter}
      />

      {/* SQL Editor Modal */}
      <SqlEditorModal
        open={sqlEditorOpen}
        onOpenChange={setSqlEditorOpen}
        table={selectedTable}
        onSave={handleSaveCustomSql}
      />
    </div>
  )
}
