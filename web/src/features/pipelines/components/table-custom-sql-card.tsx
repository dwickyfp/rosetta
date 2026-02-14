import { useState, useEffect } from 'react'
import { api } from '@/repo/client'
import { TableWithSyncInfo, TableSyncConfig } from '@/repo/pipelines'
import ace from 'ace-builds'
import 'ace-builds/src-noconflict/ext-language_tools'
import 'ace-builds/src-noconflict/mode-mysql'
import 'ace-builds/src-noconflict/theme-tomorrow'
import 'ace-builds/src-noconflict/theme-tomorrow_night'
import { Loader2, Save, X, Eye, AlertCircle } from 'lucide-react'
import AceEditor from 'react-ace'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { createSqlCompleter } from '@/features/pipelines/utils/sql-completer'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'

interface TableCustomSqlCardProps {
  table: (TableWithSyncInfo & { sync_config?: TableSyncConfig }) | null
  open: boolean
  onClose: () => void
  onSave: (sql: string) => Promise<void>
  className?: string
  destinationName?: string
  destinationId?: number | null
  sourceName?: string
  sourceId?: number | null
  pipelineId: number
}

interface PreviewData {
  columns: string[]
  data: Record<string, any>[]
  error?: string
}

export function TableCustomSqlCard({
  table,
  open,
  onClose,
  onSave,
  className,
  destinationName,
  destinationId,
  sourceName,
  sourceId,
  pipelineId,
}: TableCustomSqlCardProps) {
  const [sql, setSql] = useState('')
  const [editorInstance, setEditorInstance] = useState<any>(null)
  const [isSaving, setIsSaving] = useState(false)
  const [isFetchingSchema, setIsFetchingSchema] = useState(false)
  const [isDarkMode, setIsDarkMode] = useState(
    document.documentElement.classList.contains('dark')
  )

  // Preview State
  const [isPreviewLoading, setIsPreviewLoading] = useState(false)
  const [previewData, setPreviewData] = useState<PreviewData | null>(null)
  const [isPreviewOpen, setIsPreviewOpen] = useState(false)

  // Watch for theme changes
  useEffect(() => {
    const observer = new MutationObserver(() => {
      setIsDarkMode(document.documentElement.classList.contains('dark'))
    })

    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    })

    return () => observer.disconnect()
  }, [])

  // Only reset SQL when drawer opens or table changes (by table name, not object reference)
  useEffect(() => {
    if (!open) return // Don't reset when drawer is closed

    if (table && table.sync_config?.custom_sql) {
      setSql(table.sync_config.custom_sql)
    } else {
      setSql(`SELECT * FROM ${table?.table_name || 'table_name'}`)
    }
  }, [open, table?.table_name, table?.sync_config?.custom_sql])

  // --- Configure Completer with Lazy Fetching ---
  useEffect(() => {
    if (!table || !editorInstance) return

    const langTools = ace.require('ace/ext/language_tools')

    // Prepare Source Schema
    const sourceSchema: Record<string, string[]> = {}
    if (table) {
      sourceSchema[table.table_name] = table.columns.map((c) => c.column_name)
    }

    // Async Fetcher for Destination Tables
    const fetchDestinationSchema = async (tableName: string) => {
      if (!destinationId) return []
      try {
        // If tableName is empty, we just want list of tables
        const params: any = { table: tableName }
        if (!tableName) {
          params.scope = 'tables'
        }

        const res = await api.get(`/destinations/${destinationId}/schema`, {
          params,
        })
        const data = res.data

        if (!tableName) {
          // Return list of table names
          return Object.keys(data)
        }

        // Check for key case-insensitive
        const key =
          Object.keys(data).find(
            (k) => k.toLowerCase() === tableName.toLowerCase()
          ) || tableName
        return data[key] || []
      } catch (e) {
        return []
      }
    }

    // Async Fetcher for Source Tables
    const fetchSourceSchema = async (tableName: string) => {
      if (!sourceId) return []
      try {
        // If tableName is empty, we just want list of tables
        const params: any = { table: tableName }
        if (!tableName) {
          params.scope = 'tables'
        }

        const res = await api.get(`/sources/${sourceId}/schema`, {
          params,
        })
        const data = res.data

        if (!tableName) {
          // Return list of table names
          return Object.keys(data)
        }

        // Check for key case-insensitive
        const key =
          Object.keys(data).find(
            (k) => k.toLowerCase() === tableName.toLowerCase()
          ) || tableName
        return data[key] || []
      } catch (e) {
        return []
      }
    }

    // Custom Completer
    const sqlCompleter = createSqlCompleter(
      sourceSchema,
      fetchDestinationSchema,
      fetchSourceSchema,
      destinationName,
      sourceName,
      setIsFetchingSchema // Pass setLoading callback
    )

    // Register on the specific editor instance
    // Exclude textCompleter to reduce noise, keep default keyword completer
    editorInstance.completers = [sqlCompleter, langTools.keyWordCompleter]

    // --- Auto-trigger on dot (.) ---
    const onAfterExec = (e: any) => {
      if (e.command.name === 'insertstring' && e.args === '.') {
        editorInstance.execCommand('startAutocomplete')
      }
    }

    editorInstance.commands.on('afterExec', onAfterExec)
    return () => {
      editorInstance.commands.off('afterExec', onAfterExec)
    }
  }, [
    table,
    destinationId,
    destinationName,
    sourceId,
    sourceName,
    editorInstance,
  ])

  const handleSave = async (e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()

    if (!validateSql(sql)) return

    setIsSaving(true)
    try {
      await onSave(sql)
    } finally {
      setIsSaving(false)
    }
  }

  // Validate SQL for forbidden operations
  const validateSql = (sqlText: string): boolean => {
    if (!sqlText) return true

    // List of forbidden keywords for custom SQL
    // We strictly block destructive operations
    const forbiddenKeywords = [
      'UPDATE', 'DELETE', 'TRUNCATE', 'DROP', 'ALTER', 'GRANT', 'REVOKE',
      'INSERT', 'CREATE', 'REPLACE', 'MERGE'
    ]

    for (const keyword of forbiddenKeywords) {
      // Check for whole word match, case insensitive
      const regex = new RegExp(`\\b${keyword}\\b`, 'i')
      if (regex.test(sqlText)) {
        toast.error(`Operation '${keyword}' is not allowed. Only SELECT statements are permitted.`)
        return false
      }
    }
    return true
  }

  const handlePreview = async () => {
    if (!pipelineId || !sourceId || !destinationId || !table) return
    if (!validateSql(sql)) return

    setIsPreviewLoading(true)
    setPreviewData(null)
    setIsPreviewOpen(true) // Open popover immediately to show loading state

    try {
      const res = await api.post(`/pipelines/${pipelineId}/preview`, {
        sql,
        source_id: sourceId,
        destination_id: destinationId,
        table_name: table.table_name,
      })
      setPreviewData(res.data)
    } catch (e: any) {
      setPreviewData({
        columns: [],
        data: [],
        error: e.response?.data?.detail || e.message || 'Failed to preview data',
      })
    } finally {
      setIsPreviewLoading(false)
    }
  }

  const handleClose = (e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()
    onClose()
  }

  if (!open || !table) return null

  return (
    <div
      className={cn(
        'fixed top-2 bottom-2 left-[520px] flex w-[800px] flex-col rounded-2xl border bg-background shadow-2xl',
        'animate-in duration-300 slide-in-from-left-4',
        className
      )}
      style={{ zIndex: 100 }}
      onClick={(e) => e.stopPropagation()}
      onMouseDown={(e) => e.stopPropagation()}
    >
      {/* Header */}
      <div className='flex items-center justify-between border-b bg-muted/30 px-6 py-4'>
        <div className='flex items-center gap-4'>
          <div>
            <h2 className='text-lg font-semibold'>Custom SQL</h2>
            <p className='text-sm text-muted-foreground'>
              Define custom SQL for{' '}
              <span className='font-medium text-foreground'>
                {table.table_name}
              </span>
            </p>
          </div>
          {isFetchingSchema && (
            <div className='flex items-center gap-2 rounded bg-blue-100 px-2 py-1 text-xs text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'>
              <Loader2 className='h-3 w-3 animate-spin' />
              <span>Fetching schema...</span>
            </div>
          )}
        </div>
        <Button
          variant='ghost'
          size='icon'
          onClick={handleClose}
          className='h-8 w-8'
        >
          <X className='h-4 w-4' />
        </Button>
      </div>

      {/* Content */}
      <div className='flex flex-1 flex-col overflow-hidden p-6'>
        <div className='mb-3'>
          <p className='text-xs text-muted-foreground'>
            Write a custom SQL query to transform data before syncing. Use the
            table columns as reference.
          </p>
        </div>

        <div className='mb-4 rounded-lg border border-blue-100 bg-blue-50/50 p-3 dark:border-blue-900/30 dark:bg-blue-900/10'>
          <div className='flex gap-2'>
            <div className='flex-1 space-y-2'>
              <p className='text-xs text-blue-700 dark:text-blue-300'>
                You can join this table with tables from the{' '}
                <strong>destination database</strong> using{' '}
                <code className='rounded bg-blue-100 px-1 py-0.5 dark:bg-blue-900/40'>
                  pg_
                  {destinationName
                    ? destinationName.toLowerCase()
                    : 'dest_name'}
                </code>{' '}
                and from the <strong>source database</strong> using{' '}
                <code className='rounded bg-blue-100 px-1 py-0.5 dark:bg-blue-900/40'>
                  pg_src_
                  {sourceName ? sourceName.toLowerCase() : 'source_name'}
                </code>
                .
              </p>
              <div className='rounded border border-blue-200/50 bg-background/80 p-2 dark:border-blue-800/30'>
                <code className='block font-mono text-[10px] text-muted-foreground'>
                  -- Example: Join with destination and source tables
                  <br />
                  SELECT t.*, d.status, s.metadata FROM {
                    table.table_name
                  } t <br />
                  JOIN pg_
                  {destinationName ? destinationName.toLowerCase() : 'dest_1'}
                  .orders d ON t.id = d.id
                  <br />
                  JOIN pg_src_
                  {sourceName ? sourceName.toLowerCase() : 'source_1'}
                  .customers s ON t.customer_id = s.id
                </code>
              </div>
            </div>
          </div>
        </div>

        <div className='flex-1 overflow-hidden rounded-lg border'>
          <AceEditor
            placeholder={`SELECT * FROM ${table.table_name}`}
            mode='mysql'
            theme={isDarkMode ? 'tomorrow_night' : 'tomorrow'}
            name='custom-sql-editor'
            onLoad={(editor) => setEditorInstance(editor)}
            onChange={setSql}
            fontSize={14}
            lineHeight={20}
            showPrintMargin={false}
            showGutter={true}
            highlightActiveLine={true}
            value={sql}
            width='100%'
            height='100%'
            setOptions={{
              enableBasicAutocompletion: true,
              enableLiveAutocompletion: true,
              enableSnippets: true,
              showLineNumbers: true,
              tabSize: 2,
              useWorker: false,
            }}
          />
        </div>
      </div>

      {/* Footer */}
      <div className='flex items-center justify-between border-t bg-muted/30 px-6 py-4'>
        <div className='flex gap-2'>
          <Button
            variant='ghost'
            size='sm'
            onClick={(e) => {
              e.stopPropagation()
              setSql(`SELECT * FROM ${table.table_name}`)
            }}
            className='text-muted-foreground'
          >
            Reset to Default
          </Button>
          {table.sync_config?.custom_sql && (
            <Button
              variant='ghost'
              size='sm'
              onClick={async (e) => {
                e.stopPropagation()
                setIsSaving(true)
                try {
                  await onSave('')
                  setSql(`SELECT * FROM ${table.table_name}`)
                } finally {
                  setIsSaving(false)
                }
              }}
              className='text-destructive hover:bg-destructive/10 hover:text-destructive'
              disabled={isSaving}
            >
              Remove Custom SQL
            </Button>
          )}
        </div>
        <div className='flex gap-2'>
          <Popover open={isPreviewOpen} onOpenChange={setIsPreviewOpen}>
            <PopoverTrigger asChild>
              <Button
                variant='outline'
                size='sm'
                onClick={handlePreview}
                disabled={isPreviewLoading || isSaving}
              >
                {isPreviewLoading ? (
                  <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                ) : (
                  <Eye className='mr-2 h-4 w-4' />
                )}
                Preview Data
              </Button>
            </PopoverTrigger>
            <PopoverContent
              className='w-[800px] border-none p-0 shadow-2xl z-[200]'
              side='top'
              align='end'
            >
              <div className='flex flex-col overflow-hidden rounded-lg border bg-popover'>
                <div className='border-b bg-muted/30 px-4 py-3'>
                  <h3 className='font-semibold'>Preview Results</h3>
                  <p className='text-xs text-muted-foreground'>
                    Showing first 10 rows
                  </p>
                </div>
                <div className='p-0'>
                  {isPreviewLoading ? (
                    <div className='flex h-[200px] items-center justify-center'>
                      <div className='flex flex-col items-center gap-2'>
                        <Loader2 className='h-8 w-8 animate-spin text-primary' />
                        <span className='text-sm text-muted-foreground'>
                          Executing query...
                        </span>
                      </div>
                    </div>
                  ) : previewData?.error ? (
                    <div className='flex h-[200px] flex-col items-center justify-center p-6 text-center'>
                      <AlertCircle className='mb-2 h-8 w-8 text-destructive' />
                      <p className='font-medium text-destructive'> Preview Failed</p>
                      <p className='mt-1 text-sm text-muted-foreground'>
                        {previewData.error}
                      </p>
                    </div>
                  ) : previewData && previewData.columns.length > 0 ? (
                    <ScrollArea className='h-[400px] w-full rounded-md border'>
                      <div className='w-max min-w-full'>
                        <table className='w-full caption-bottom text-sm'>
                          <thead className='sticky top-0 z-10 bg-muted [&_tr]:border-b border-border/50'>
                            <tr className='border-b border-border/50 transition-colors duration-150 hover:bg-muted/50 data-[state=selected]:bg-muted'>
                              {previewData.columns.map((col) => (
                                <th key={col} className='h-10 px-3 text-left align-middle font-medium text-xs uppercase tracking-wider text-muted-foreground [&>[role=checkbox]]:translate-y-[2px] whitespace-nowrap'>
                                  {col}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className='[&_tr:last-child]:border-0'>
                            {previewData.data.map((row, i) => (
                              <tr key={i} className='border-b border-border/50 transition-colors duration-150 hover:bg-muted/50 data-[state=selected]:bg-muted'>
                                {previewData.columns.map((col) => (
                                  <td key={col} className='px-3 py-2.5 align-middle whitespace-nowrap [&>[role=checkbox]]:translate-y-[2px]'>
                                    {row[col]?.toString() ?? (
                                      <span className='italic text-muted-foreground'>
                                        null
                                      </span>
                                    )}
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <ScrollBar orientation='horizontal' />
                      <ScrollBar orientation='vertical' />
                    </ScrollArea>
                  ) : (
                    <div className='flex h-[100px] items-center justify-center text-sm text-muted-foreground'>
                      No data returned
                    </div>
                  )}
                </div>
              </div>
            </PopoverContent>
          </Popover>

          <Button variant='outline' size='sm' onClick={handleClose}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={isSaving} size='sm'>
            {isSaving && <Loader2 className='mr-2 h-4 w-4 animate-spin' />}
            {!isSaving && <Save className='mr-2 h-4 w-4' />}
            Save SQL
          </Button>
        </div>
      </div>
    </div>
  )
}
