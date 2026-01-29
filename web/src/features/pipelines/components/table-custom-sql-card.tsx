import { useState, useEffect } from 'react'
import AceEditor from 'react-ace'
import { Button } from '@/components/ui/button'
import { Loader2, Save, X } from 'lucide-react'
import { TableWithSyncInfo } from '@/repo/pipelines'
import { cn } from '@/lib/utils'

import 'ace-builds/src-noconflict/mode-mysql'
import 'ace-builds/src-noconflict/theme-tomorrow'
import 'ace-builds/src-noconflict/ext-language_tools'

interface TableCustomSqlCardProps {
    table: TableWithSyncInfo | null
    open: boolean
    onClose: () => void
    onSave: (sql: string) => Promise<void>
    className?: string
}

export function TableCustomSqlCard({
    table,
    open,
    onClose,
    onSave,
    className
}: TableCustomSqlCardProps) {
    const [sql, setSql] = useState('')
    const [isSaving, setIsSaving] = useState(false)

    useEffect(() => {
        if (table && table.sync_config?.custom_sql) {
            setSql(table.sync_config.custom_sql)
        } else {
            setSql(`SELECT * FROM ${table?.table_name || 'table_name'}`)
        }
    }, [table])

    const handleSave = async (e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()

        setIsSaving(true)
        try {
            await onSave(sql)
        } finally {
            setIsSaving(false)
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
                "fixed top-2 bottom-2 left-[520px] w-[800px] bg-background border rounded-2xl shadow-2xl flex flex-col",
                "animate-in slide-in-from-left-4 duration-300",
                className
            )}
            style={{ zIndex: 100 }}
            onClick={(e) => e.stopPropagation()}
            onMouseDown={(e) => e.stopPropagation()}
        >
            {/* Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b bg-muted/30">
                <div>
                    <h2 className="text-lg font-semibold">Custom SQL</h2>
                    <p className="text-sm text-muted-foreground">
                        Define custom SQL for <span className="font-medium text-foreground">{table.table_name}</span>
                    </p>
                </div>
                <Button variant="ghost" size="icon" onClick={handleClose} className="h-8 w-8">
                    <X className="h-4 w-4" />
                </Button>
            </div>

            {/* Content */}
            <div className="flex-1 p-6 overflow-hidden flex flex-col">
                <div className="mb-3">
                    <p className="text-xs text-muted-foreground">
                        Write a custom SQL query to transform data before syncing. Use the table columns as reference.
                    </p>
                </div>

                {/* Columns reference */}
                <div className="mb-4 p-3 bg-muted/50 rounded-lg border">
                    <p className="text-xs font-medium text-muted-foreground mb-2 uppercase tracking-wide">Available Columns</p>
                    <div className="flex flex-wrap gap-1.5">
                        {table.columns.slice(0, 15).map(col => (
                            <span
                                key={col.column_name}
                                className="text-xs bg-background px-2 py-1 rounded border cursor-pointer hover:bg-accent transition-colors"
                                title={col.data_type}
                                onClick={() => {
                                    setSql(prev => prev + col.column_name)
                                }}
                            >
                                {col.column_name}
                            </span>
                        ))}
                        {table.columns.length > 15 && (
                            <span className="text-xs text-muted-foreground px-2 py-1">
                                +{table.columns.length - 15} more
                            </span>
                        )}
                    </div>
                </div>

                <div className="flex-1 rounded-lg border overflow-hidden">
                    <AceEditor
                        placeholder={`SELECT * FROM ${table.table_name}`}
                        mode="mysql"
                        theme="tomorrow"
                        name="custom-sql-editor"
                        onChange={setSql}
                        fontSize={14}
                        lineHeight={20}
                        showPrintMargin={false}
                        showGutter={true}
                        highlightActiveLine={true}
                        value={sql}
                        width="100%"
                        height="100%"
                        setOptions={{
                            enableBasicAutocompletion: true,
                            enableLiveAutocompletion: true,
                            enableSnippets: true,
                            showLineNumbers: true,
                            tabSize: 2,
                            useWorker: false
                        }}
                    />
                </div>
            </div>

            {/* Footer */}
            <div className="flex items-center justify-between px-6 py-4 border-t bg-muted/30">
                <div className="flex gap-2">
                    <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => {
                            e.stopPropagation()
                            setSql(`SELECT * FROM ${table.table_name}`)
                        }}
                        className="text-muted-foreground"
                    >
                        Reset to Default
                    </Button>
                    {table.sync_config?.custom_sql && (
                        <Button
                            variant="ghost"
                            size="sm"
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
                            className="text-destructive hover:text-destructive hover:bg-destructive/10"
                            disabled={isSaving}
                        >
                            Remove Custom SQL
                        </Button>
                    )}
                </div>
                <div className="flex gap-2">
                    <Button variant="outline" size="sm" onClick={handleClose}>
                        Cancel
                    </Button>
                    <Button onClick={handleSave} disabled={isSaving} size="sm">
                        {isSaving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        {!isSaving && <Save className="mr-2 h-4 w-4" />}
                        Save SQL
                    </Button>
                </div>
            </div>
        </div>
    )
}
