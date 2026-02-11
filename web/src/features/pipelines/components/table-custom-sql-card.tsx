import { useState, useEffect } from 'react'
import ace from 'ace-builds'
import AceEditor from 'react-ace'
import { Button } from '@/components/ui/button'
import { Loader2, Save, X } from 'lucide-react'
import { TableWithSyncInfo, TableSyncConfig } from '@/repo/pipelines'
import { cn } from '@/lib/utils'
import { createSqlCompleter } from '@/features/pipelines/utils/sql-completer'
import { api } from '@/repo/client'

import 'ace-builds/src-noconflict/mode-mysql'
import 'ace-builds/src-noconflict/theme-tomorrow'
import 'ace-builds/src-noconflict/theme-tomorrow_night'
import 'ace-builds/src-noconflict/ext-language_tools'

interface TableCustomSqlCardProps {
    table: (TableWithSyncInfo & { sync_config?: TableSyncConfig }) | null
    open: boolean
    onClose: () => void
    onSave: (sql: string) => Promise<void>
    className?: string
    destinationName?: string
    destinationId?: number | null
}

export function TableCustomSqlCard({
    table,
    open,
    onClose,
    onSave,
    className,
    destinationName,
    destinationId
}: TableCustomSqlCardProps) {
    const [sql, setSql] = useState('')
    const [editorInstance, setEditorInstance] = useState<any>(null)
    const [isSaving, setIsSaving] = useState(false)
    const [isFetchingSchema, setIsFetchingSchema] = useState(false)
    const [isDarkMode, setIsDarkMode] = useState(document.documentElement.classList.contains('dark'))

    // Watch for theme changes
    useEffect(() => {
        const observer = new MutationObserver(() => {
            setIsDarkMode(document.documentElement.classList.contains('dark'))
        })

        observer.observe(document.documentElement, {
            attributes: true,
            attributeFilter: ['class']
        })

        return () => observer.disconnect()
    }, [])

    useEffect(() => {
        if (table && table.sync_config?.custom_sql) {
            setSql(table.sync_config.custom_sql)
        } else {
            setSql(`SELECT * FROM ${table?.table_name || 'table_name'}`)
        }
    }, [table, destinationName])

    // --- Configure Completer with Lazy Fetching ---
    useEffect(() => {
        if (!table || !editorInstance) return;

        const langTools = ace.require("ace/ext/language_tools");

        // Prepare Source Schema
        const sourceSchema: Record<string, string[]> = {};
        if (table) {
            sourceSchema[table.table_name] = table.columns.map(c => c.column_name);
        }

        // Async Fetcher for Destination Tables
        const fetchDestinationSchema = async (tableName: string) => {
            if (!destinationId) return [];
            try {
                // If tableName is empty, we just want list of tables
                const params: any = { table: tableName };
                if (!tableName) {
                    params.scope = 'tables';
                }

                const res = await api.get(`/destinations/${destinationId}/schema`, { params });
                const data = res.data;

                if (!tableName) {
                    // Return list of table names
                    return Object.keys(data);
                }

                // Check for key case-insensitive
                const key = Object.keys(data).find(k => k.toLowerCase() === tableName.toLowerCase()) || tableName;
                return data[key] || [];
            } catch (e) {
                return [];
            }
        };

        // Custom Completer
        const sqlCompleter = createSqlCompleter(
            sourceSchema,
            fetchDestinationSchema,
            destinationName,
            setIsFetchingSchema // Pass setLoading callback
        );

        // Register on the specific editor instance
        // Exclude textCompleter to reduce noise, keep default keyword completer
        editorInstance.completers = [
            sqlCompleter,
            langTools.keyWordCompleter
        ];

        // --- Auto-trigger on dot (.) ---
        const onAfterExec = (e: any) => {
            if (e.command.name === "insertstring" && e.args === ".") {
                editorInstance.execCommand("startAutocomplete");
            }
        };

        editorInstance.commands.on("afterExec", onAfterExec);
        return () => {
            editorInstance.commands.off("afterExec", onAfterExec);
        };
    }, [table, destinationId, destinationName, editorInstance]);


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
                <div className="flex items-center gap-4">
                    <div>
                        <h2 className="text-lg font-semibold">Custom SQL</h2>
                        <p className="text-sm text-muted-foreground">
                            Define custom SQL for <span className="font-medium text-foreground">{table.table_name}</span>
                        </p>
                    </div>
                    {isFetchingSchema && (
                        <div className="flex items-center gap-2 bg-blue-100 dark:bg-blue-900/30 px-2 py-1 rounded text-xs text-blue-700 dark:text-blue-300">
                            <Loader2 className="h-3 w-3 animate-spin" />
                            <span>Fetching schema...</span>
                        </div>
                    )}
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

                <div className="mb-4 p-3 bg-blue-50/50 rounded-lg border border-blue-100 dark:bg-blue-900/10 dark:border-blue-900/30">
                    <div className="flex gap-2">
                        <div className="flex-1 space-y-2">
                            <p className="text-xs text-blue-700 dark:text-blue-300">
                                You can join this table with all tables in the destination database using the prefix <code className="bg-blue-100 dark:bg-blue-900/40 px-1 py-0.5 rounded">pg_{destinationName ? destinationName.toLowerCase() : 'dest_name'}</code>.
                            </p>
                            <div className="bg-background/80 p-2 rounded border border-blue-200/50 dark:border-blue-800/30">
                                <code className="text-[10px] font-mono text-muted-foreground block">
                                    -- Example query<br />
                                    SELECT * FROM {table.table_name} t <br />
                                    JOIN pg_{destinationName ? destinationName.toLowerCase() : 'dest_1'}.table_a a ON t.id = a.id
                                </code>
                            </div>
                        </div>
                    </div>
                </div>

                <div className="flex-1 rounded-lg border overflow-hidden">
                    <AceEditor
                        placeholder={`SELECT * FROM ${table.table_name}`}
                        mode="mysql"
                        theme={isDarkMode ? 'tomorrow_night' : 'tomorrow'}
                        name="custom-sql-editor"
                        onLoad={(editor) => setEditorInstance(editor)}
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
