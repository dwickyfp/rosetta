import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Loader2, Save, X, Database } from 'lucide-react'
import { TableWithSyncInfo } from '@/repo/pipelines'
import { cn } from '@/lib/utils'

interface TableTargetNameCardProps {
    table: TableWithSyncInfo | null
    open: boolean
    onClose: () => void
    onSave: (targetName: string) => Promise<void>
    className?: string
}

export function TableTargetNameCard({
    table,
    open,
    onClose,
    onSave,
    className
}: TableTargetNameCardProps) {
    const [targetName, setTargetName] = useState('')
    const [isSaving, setIsSaving] = useState(false)

    // Load target name from sync_config when table changes
    useEffect(() => {
        if (open && table?.sync_configs?.[0]) {
            setTargetName(table.sync_configs[0].table_name_target || table.table_name)
        } else if (open && table) {
            setTargetName(table.table_name)
        }
    }, [table?.table_name, table?.sync_configs, open])

    const handleSave = async (e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()

        setIsSaving(true)
        try {
            await onSave(targetName.trim() || table?.table_name || '')
        } finally {
            setIsSaving(false)
        }
    }

    const handleClose = (e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()
        onClose()
    }

    const handleReset = (e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()
        setTargetName(table?.table_name || '')
    }

    if (!open || !table) return null

    const hasChanged = targetName !== (table.sync_configs?.[0]?.table_name_target || table.table_name)

    return (
        <div
            className={cn(
                "fixed top-2 bottom-2 left-[520px] w-[450px] bg-background border rounded-2xl shadow-2xl flex flex-col",
                "animate-in slide-in-from-left-4 duration-300",
                className
            )}
            style={{ zIndex: 100 }}
            onClick={(e) => e.stopPropagation()}
            onMouseDown={(e) => e.stopPropagation()}
        >
            {/* Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b bg-muted/30 flex-shrink-0">
                <div>
                    <h2 className="text-lg font-semibold">Target Table Name</h2>
                    <p className="text-sm text-muted-foreground">
                        Configure destination table name for <span className="font-medium text-foreground">{table.table_name}</span>
                    </p>
                </div>
                <Button variant="ghost" size="icon" onClick={handleClose} className="h-8 w-8">
                    <X className="h-4 w-4" />
                </Button>
            </div>

            {/* Content */}
            <div className="flex-1 p-6 space-y-6">
                {/* Source Table Info */}
                <div className="p-4 border rounded-lg bg-muted/30">
                    <div className="flex items-center gap-2 mb-2">
                        <Database className="h-4 w-4 text-muted-foreground" />
                        <span className="text-xs font-medium text-muted-foreground uppercase">Source Table</span>
                    </div>
                    <p className="text-sm font-mono">{table.table_name}</p>
                </div>

                {/* Target Name Input */}
                <div className="space-y-2">
                    <label className="text-sm font-medium">Target Table Name</label>
                    <Input
                        value={targetName}
                        onChange={(e) => setTargetName(e.target.value)}
                        onClick={(e) => e.stopPropagation()}
                        placeholder={table.table_name}
                        className="font-mono"
                    />
                    <p className="text-xs text-muted-foreground">
                        This is the name of the table in the destination database. Leave empty to use the source table name.
                    </p>
                </div>

                {/* Changed indicator */}
                {hasChanged && (
                    <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg">
                        <p className="text-xs text-amber-700">
                            <span className="font-medium">Preview:</span> Data from <code className="bg-amber-100 px-1 rounded">{table.table_name}</code> will be synced to <code className="bg-amber-100 px-1 rounded">{targetName}</code>
                        </p>
                    </div>
                )}
            </div>

            {/* Footer */}
            <div className="flex items-center justify-between px-6 py-4 border-t bg-muted/30 flex-shrink-0">
                <Button
                    variant="ghost"
                    size="sm"
                    onClick={handleReset}
                    className="text-muted-foreground"
                >
                    Reset to Default
                </Button>
                <div className="flex gap-2">
                    <Button variant="outline" size="sm" onClick={handleClose}>
                        Cancel
                    </Button>
                    <Button
                        onClick={handleSave}
                        disabled={isSaving}
                        size="sm"
                    >
                        {isSaving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        {!isSaving && <Save className="mr-2 h-4 w-4" />}
                        Save
                    </Button>
                </div>
            </div>
        </div>
    )
}
