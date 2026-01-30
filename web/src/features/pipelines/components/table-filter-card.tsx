import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Input } from '@/components/ui/input'
import { Loader2, Plus, Save, X, Calendar as CalendarIcon, Trash2 } from 'lucide-react'
import { TableWithSyncInfo } from '@/repo/pipelines'
import { cn } from '@/lib/utils'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Calendar } from '@/components/ui/calendar'
import { format } from 'date-fns'
import { ScrollArea } from '@/components/ui/scroll-area'

interface TableFilterCardProps {
    table: TableWithSyncInfo | null
    open: boolean
    onClose: () => void
    onSave: (filterSql: string) => Promise<void>
    className?: string
}

interface FilterClause {
    id: string
    column: string
    operator: string
    value: string
    value2?: string
}

const OPERATORS = [
    { value: '=', label: '= (Equals)' },
    { value: '!=', label: '!= (Not Equals)' },
    { value: '>', label: '> (Greater Than)' },
    { value: '<', label: '< (Less Than)' },
    { value: '>=', label: '>= (Greater or Equal)' },
    { value: '<=', label: '<= (Less or Equal)' },
    { value: 'LIKE', label: 'LIKE (Contains)' },
    { value: 'BETWEEN', label: 'BETWEEN' },
]

export function TableFilterCard({
    table,
    open,
    onClose,
    onSave,
    className
}: TableFilterCardProps) {
    const [filters, setFilters] = useState<FilterClause[]>([])
    const [isSaving, setIsSaving] = useState(false)

    const parseFilterSql = (sql: string): FilterClause[] => {
        if (!sql) return []

        const clauses: FilterClause[] = []

        // Use matchAll to find all filter patterns in the string
        // Patterns:
        // 1. BETWEEN: (\w+) BETWEEN '([^']*)' AND '([^']*)'
        // 2. LIKE: (\w+) LIKE '%([^%]*)%'
        // 3. Standard: (\w+) (=|!=|>|>=|<=|<) ('([^']*)'|(\d+(?:\.\d+)?))

        const pattern = /(?:(\w+)\s+BETWEEN\s+'([^']*)'\s+AND\s+'([^']*)')|(?:(\w+)\s+LIKE\s+'%([^%]*)%')|(?:(\w+)\s*(=|!=|>|>=|<=|<)\s*(?:'([^']*)'|(\d+(?:\.\d+)?)))/gi

        const matches = sql.matchAll(pattern)

        for (const match of matches) {
            const id = typeof crypto !== 'undefined' && crypto.randomUUID
                ? crypto.randomUUID()
                : Math.random().toString(36).substring(2) + Date.now().toString(36)

            // Check which group matched
            if (match[1]) { // BETWEEN
                clauses.push({
                    id,
                    column: match[1],
                    operator: 'BETWEEN',
                    value: match[2],
                    value2: match[3]
                })
            } else if (match[4]) { // LIKE
                clauses.push({
                    id,
                    column: match[4],
                    operator: 'LIKE',
                    value: match[5]
                })
            } else if (match[6]) { // Standard
                clauses.push({
                    id,
                    column: match[6],
                    operator: match[7],
                    // Use the quoted string match (match[8]) or the numeric match (match[9])
                    value: match[8] !== undefined ? match[8] : match[9]
                })
            }
        }

        return clauses
    }

    // Load filters from sync_config or reset when table changes
    useEffect(() => {
        if (open && table?.sync_configs?.[0]?.filter_sql) {
            setFilters(parseFilterSql(table.sync_configs[0].filter_sql))
        } else if (!open) {
            // Keep filters while open even if table name changes (shouldn't happen in this view)
        } else {
            setFilters([])
        }
    }, [table?.table_name, table?.sync_configs, open])

    const columns = table?.columns || []

    const getColumnType = (colName: string) => {
        const col = columns.find(c => c.column_name === colName)
        if (!col) return 'text'
        // Check real_data_type first (from schema), fallback to data_type
        const type = (col.real_data_type || col.data_type || '').toLowerCase()

        const numericTypes = [
            'int', 'integer', 'smallint', 'bigint', 'int2', 'int4', 'int8',
            'decimal', 'numeric', 'real', 'double precision', 'float', 'float4', 'float8',
            'number', 'money'
        ]
        if (numericTypes.some(t => type.includes(t))) return 'numeric'

        // Timestamp types that include time component
        const datetimeTypes = [
            'timestamp', 'timestamptz', 'datetime', 'timestamp with time zone',
            'timestamp without time zone'
        ]
        if (datetimeTypes.some(t => type.includes(t))) return 'datetime'

        // Date-only type
        if (type.includes('date')) return 'date'

        // Time-only type (treat as text input)
        if (type.includes('time')) return 'text'

        return 'text'
    }

    const handleAddFilter = (e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()
        console.log("Add Filter Clicked", columns)

        const firstColumn = columns.length > 0 ? columns[0].column_name : ''

        // Generate a random ID (fallback for crypto.randomUUID if not available)
        const newId = typeof crypto !== 'undefined' && crypto.randomUUID
            ? crypto.randomUUID()
            : Math.random().toString(36).substring(2) + Date.now().toString(36)

        setFilters(prev => [
            ...prev,
            {
                id: newId,
                column: firstColumn,
                operator: '=',
                value: ''
            }
        ])
    }

    const handleRemoveFilter = (id: string, e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()
        setFilters(prev => prev.filter(f => f.id !== id))
    }

    const updateFilter = (id: string, field: keyof FilterClause, val: string) => {
        setFilters(prev => prev.map(f => {
            if (f.id === id) {
                if (field === 'column' && val !== f.column) {
                    return { ...f, [field]: val, value: '', value2: undefined }
                }
                if (field === 'operator' && val !== 'BETWEEN') {
                    return { ...f, [field]: val, value2: undefined }
                }
                return { ...f, [field]: val }
            }
            return f
        }))
    }

    const handleSave = async (e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()

        const clauses = filters.map(f => {
            if (!f.column || !f.value) return null

            const colType = getColumnType(f.column)
            const quote = (val: string) =>
                colType === 'text' || colType === 'date' || colType === 'datetime' ? `'${val}'` : val

            if (f.operator === 'BETWEEN' && f.value && f.value2) {
                return `${f.column} BETWEEN ${quote(f.value)} AND ${quote(f.value2)}`
            }

            if (f.operator === 'LIKE') {
                return `${f.column} LIKE '%${f.value}%'`
            }

            return `${f.column} ${f.operator} ${quote(f.value)}`
        }).filter(Boolean)

        const sql = clauses.join(' AND ')

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
            <div className="flex items-center justify-between px-6 py-4 border-b bg-muted/30 flex-shrink-0">
                <div>
                    <h2 className="text-lg font-semibold">Filter Table</h2>
                    <p className="text-sm text-muted-foreground">
                        Configure filters for <span className="font-medium text-foreground">{table.table_name}</span>
                    </p>
                </div>
                <Button variant="ghost" size="icon" onClick={handleClose} className="h-8 w-8">
                    <X className="h-4 w-4" />
                </Button>
            </div>

            {/* Content */}
            <ScrollArea className="flex-1">
                <div className="p-6 space-y-4">
                    {/* Filter List */}
                    {filters.map((filter, index) => {
                        const colType = getColumnType(filter.column)

                        return (
                            <div
                                key={filter.id}
                                className="p-4 border rounded-lg bg-card"
                            >
                                <div className="flex items-center justify-between mb-3">
                                    <span className="text-xs font-medium text-muted-foreground">Filter #{index + 1}</span>
                                    <Button
                                        variant="ghost"
                                        size="icon"
                                        className="h-6 w-6 text-muted-foreground hover:text-destructive"
                                        onClick={(e) => handleRemoveFilter(filter.id, e)}
                                    >
                                        <Trash2 className="h-3.5 w-3.5" />
                                    </Button>
                                </div>

                                {/* Layout: Column | Operator | Val1 | [Val2 if BETWEEN] */}
                                <div className={cn("grid gap-2", filter.operator === 'BETWEEN' ? "grid-cols-[1.2fr_0.8fr_2fr_2fr]" : "grid-cols-[1.2fr_0.8fr_4fr]")}>
                                    {/* Column Dropdown */}
                                    <div>
                                        <label className="text-[10px] font-medium text-muted-foreground uppercase mb-1 block">Column</label>
                                        <Select
                                            value={filter.column}
                                            onValueChange={(val) => updateFilter(filter.id, 'column', val)}
                                        >
                                            <SelectTrigger className="h-9 text-xs">
                                                <SelectValue placeholder="Select..." />
                                            </SelectTrigger>
                                            <SelectContent style={{ zIndex: 1000 }}>
                                                {columns.length > 0 ? (
                                                    columns.map(col => (
                                                        <SelectItem key={col.column_name} value={col.column_name}>
                                                            <span className="text-xs">{col.column_name}</span>
                                                        </SelectItem>
                                                    ))
                                                ) : (
                                                    <SelectItem value="_none" disabled>No columns</SelectItem>
                                                )}
                                            </SelectContent>
                                        </Select>
                                    </div>

                                    {/* Operator Dropdown */}
                                    <div>
                                        <label className="text-[10px] font-medium text-muted-foreground uppercase mb-1 block">Operator</label>
                                        <Select
                                            value={filter.operator}
                                            onValueChange={(val) => updateFilter(filter.id, 'operator', val)}
                                        >
                                            <SelectTrigger className="h-9 text-xs">
                                                <SelectValue />
                                            </SelectTrigger>
                                            <SelectContent style={{ zIndex: 1000 }}>
                                                {OPERATORS.map(op => (
                                                    <SelectItem key={op.value} value={op.value}>
                                                        <span className="text-xs">{op.label}</span>
                                                    </SelectItem>
                                                ))}
                                            </SelectContent>
                                        </Select>
                                    </div>

                                    {/* Value Input */}
                                    <div>
                                        <label className="text-[10px] font-medium text-muted-foreground uppercase mb-1 block">Value</label>
                                        {colType === 'date' ? (
                                            <DatePicker
                                                value={filter.value}
                                                onChange={(v) => updateFilter(filter.id, 'value', v)}
                                            />
                                        ) : colType === 'datetime' ? (
                                            <DateTimePicker
                                                value={filter.value}
                                                onChange={(v) => updateFilter(filter.id, 'value', v)}
                                            />
                                        ) : (
                                            <Input
                                                value={filter.value}
                                                onChange={(e) => {
                                                    let val = e.target.value;
                                                    if (colType === 'numeric') {
                                                        val = val.replace(/[^0-9.-]/g, '');
                                                        const parts = val.split('.');
                                                        if (parts.length > 2) val = parts[0] + '.' + parts.slice(1).join('');
                                                        if (val.indexOf('-') > 0) val = val.slice(0, 1) + val.slice(1).replace(/-/g, '');
                                                    }
                                                    updateFilter(filter.id, 'value', val);
                                                }}
                                                onClick={(e) => e.stopPropagation()}
                                                className="h-9 text-xs"
                                                type="text"
                                                inputMode={colType === 'numeric' ? 'decimal' : 'text'}
                                                placeholder={colType === 'numeric' ? '0' : 'Value'}
                                            />
                                        )}
                                    </div>

                                    {/* Second Value for BETWEEN */}
                                    {filter.operator === 'BETWEEN' && (
                                        <div>
                                            <label className="text-[10px] font-medium text-muted-foreground uppercase mb-1 block">To Value</label>
                                            {colType === 'date' ? (
                                                <DatePicker
                                                    value={filter.value2 || ''}
                                                    onChange={(v) => updateFilter(filter.id, 'value2', v)}
                                                />
                                            ) : colType === 'datetime' ? (
                                                <DateTimePicker
                                                    value={filter.value2 || ''}
                                                    onChange={(v) => updateFilter(filter.id, 'value2', v)}
                                                />
                                            ) : (
                                                <Input
                                                    value={filter.value2 || ''}
                                                    onChange={(e) => {
                                                        let val = e.target.value;
                                                        if (colType === 'numeric') {
                                                            val = val.replace(/[^0-9.-]/g, '');
                                                            const parts = val.split('.');
                                                            if (parts.length > 2) val = parts[0] + '.' + parts.slice(1).join('');
                                                            if (val.indexOf('-') > 0) val = val.slice(0, 1) + val.slice(1).replace(/-/g, '');
                                                        }
                                                        updateFilter(filter.id, 'value2', val);
                                                    }}
                                                    onClick={(e) => e.stopPropagation()}
                                                    className="h-9 text-xs"
                                                    type="text"
                                                    inputMode={colType === 'numeric' ? 'decimal' : 'text'}
                                                    placeholder={colType === 'numeric' ? '0' : 'Value'}
                                                />
                                            )}
                                        </div>
                                    )}
                                </div>
                            </div>
                        )
                    })}

                    {/* Empty State */}
                    {filters.length === 0 && (
                        <div className="text-center py-8 text-muted-foreground border-2 border-dashed rounded-lg">
                            <p className="text-sm font-medium mb-1">No filters configured</p>
                            <p className="text-xs">Click the button below to add a filter</p>
                        </div>
                    )}

                    {/* Add Filter Button */}
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={handleAddFilter}
                        onMouseDown={(e) => e.stopPropagation()}
                        className="w-full"
                    >
                        <Plus className="mr-2 h-4 w-4" /> Add Filter
                    </Button>

                    {/* Debug info */}
                    {columns.length === 0 && (
                        <p className="text-xs text-amber-600 mt-2">
                            Note: No columns available for this table. Filters may not work correctly.
                        </p>
                    )}
                </div>
            </ScrollArea>

            {/* Footer */}
            <div className="flex items-center justify-between px-6 py-4 border-t bg-muted/30 flex-shrink-0">
                <Button
                    variant="ghost"
                    size="sm"
                    onClick={(e) => { e.stopPropagation(); setFilters([]) }}
                    className="text-muted-foreground"
                >
                    Reset All
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
                        Save Filters
                    </Button>
                </div>
            </div>
        </div>
    )
}

function DatePicker({ value, onChange }: { value: string, onChange: (val: string) => void }) {
    return (
        <Popover>
            <PopoverTrigger asChild>
                <Button
                    variant={"outline"}
                    className={cn(
                        "w-full h-9 justify-start text-left font-normal text-xs",
                        !value && "text-muted-foreground"
                    )}
                    onClick={(e) => e.stopPropagation()}
                >
                    <CalendarIcon className="mr-2 h-3 w-3" />
                    {value ? format(new Date(value), 'PP') : <span>Pick date</span>}
                </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="start" style={{ zIndex: 1000 }}>
                <Calendar
                    mode="single"
                    selected={value ? new Date(value) : undefined}
                    onSelect={(date) => date && onChange(format(date, 'yyyy-MM-dd'))}
                    initialFocus
                />
            </PopoverContent>
        </Popover>
    )
}

function DateTimePicker({ value, onChange }: { value: string, onChange: (val: string) => void }) {
    // Parse existing value or use defaults
    const parseValue = () => {
        if (!value) return { date: undefined, time: '00:00:00' }
        const parts = value.split(' ')
        const datePart = parts[0]
        const timePart = parts[1] || '00:00:00'
        return {
            date: datePart ? new Date(datePart) : undefined,
            time: timePart
        }
    }

    const { date, time } = parseValue()

    const handleDateChange = (newDate: Date | undefined) => {
        if (newDate) {
            const formattedDate = format(newDate, 'yyyy-MM-dd')
            onChange(`${formattedDate} ${time}`)
        }
    }

    const handleTimeChange = (newTime: string) => {
        if (date) {
            const formattedDate = format(date, 'yyyy-MM-dd')
            onChange(`${formattedDate} ${newTime}`)
        } else {
            // If no date selected, just store time with placeholder date
            onChange(`1970-01-01 ${newTime}`)
        }
    }

    return (
        <Popover>
            <PopoverTrigger asChild>
                <Button
                    variant={"outline"}
                    className={cn(
                        "w-full h-9 justify-start text-left font-normal text-xs",
                        !value && "text-muted-foreground"
                    )}
                    onClick={(e) => e.stopPropagation()}
                >
                    <CalendarIcon className="mr-2 h-3 w-3" />
                    {value ? value : <span>Pick date & time</span>}
                </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-3" align="start" style={{ zIndex: 1000 }}>
                <Calendar
                    mode="single"
                    selected={date}
                    onSelect={handleDateChange}
                    initialFocus
                />
                <div className="border-t mt-3 pt-3">
                    <label className="text-xs font-medium text-muted-foreground mb-1 block">Time</label>
                    <Input
                        type="time"
                        step="1"
                        value={time}
                        onChange={(e) => handleTimeChange(e.target.value || '00:00:00')}
                        onClick={(e) => e.stopPropagation()}
                        className="h-8 text-xs"
                    />
                </div>
            </PopoverContent>
        </Popover>
    )
}
