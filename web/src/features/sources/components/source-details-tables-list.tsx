import { useState, useMemo } from 'react'
import {
    type SortingState,
    type VisibilityState,
    flexRender,
    getCoreRowModel,
    getFacetedRowModel,
    getFacetedUniqueValues,
    getFilteredRowModel,
    getPaginationRowModel,
    getSortedRowModel,
    useReactTable,
} from '@tanstack/react-table'
import { cn } from '@/lib/utils'
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTablePagination, DataTableToolbar } from '@/components/data-table'
import { type SourceTableInfo, sourcesRepo } from '@/repo/sources'
import { getSourceDetailsTablesColumns } from './source-details-tables-columns'
import { SourceDetailsSchemaDrawer } from './source-details-schema-drawer'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Loader2 } from 'lucide-react'
import {
    AlertDialog,
    AlertDialogAction,
    AlertDialogCancel,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
} from '@/components/ui/alert-dialog'

interface SourceDetailsTablesListProps {
    sourceId: number
    tables: SourceTableInfo[]
}

export function SourceDetailsTablesList({ sourceId, tables }: SourceDetailsTablesListProps) {
    const [rowSelection, setRowSelection] = useState({})
    const [sorting, setSorting] = useState<SortingState>([])
    const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})
    const [globalFilter, setGlobalFilter] = useState('')

    const [drawerOpen, setDrawerOpen] = useState(false)
    const [selectedTable, setSelectedTable] = useState<SourceTableInfo | null>(null)
    const [selectedVersions, setSelectedVersions] = useState<Record<number, number>>({})
    const [fetchedSchema, setFetchedSchema] = useState<SourceTableInfo['schema_table']>([])
    const [isLoadingSchema, setIsLoadingSchema] = useState(false)

    // New state for drop confirmation
    const [tableToDrop, setTableToDrop] = useState<string | null>(null)
    const [isProcessingDrop, setIsProcessingDrop] = useState(false)

    const queryClient = useQueryClient()

    const handleVersionChange = (tableId: number, version: number) => {
        setSelectedVersions(prev => ({ ...prev, [tableId]: version }))
    }

    const handleCheckSchema = async (table: SourceTableInfo) => {
        setSelectedTable(table)
        setDrawerOpen(true)
        setIsLoadingSchema(true)
        try {
            const version = selectedVersions[table.id] || table.version
            const schema = await sourcesRepo.getTableSchema(table.id, version)
            setFetchedSchema(schema)
        } catch (error) {
            console.error("Failed to fetch schema", error)
            setFetchedSchema([])
        } finally {
            setIsLoadingSchema(false)
        }
    }



    const handleUnregisterTable = async (tableName: string) => {
        setTableToDrop(tableName)
    }

    const confirmDropTable = async (tableName: string) => {
        setIsProcessingDrop(true)
        try {
            await sourcesRepo.unregisterTable(sourceId, tableName)
            // Auto-refresh after drop
            await sourcesRepo.refreshSource(sourceId)
            toast.success(`Table ${tableName} dropped from publication successfully`)
            queryClient.invalidateQueries({ queryKey: ['source-details', sourceId] })
            setTableToDrop(null)
        } catch (error) {
            toast.error(`Failed to drop table ${tableName}`)
            console.error(error)
        } finally {
            setIsProcessingDrop(false)
        }
    }

    const columns = useMemo(() => getSourceDetailsTablesColumns(handleCheckSchema, selectedVersions, handleVersionChange, handleUnregisterTable), [selectedVersions])

    const table = useReactTable({
        data: tables,
        columns,
        state: {
            sorting,
            columnVisibility,
            rowSelection,
            globalFilter,
        },
        enableRowSelection: true,
        onRowSelectionChange: setRowSelection,
        onSortingChange: setSorting,
        onColumnVisibilityChange: setColumnVisibility,
        onGlobalFilterChange: setGlobalFilter,
        globalFilterFn: (row, _columnId, filterValue) => {
            const name = String(row.getValue('table_name')).toLowerCase()
            const searchValue = String(filterValue).toLowerCase()
            return name.includes(searchValue)
        },
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getPaginationRowModel: getPaginationRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFacetedRowModel: getFacetedRowModel(),
        getFacetedUniqueValues: getFacetedUniqueValues(),
    })

    return (
        <Card>
            <CardHeader>
                <CardTitle>Tables</CardTitle>
            </CardHeader>
            <CardContent>
                <div className='flex flex-1 flex-col gap-4'>
                    <DataTableToolbar
                        table={table}
                        searchPlaceholder='Filter by table name...'
                    />
                    <div className='rounded-md border'>
                        <Table>
                            <TableHeader>
                                {table.getHeaderGroups().map((headerGroup) => (
                                    <TableRow key={headerGroup.id}>
                                        {headerGroup.headers.map((header) => {
                                            return (
                                                <TableHead
                                                    key={header.id}
                                                    colSpan={header.colSpan}
                                                    className={cn(
                                                        header.column.columnDef.meta?.className,
                                                        header.column.columnDef.meta?.thClassName
                                                    )}
                                                >
                                                    {header.isPlaceholder
                                                        ? null
                                                        : flexRender(
                                                            header.column.columnDef.header,
                                                            header.getContext()
                                                        )}
                                                </TableHead>
                                            )
                                        })}
                                    </TableRow>
                                ))}
                            </TableHeader>
                            <TableBody>
                                {table.getRowModel().rows?.length ? (
                                    table.getRowModel().rows.map((row) => (
                                        <TableRow
                                            key={row.id}
                                            data-state={row.getIsSelected() && 'selected'}
                                        >
                                            {row.getVisibleCells().map((cell) => (
                                                <TableCell
                                                    key={cell.id}
                                                    className={cn(
                                                        cell.column.columnDef.meta?.className,
                                                        cell.column.columnDef.meta?.tdClassName
                                                    )}
                                                >
                                                    {flexRender(
                                                        cell.column.columnDef.cell,
                                                        cell.getContext()
                                                    )}
                                                </TableCell>
                                            ))}
                                        </TableRow>
                                    ))
                                ) : (
                                    <TableRow>
                                        <TableCell
                                            colSpan={columns.length}
                                            className='h-24 text-center'
                                        >
                                            No results.
                                        </TableCell>
                                    </TableRow>
                                )}
                            </TableBody>
                        </Table>
                    </div>
                    <DataTablePagination table={table} />
                </div>
            </CardContent>

            {selectedTable && (
                <SourceDetailsSchemaDrawer
                    open={drawerOpen}
                    onOpenChange={setDrawerOpen}
                    tableName={selectedTable.table_name}
                    schema={fetchedSchema || []}
                    isLoading={isLoadingSchema}
                    version={selectedVersions[selectedTable.id] || selectedTable.version}
                />
            )}

            <AlertDialog open={!!tableToDrop} onOpenChange={(open) => !open && setTableToDrop(null)}>
                <AlertDialogContent>
                    <AlertDialogHeader>
                        <AlertDialogTitle>Are you sure?</AlertDialogTitle>
                        <AlertDialogDescription>
                            This will drop the table <strong>{tableToDrop}</strong> from the publication. This action cannot be undone immediately.
                        </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                        <AlertDialogCancel disabled={isProcessingDrop}>Cancel</AlertDialogCancel>
                        <AlertDialogAction
                            onClick={(e) => {
                                e.preventDefault()
                                if (tableToDrop) confirmDropTable(tableToDrop)
                            }}
                            className="text-white  hover:bg-destructive/90"
                            disabled={isProcessingDrop}
                        >
                            {isProcessingDrop && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                            Drop
                        </AlertDialogAction>
                    </AlertDialogFooter>
                </AlertDialogContent>
            </AlertDialog>
        </Card>
    )
}
