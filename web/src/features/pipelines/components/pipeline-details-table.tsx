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
import { type SourceTableInfo } from '@/repo/sources'
import { pipelinesRepo } from '@/repo/pipelines'
import { useQuery } from '@tanstack/react-query'
import { getPipelineDetailsTableColumns } from './pipeline-details-table-columns'

interface PipelineDetailsTableProps {
    pipelineId: number
    tables: SourceTableInfo[]
}

export function PipelineDetailsTable({ pipelineId, tables }: PipelineDetailsTableProps) {
    const [rowSelection, setRowSelection] = useState({})
    const [sorting, setSorting] = useState<SortingState>([])
    const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})
    const [globalFilter, setGlobalFilter] = useState('')

    const { data: pipelineStats } = useQuery({
        queryKey: ['pipeline-stats', pipelineId],
        queryFn: () => pipelinesRepo.getStats(pipelineId),
        enabled: !!pipelineId,
        refetchInterval: 5000, // Refetch every 5 seconds
    })

    const statsMap = useMemo(() => {
        if (!pipelineStats) return {}
        return pipelineStats.reduce((acc, stat) => {
            acc[stat.table_name] = stat
            return acc
        }, {} as Record<string, typeof pipelineStats[0]>)
    }, [pipelineStats])

    const columns = useMemo(() => getPipelineDetailsTableColumns(
        statsMap
    ), [statsMap])

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
        </Card>
    )
}
