import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type SourceTableInfo } from '@/repo/sources'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { Eye } from 'lucide-react'
import { cn } from '@/lib/utils'

export const getSourceDetailsTablesColumns = (
    onUnregister: ((tableName: string) => void) | undefined,
    onViewSchema?: (tableId: number, version: number) => void
): ColumnDef<SourceTableInfo>[] => {

    const columns: ColumnDef<SourceTableInfo>[] = [

        {
            accessorKey: 'table_name',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Table Name' />
            ),
            cell: ({ row }) => (
                <span className='font-medium'>{row.getValue('table_name')}</span>
            ),
            enableSorting: true,
            enableHiding: false,
            meta: { title: 'Table Name' },
        },
        {
            id: 'status',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Pipeline Status' />
            ),
            cell: ({ row }) => {
                const table = row.original

                const Step = ({ label, active }: { label: string, active: boolean }) => (
                    <div className="flex flex-col items-center gap-1">
                        <div
                            className={cn(
                                "h-2 w-2 rounded-full ring-2 ring-offset-2",
                                active
                                    ? "bg-green-500 ring-green-500"
                                    : "bg-muted ring-muted"
                            )}
                            title={active ? `${label} Exists` : `${label} Missing`}
                        />
                        <span className={cn("text-[10px] font-medium uppercase", active ? "text-foreground" : "text-muted-foreground")}>{label}</span>
                    </div>
                )

                const Connector = ({ active }: { active: boolean }) => (
                    <div className={cn("h-[2px] w-4 mb-3.5", active ? "bg-green-500/50" : "bg-muted")} />
                )

                return (
                    <div className="flex items-end gap-0.5 py-2">
                        <Step label="Landing" active={table.is_exists_table_landing} />
                        <Connector active={table.is_exists_table_landing && table.is_exists_stream} />
                        <Step label="Stream" active={table.is_exists_stream} />
                        <Connector active={table.is_exists_stream && table.is_exists_task} />
                        <Step label="Task" active={table.is_exists_task} />
                        <Connector active={table.is_exists_task && table.is_exists_table_destination} />
                        <Step label="Target" active={table.is_exists_table_destination} />
                    </div>
                )
            },
            meta: { title: 'Pipeline Status' },
        },
        {
            id: 'schema_version',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Schema Version' />
            ),
            cell: ({ row }) => {
                const table = row.original
                // Generate list of all versions from 1 to current version
                const versions = Array.from({ length: table.version }, (_, i) => i + 1)

                return (
                    <Select
                        value={table.version.toString()}
                        onValueChange={(value) => onViewSchema?.(table.id, parseInt(value))}
                    >
                        <SelectTrigger className="w-[120px]">
                            <SelectValue placeholder="Version" />
                        </SelectTrigger>
                        <SelectContent>
                            {versions.map((v) => (
                                <SelectItem key={v} value={v.toString()}>
                                    <div className="flex items-center gap-2">
                                        <span>Version {v}</span>
                                        {v === table.version && (
                                            <Badge className="bg-green-100 text-green-800 hover:bg-green-100 dark:bg-green-900/30 dark:text-green-400 h-5 px-1.5">
                                                Active
                                            </Badge>
                                        )}
                                    </div>
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                )
            },
            meta: { title: 'Schema Version' },
        },
        {
            id: 'view_schema',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Schema Details' />
            ),
            cell: ({ row }) => {
                const table = row.original
                return (
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={() => onViewSchema?.(table.id, table.version)}
                    >
                        <Eye className="h-4 w-4 mr-2" />
                        View Schema
                    </Button>
                )
            },
            meta: { title: 'Schema Details' },
        }
    ]

    if (onUnregister) {
        columns.push({
            id: 'actions',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Action' className='w-full justify-center' />
            ),
            cell: ({ row }) => (
                <div className='flex justify-center'>
                    <Button
                        variant='destructive'
                        size='sm'
                        onClick={() => onUnregister(row.original.table_name)}
                    >
                        Drop
                    </Button>
                </div>
            ),
            meta: { title: 'Action' },
        })
    }

    return columns
}
