import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type SourceTableInfo } from '@/repo/sources'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from '@/components/ui/select'

export const getSourceDetailsTablesColumns = (
    onCheckSchema: (table: SourceTableInfo) => void,
    selectedVersions: Record<number, number>,
    onVersionChange: (tableId: number, version: number) => void,
    onUnregister: (tableName: string) => void
): ColumnDef<SourceTableInfo>[] => [

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
        },
        {
            accessorKey: 'is_exists_table_landing',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Landing' className='w-full justify-center' />
            ),
            cell: ({ row }) => {
                const exists = row.getValue('is_exists_table_landing') as boolean
                return (
                    <div className='flex justify-center'>
                        <Checkbox
                            checked={exists}
                            disabled
                            className="border-2 cursor-default opacity-100 data-[state=checked]:bg-primary data-[state=checked]:text-primary-foreground"
                        />
                    </div>
                )
            },
        },
        {
            accessorKey: 'is_exists_task',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Task' className='w-full justify-center' />
            ),
            cell: ({ row }) => {
                const exists = row.getValue('is_exists_task') as boolean
                return (
                    <div className='flex justify-center'>
                        <Checkbox
                            checked={exists}
                            disabled
                            className="border-2 cursor-default opacity-100 data-[state=checked]:bg-primary data-[state=checked]:text-primary-foreground"
                        />
                    </div>
                )
            },
        },
        {
            accessorKey: 'is_exists_stream_table',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Stream' className='w-full justify-center' />
            ),
            cell: ({ row }) => {
                const exists = row.getValue('is_exists_stream_table') as boolean
                return (
                    <div className='flex justify-center'>
                        <Checkbox
                            checked={exists}
                            disabled
                            className="border-2 cursor-default opacity-100 data-[state=checked]:bg-primary data-[state=checked]:text-primary-foreground"
                        />
                    </div>
                )
            },
        },
        {
            accessorKey: 'is_exists_table_destination',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Destination' className='w-full justify-center' />
            ),
            cell: ({ row }) => {
                const exists = row.getValue('is_exists_table_destination') as boolean
                return (
                    <div className='flex justify-center'>
                        <Checkbox
                            checked={exists}
                            disabled
                            className="cursor-default opacity-100 data-[state=checked]:bg-primary data-[state=checked]:text-primary-foreground"
                        />
                    </div>
                )
            },
        },
        {
            accessorKey: 'version',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Version' className='w-full justify-center' />
            ),
            cell: ({ row }) => {
                const maxVersion = row.original.version
                const currentSelected = selectedVersions[row.original.id] || maxVersion
                const versions = Array.from({ length: maxVersion }, (_, i) => maxVersion - i)

                return (
                    <div className='flex justify-center'>
                        <Select
                            value={String(currentSelected)}
                            onValueChange={(value) => onVersionChange(row.original.id, Number(value))}
                        >
                            <SelectTrigger className="h-8 w-[140px]">
                                <SelectValue placeholder="Ver" />
                            </SelectTrigger>
                            <SelectContent>
                                {versions.map((v) => (
                                    <SelectItem key={v} value={String(v)}>
                                        v{v} {v === maxVersion ? '(Active)' : ''}
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>
                )
            },
        },
        {
            accessorKey: 'schema_table',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Schema' className='w-full justify-center' />
            ),
            cell: ({ row }) => (
                <div className='flex justify-center'>
                    <Button
                        variant='outline'
                        size='sm'
                        onClick={() => onCheckSchema(row.original)}
                    >
                        Check Schema
                    </Button>
                </div>
            ),
        },
        {
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
            )
        }
    ]
