import { useState } from 'react'
import { useParams } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { sourcesRepo, Preset } from '@/repo/sources'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Loader2, Trash2, Search } from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogHeader,
    DialogTitle,
} from '@/components/ui/dialog'

export function SourceDetailsPresets() {
    const { sourceId } = useParams({ from: '/_authenticated/sources/$sourceId/details' })
    const id = parseInt(sourceId)
    const queryClient = useQueryClient()
    const [viewPreset, setViewPreset] = useState<Preset | null>(null)

    const { data: presets, isLoading } = useQuery({
        queryKey: ['source-presets', id],
        queryFn: () => sourcesRepo.getPresets(id),
        enabled: !!id,
    })

    const deletePresetMutation = useMutation({
        mutationFn: async (presetId: number) => {
            await sourcesRepo.deletePreset(presetId)
        },
        onSuccess: () => {
            toast.success("Preset deleted successfully")
            queryClient.invalidateQueries({ queryKey: ['source-presets', id] })
        },
        onError: () => {
            toast.error("Failed to delete preset")
        }
    })

    if (isLoading) {
        return <div className="flex justify-center p-8"><Loader2 className="h-8 w-8 animate-spin" /></div>
    }

    return (
        <>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {presets?.length === 0 && (
                    <div className="col-span-full text-center text-muted-foreground p-8 border border-dashed rounded-md">
                        No presets found. Save tables from the "List Tables" tab to create a preset.
                    </div>
                )}
                {presets?.map(preset => (
                    <Card key={preset.id}>
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">
                                {preset.name}
                            </CardTitle>
                            <div className="flex gap-1">
                                <Button
                                    variant="ghost"
                                    size="icon"
                                    onClick={() => setViewPreset(preset)}
                                >
                                    <Search className="h-4 w-4" />
                                </Button>
                                <Button
                                    variant="ghost"
                                    size="icon"
                                    className="text-destructive hover:bg-destructive/10"
                                    onClick={() => deletePresetMutation.mutate(preset.id)}
                                    disabled={deletePresetMutation.isPending}
                                >
                                    <Trash2 className="h-4 w-4" />
                                </Button>
                            </div>
                        </CardHeader>
                        <CardContent>
                            <div className="text-2xl font-bold">{preset.table_names.length} Tables</div>
                            <p className="text-xs text-muted-foreground mt-1">
                                {new Date(preset.created_at).toLocaleDateString()}
                            </p>
                            <div className="mt-4 flex flex-wrap gap-1">
                                {preset.table_names.slice(0, 5).map(name => (
                                    <Badge key={name} variant="secondary" className="text-[10px]">{name}</Badge>
                                ))}
                                {preset.table_names.length > 5 && (
                                    <Badge variant="outline" className="text-[10px]">+{preset.table_names.length - 5} more</Badge>
                                )}
                            </div>
                        </CardContent>
                    </Card>
                ))}
            </div>

            <Dialog open={!!viewPreset} onOpenChange={(open) => !open && setViewPreset(null)}>
                <DialogContent className="sm:max-w-[600px]">
                    <DialogHeader>
                        <DialogTitle>Preset Details: {viewPreset?.name}</DialogTitle>
                        <DialogDescription>
                            Contains {viewPreset?.table_names.length} tables.
                        </DialogDescription>
                    </DialogHeader>
                    <div className="max-h-[60vh] overflow-y-auto mt-4 pr-2">
                        <div className="grid grid-cols-2 gap-2">
                            {viewPreset?.table_names.map(name => (
                                <div key={name} className="flex items-center p-2 rounded-md border text-sm">
                                    {name}
                                </div>
                            ))}
                        </div>
                    </div>
                </DialogContent>
            </Dialog>
        </>
    )
}
