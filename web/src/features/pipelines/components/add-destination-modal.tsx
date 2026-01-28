import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { destinationsRepo } from '@/repo/destinations'
import { pipelinesRepo } from '@/repo/pipelines'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { toast } from 'sonner'
import { z } from 'zod'

const formSchema = z.object({
  destination_id: z.string().min(1, 'Destination is required'),
})

interface AddDestinationModalProps {
  open: boolean
  setOpen: (open: boolean) => void
  pipelineId: number
  existingDestinationIds: Set<number>
}

export function AddDestinationModal({
  open,
  setOpen,
  pipelineId,
  existingDestinationIds,
}: AddDestinationModalProps) {
  const queryClient = useQueryClient()
  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      destination_id: '',
    },
  })

  // Fetch destinations
  const { data: destinations } = useQuery({
    queryKey: ['destinations'],
    queryFn: destinationsRepo.getAll,
  })

  const { mutate, isPending } = useMutation({
    mutationFn: (values: z.infer<typeof formSchema>) =>
      pipelinesRepo.addDestination(pipelineId, parseInt(values.destination_id)),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['pipeline', pipelineId] })
      setOpen(false)
      form.reset()
      toast.success('Destination added successfully')
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.message || 'Failed to add destination')
    },
  })

  function onSubmit(values: z.infer<typeof formSchema>) {
    mutate(values)
  }

  useEffect(() => {
    if (!open) {
      form.reset()
    }
  }, [open, form])

  // Filter out already added destinations
  const availableDestinations = destinations?.destinations.filter(
    (d) => !existingDestinationIds.has(d.id)
  )

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className='sm:max-w-[425px]'>
        <DialogHeader>
          <DialogTitle>Add Destination</DialogTitle>
          <DialogDescription>
            Add a destination to this pipeline.
          </DialogDescription>
        </DialogHeader>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className='space-y-4'>
            <FormField
              control={form.control}
              name='destination_id'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Destination</FormLabel>
                  <Select
                    onValueChange={field.onChange}
                    defaultValue={field.value}
                  >
                    <FormControl>
                      <SelectTrigger>
                        <SelectValue placeholder='Select a destination' />
                      </SelectTrigger>
                    </FormControl>
                    <SelectContent>
                      {availableDestinations?.map((dest) => (
                        <SelectItem key={dest.id} value={dest.id.toString()}>
                          {dest.name} ({dest.type})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <FormMessage />
                </FormItem>
              )}
            />
            <DialogFooter>
              <Button type='submit' disabled={isPending}>
                {isPending ? 'Adding...' : 'Add'}
              </Button>
            </DialogFooter>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  )
}
