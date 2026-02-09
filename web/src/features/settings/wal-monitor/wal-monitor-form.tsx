import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { Button } from '@/components/ui/button'
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { configurationRepo } from '@/repo/configuration'
import { toast } from 'sonner'
import { Loader2 } from 'lucide-react'
import { useEffect } from 'react'

const walMonitorFormSchema = z.object({
  warning: z.number()
    .min(1, 'Warning threshold must be at least 1 MB')
    .max(1000000, 'Warning threshold is too large'),
  error: z.number()
    .min(1, 'Error threshold must be at least 1 MB')
    .max(1000000, 'Error threshold is too large'),
  webhook_url: z.string().url('Please enter a valid URL').or(z.literal('')),
  notification_iteration: z.number()
    .min(1, 'Iteration must be at least 1')
    .max(100, 'Iteration cannot exceed 100'),
}).refine((data) => data.error > data.warning, {
  message: 'Error threshold must be greater than warning threshold',
  path: ['error'],
})

type WALMonitorFormValues = z.infer<typeof walMonitorFormSchema>

export function WALMonitorForm() {
  const queryClient = useQueryClient()

  const { data: config, isLoading } = useQuery({
    queryKey: ['configuration', 'wal-thresholds'],
    queryFn: configurationRepo.getWALThresholds,
  })

  const defaultValues: WALMonitorFormValues = {
    warning: 3000,
    error: 6000,
    webhook_url: '',
    notification_iteration: 3,
  }

  const form = useForm<WALMonitorFormValues>({
    resolver: zodResolver(walMonitorFormSchema),
    defaultValues,
  })

  // Update form values when config data changes
  useEffect(() => {
    if (config) {
      form.reset({
        warning: config.warning,
        error: config.error,
        webhook_url: config.webhook_url,
        notification_iteration: config.notification_iteration,
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [config])

  const updateMutation = useMutation({
    mutationFn: configurationRepo.updateWALThresholds,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['configuration', 'wal-thresholds'] })
      // Update form with the saved values
      form.reset({
        warning: data.warning,
        error: data.error,
        webhook_url: data.webhook_url,
        notification_iteration: data.notification_iteration,
      })
      toast.success('WAL Monitor settings updated successfully')
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to update WAL Monitor settings')
    },
  })

  function onSubmit(data: WALMonitorFormValues) {
    updateMutation.mutate(data)
  }

  if (isLoading) {
    return (
      <div className='flex items-center justify-center p-8'>
        <Loader2 className='h-8 w-8 animate-spin text-muted-foreground' />
      </div>
    )
  }

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className='space-y-8'>
        <FormField
          control={form.control}
          name='warning'
          render={({ field }) => (
            <FormItem>
              <FormLabel>Warning Threshold (MB)</FormLabel>
              <FormControl>
                <Input
                  type='number'
                  placeholder='3000'
                  {...field}
                  onChange={(e) => field.onChange(e.target.valueAsNumber)}
                  className='max-w-xs'
                />
              </FormControl>
              <FormDescription>
                WAL size threshold for warning alerts. Values are in MB.
              </FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name='error'
          render={({ field }) => (
            <FormItem>
              <FormLabel>Error Threshold (MB)</FormLabel>
              <FormControl>
                <Input
                  type='number'
                  placeholder='6000'
                  {...field}
                  onChange={(e) => field.onChange(e.target.valueAsNumber)}
                  className='max-w-xs'
                />
              </FormControl>
              <FormDescription>
                WAL size threshold for error alerts. Must be greater than warning threshold. Values are in MB.
              </FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name='webhook_url'
          render={({ field }) => (
            <FormItem>
              <FormLabel>Webhook URL</FormLabel>
              <FormControl>
                <Input
                  type='url'
                  placeholder='https://your-webhook-endpoint.com/alerts'
                  {...field}
                />
              </FormControl>
              <FormDescription>
                Webhook URL for alert notifications. Leave empty to disable notifications.
                Alerts will be sent when WAL size reaches warning or error thresholds.
              </FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name='notification_iteration'
          render={({ field }) => (
            <FormItem>
              <FormLabel>Notification Iteration</FormLabel>
              <FormControl>
                <Input
                  type='number'
                  placeholder='3'
                  {...field}
                  onChange={(e) => field.onChange(e.target.valueAsNumber)}
                  className='max-w-xs'
                />
              </FormControl>
              <FormDescription>
                Number of check iterations before sending a notification (defaults to 3).
              </FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
        <Button type='submit' disabled={updateMutation.isPending}>
          {updateMutation.isPending && (
            <Loader2 className='mr-2 h-4 w-4 animate-spin' />
          )}
          Update settings
        </Button>
      </form>
    </Form>
  )
}
