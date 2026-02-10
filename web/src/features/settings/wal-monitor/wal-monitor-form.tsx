import { useEffect } from 'react'
import { z } from 'zod'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { configurationRepo } from '@/repo/configuration'
import { Loader2 } from 'lucide-react'
import { toast } from 'sonner'
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

const walMonitorFormSchema = z
  .object({
    warning: z
      .number()
      .min(1, 'Warning threshold must be at least 1 MB')
      .max(1000000, 'Warning threshold is too large'),
    error: z
      .number()
      .min(1, 'Error threshold must be at least 1 MB')
      .max(1000000, 'Error threshold is too large'),
    webhook_url: z.string().url('Please enter a valid URL').or(z.literal('')),
    notification_iteration: z
      .number()
      .min(1, 'Iteration must be at least 1')
      .max(100, 'Iteration cannot exceed 100'),
  })
  .refine((data) => data.error > data.warning, {
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
      queryClient.invalidateQueries({
        queryKey: ['configuration', 'wal-thresholds'],
      })
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
        <div className='grid grid-cols-1 gap-8 lg:grid-cols-2'>
          <div className='space-y-6'>
            <div className='grid grid-cols-1 gap-6 md:grid-cols-2'>
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
                      />
                    </FormControl>
                    <FormDescription>
                      WAL size threshold for warning alerts.
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
                      />
                    </FormControl>
                    <FormDescription>
                      WAL size threshold for error alerts.
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>

            <FormField
              control={form.control}
              name='webhook_url'
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Webhook URL</FormLabel>
                  <div className='flex gap-2'>
                    <FormControl>
                      <Input
                        type='url'
                        placeholder='https://your-webhook-endpoint.com/webhook'
                        {...field}
                      />
                    </FormControl>
                    <Button
                      type='button'
                      variant='secondary'
                      onClick={async () => {
                        const currentWebhookUrl = form.getValues('webhook_url')
                        if (!currentWebhookUrl) {
                          toast.error('Please enter a webhook URL first')
                          return
                        }
                        try {
                          await configurationRepo.testNotification(currentWebhookUrl)
                          toast.success('Test notification sent successfully')
                        } catch (e: any) {
                          toast.error(e.response?.data?.detail || 'Failed to trigger test notification')
                        }
                      }}
                    >
                      Test
                    </Button>
                  </div>
                  <FormDescription>
                    Webhook URL for alert notifications. Leave empty to disable.
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
                      className='max-w-50'
                    />
                  </FormControl>
                  <FormDescription>
                    Number of check iterations before sending a notification.
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />

            <div className='pt-4'>
              <Button type='submit' disabled={updateMutation.isPending}>
                {updateMutation.isPending && (
                  <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                )}
                Save Changes
              </Button>
            </div>
          </div>

          <div>
            <div className='sticky top-6 rounded-xl border bg-card p-6 shadow-sm'>
              <div className='mb-4 flex items-center gap-2 text-muted-foreground'>
                <div className='h-2 w-2 animate-pulse rounded-full bg-green-500' />
                <h3 className='text-xs font-medium tracking-wider uppercase'>
                  Example Payload
                </h3>
              </div>
              <p className='mb-4 text-sm leading-relaxed text-muted-foreground'>
                Notifications are sent as JSON POST requests. Ensure your
                endpoint can parse the following structure:
              </p>
              <div className='overflow-hidden rounded-lg border bg-zinc-950 p-4'>
                <pre className='custom-scrollbar overflow-auto font-mono text-[10px] text-zinc-50 sm:text-xs'>
                  {JSON.stringify(
                    {
                      key_notification: 'WAL_SIZE_WARNING',
                      title: 'WAL Size Warning',
                      message: 'WAL size exceeded 3000MB.',
                      type: 'WARNING',
                      timestamp: '2024-01-01T12:00:00+07:00',
                    },
                    null,
                    2
                  )}
                </pre>
              </div>
            </div>
          </div>
        </div>
      </form>
    </Form>
  )
}
