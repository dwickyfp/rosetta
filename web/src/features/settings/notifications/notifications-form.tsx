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

const notificationsFormSchema = z.object({
  webhook_url: z.string().url('Please enter a valid URL').or(z.literal('')),
  notification_iteration: z
    .number()
    .min(1, 'Iteration must be at least 1')
    .max(100, 'Iteration cannot exceed 100'),
})

type NotificationsFormValues = z.infer<typeof notificationsFormSchema>

export function NotificationsForm() {
  const queryClient = useQueryClient()

  const { data: config, isLoading } = useQuery({
    queryKey: ['configuration', 'wal-thresholds'],
    queryFn: configurationRepo.getWALThresholds,
  })

  const form = useForm<NotificationsFormValues>({
    resolver: zodResolver(notificationsFormSchema),
    defaultValues: {
      webhook_url: '',
      notification_iteration: 3,
    },
  })

  // Update form values when config data changes
  useEffect(() => {
    if (config) {
      form.reset({
        webhook_url: config.webhook_url,
        notification_iteration: config.notification_iteration,
      })
    }
  }, [config, form])

  const updateMutation = useMutation({
    mutationFn: configurationRepo.updateWALThresholds,
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['configuration', 'wal-thresholds'],
      })
      toast.success('Notification settings updated successfully')
    },
    onError: (error: any) => {
      toast.error(error?.message || 'Failed to update notification settings')
    },
  })

  function onSubmit(data: NotificationsFormValues) {
    if (!config) return

    // Merge with existing config to ensure we don't lose other settings (like thresholds)
    // assuming backend requires full object
    const payload = {
      ...config,
      ...data,
    }
    updateMutation.mutate(payload)
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
