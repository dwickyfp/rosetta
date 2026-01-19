import { useParams } from '@tanstack/react-router'
import { useCreditUsage, useRefreshCredits } from '@/repo/credits'
import { Main } from '@/components/layout/main'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Bar, BarChart, ResponsiveContainer, XAxis, YAxis, Tooltip } from 'recharts'
import { Button } from '@/components/ui/button'
import { Loader2, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { format } from 'date-fns'
import { Header } from '@/components/layout/header'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ConfigDrawer } from '@/components/config-drawer'

export function DestinationDetailsPage() {
    const { destinationId } = useParams({ from: '/_authenticated/destinations/$destinationId' })
    const { data, isLoading, refetch } = useCreditUsage(destinationId)
    const { mutate: refreshCredits, isPending: isRefreshing } = useRefreshCredits()

    const handleRefresh = () => {
        refreshCredits(destinationId, {
            onSuccess: () => {
                toast.success('Credit usage data refreshed')
                refetch()
            },
            onError: (error) => {
                toast.error('Failed to refresh data: ' + error.message)
            }
        })
    }

    if (isLoading) {
        return (
            <Main>
                <div className="flex h-full items-center justify-center">
                    <Loader2 className="h-8 w-8 animate-spin" />
                </div>
            </Main>
        )
    }

    const summary = data?.summary
    const chartData = data?.daily_usage.map(d => ({
        date: format(new Date(d.date), 'MM/dd'),
        credits: d.credits
    })).reverse() || []

    return (
        <div className="flex min-h-screen flex-col">
            <Header fixed>
                <Search />
                <div className='ms-auto flex items-center space-x-4'>
                    <ThemeSwitch />
                    <ConfigDrawer />
                </div>
            </Header>

            <Main className='flex flex-1 flex-col gap-4 sm:gap-6 pt-16'>
                <div className="flex items-center justify-between">
                    <div>
                        <h2 className="text-2xl font-bold tracking-tight">Destination Details</h2>
                        <p className="text-muted-foreground">Snowflake Credit Usage Monitoring</p>
                    </div>
                    <Button onClick={handleRefresh} disabled={isRefreshing}>
                        {isRefreshing ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <RefreshCw className="mr-2 h-4 w-4" />}
                        Refresh Data
                    </Button>
                </div>

                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                    <Card>
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">This Week</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="text-2xl font-bold">{summary?.current_week.toFixed(2) ?? '0.00'}</div>
                            <p className="text-xs text-muted-foreground">Credits used</p>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">This Month</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="text-2xl font-bold">{summary?.current_month.toFixed(2) ?? '0.00'}</div>
                            <p className="text-xs text-muted-foreground">Credits used</p>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">Previous Week</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="text-2xl font-bold">{summary?.previous_week.toFixed(2) ?? '0.00'}</div>
                            <p className="text-xs text-muted-foreground">Credits used</p>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">Previous Month</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="text-2xl font-bold">{summary?.previous_month.toFixed(2) ?? '0.00'}</div>
                            <p className="text-xs text-muted-foreground">Credits used</p>
                        </CardContent>
                    </Card>
                </div>

                <Card className="col-span-4">
                    <CardHeader>
                        <CardTitle>Daily Usage (Last 30 Days)</CardTitle>
                    </CardHeader>
                    <CardContent className="pl-2">
                        <div className="h-[300px] w-full">
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={chartData}>
                                    <XAxis
                                        dataKey="date"
                                        stroke="#888888"
                                        fontSize={12}
                                        tickLine={false}
                                        axisLine={false}
                                    />
                                    <YAxis
                                        stroke="#888888"
                                        fontSize={12}
                                        tickLine={false}
                                        axisLine={false}
                                        tickFormatter={(value) => `${value}`}
                                    />
                                    <Tooltip
                                        cursor={{ fill: 'transparent' }}
                                        contentStyle={{ borderRadius: '8px' }}
                                    />
                                    <Bar
                                        dataKey="credits"
                                        fill="currentColor"
                                        radius={[4, 4, 0, 0]}
                                        className="fill-primary"
                                    />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </CardContent>
                </Card>
            </Main>
        </div>
    )
}
