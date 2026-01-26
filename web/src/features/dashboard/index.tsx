import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { ConfigDrawer } from '@/components/config-drawer'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { WALMonitorList } from './components/wal-monitor-list'
import { SystemLoadCard } from './components/system-load-card'
import { SystemHealthWidget } from './components/system-health-widget'
import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import {
  Activity,
  CreditCard,
  Server,
  TrendingDown,
  TrendingUp,
} from 'lucide-react'
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { cn } from '@/lib/utils'



export function Dashboard() {
  const { data: summary } = useQuery({
    queryKey: ['dashboard', 'summary'],
    queryFn: dashboardRepo.getSummary,
    refetchInterval: 10000,
  })

  const { data: flowChart } = useQuery({
    queryKey: ['dashboard', 'flow-chart'],
    queryFn: () => dashboardRepo.getFlowChart(14),
    refetchInterval: 60000,
  })

  const { data: creditChart } = useQuery({
    queryKey: ['dashboard', 'credit-chart'],
    queryFn: () => dashboardRepo.getCreditChart(30),
    refetchInterval: 60000,
  })

  // Calculate trends
  const flowTrend =
    summary?.data_flow && summary.data_flow.yesterday > 0
      ? ((summary.data_flow.today - summary.data_flow.yesterday) /
          summary.data_flow.yesterday) *
        100
      : 0

  return (
    <>
      <Header>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
          <ConfigDrawer />
        </div>
      </Header>

      <Main>
        <div className='mb-4 flex items-center justify-between space-y-2'>
          <h1 className='text-2xl font-bold tracking-tight'>Dashboard</h1>
        </div>

        <div className='grid gap-4 md:grid-cols-2 lg:grid-cols-4'>
          {/* Pipelines Status Card */}
          <Card>
            <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
              <CardTitle className='text-sm font-medium'>
                Pipeline Health
              </CardTitle>
              <Server className='h-4 w-4 text-muted-foreground' />
            </CardHeader>
            <CardContent>
              <div className='text-2xl font-bold'>
                {summary?.pipelines?.total || 0}
              </div>
              <div className='mt-1 flex text-xs text-muted-foreground'>
                <span className='mr-2 text-green-500'>
                  {summary?.pipelines?.START || 0} Active
                </span>
                <span className='mr-2 text-yellow-500'>
                  {summary?.pipelines?.PAUSE || 0} Paused
                </span>
                {/* Add failed if available in future */}
              </div>
            </CardContent>
          </Card>

          {/* Data Velocity Card */}
          <Card>
            <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
              <CardTitle className='text-sm font-medium'>
                Data Velocity
              </CardTitle>
              <Activity className='h-4 w-4 text-muted-foreground' />
            </CardHeader>
            <CardContent>
              <div className='text-2xl font-bold'>
                {summary?.data_flow?.today.toLocaleString() || 0}
              </div>
              <p className='text-xs text-muted-foreground'>
                Records processed today
              </p>
              <div className='mt-1 flex items-center text-xs'>
                {flowTrend > 0 ? (
                  <TrendingUp className='mr-1 h-3 w-3 text-green-500' />
                ) : (
                  <TrendingDown className='mr-1 h-3 w-3 text-red-500' />
                )}
                <span
                  className={cn(
                    flowTrend > 0 ? 'text-green-500' : 'text-red-500'
                  )}
                >
                  {Math.abs(flowTrend).toFixed(1)}%
                </span>
                <span className='ml-1 text-muted-foreground'>vs yesterday</span>
              </div>
            </CardContent>
          </Card>

          {/* Credit Usage Card */}
          <Card>
            <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
              <CardTitle className='text-sm font-medium'>
                Est. Cost (Month)
              </CardTitle>
              <CreditCard className='h-4 w-4 text-muted-foreground' />
            </CardHeader>
            <CardContent>
              <div className='text-2xl font-bold'>
                {summary?.credits?.month_total.toFixed(2) || 0}
              </div>
              <p className='text-xs text-muted-foreground'>Credits used</p>
            </CardContent>
          </Card>

          {/* System Load Card */}
          <SystemLoadCard />
          
          {/* System Health Widget */}
          <SystemHealthWidget />
        </div>

        {/* Charts Section */}
        <div className='mt-4 grid gap-4 md:grid-cols-2 lg:grid-cols-7'>
          {/* Main Flow Chart */}
          <Card className='col-span-4'>
            <CardHeader>
              <CardTitle>Data Flow Volume</CardTitle>
              <CardDescription>
                Transaction volume over the last 14 days (grouped by pipeline)
              </CardDescription>
            </CardHeader>
            <CardContent className='pl-2'>
              <div className='h-[300px] w-full'>
                <ResponsiveContainer width='100%' height={300}>
                  <AreaChart
                    data={flowChart?.history || []}
                    margin={{
                      top: 10,
                      right: 10,
                      left: 0,
                      bottom: 0,
                    }}
                  >
                     <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E5E7EB" />
                    <defs>
                      {/* We can use defined colors or generate them */}
                      <linearGradient id="colorGradient" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#8884d8" stopOpacity={0.8}/>
                        <stop offset="95%" stopColor="#8884d8" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <XAxis
                      dataKey='date'
                      stroke='#888888'
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(value) => {
                        const date = new Date(value)
                        return `${date.getMonth() + 1}/${date.getDate()}`
                      }}
                    />
                    <YAxis
                      stroke='#888888'
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(value) => `${value}`}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'rgba(255, 255, 255, 0.9)',
                        border: '1px solid #ccc',
                        borderRadius: '4px',
                        color: '#000',
                      }}
                    />
                    <Legend />
                    {flowChart?.pipelines?.map((pipeline, index) => {
                         const colors = ["#8884d8", "#82ca9d", "#ffc658", "#ff7300", "#0088fe", "#00C49F"];
                         const color = colors[index % colors.length];
                         return (
                             <Area
                                key={pipeline}
                                type='monotone'
                                dataKey={pipeline}
                                stackId="1" // Stack them to show total volume visually while separating
                                stroke={color}
                                fill={color}
                             />
                         )
                    })}
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          {/* Credit Usage Bar Chart */}
          <Card className='col-span-3'>
            <CardHeader>
              <CardTitle>Credit Usage</CardTitle>
              <CardDescription>Daily credit consumption (30d)</CardDescription>
            </CardHeader>
            <CardContent>
              <div className='h-[300px] w-full'>
                <ResponsiveContainer width='100%' height={300}>
                  <BarChart data={creditChart?.history || []}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E5E7EB" />
                    <XAxis
                      dataKey='date'
                      stroke='#888888'
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(value) => {
                        const date = new Date(value)
                        return `${date.getDate()}`
                      }}
                    />
                    <Tooltip
                      cursor={{ fill: 'transparent' }}
                      contentStyle={{
                        backgroundColor: 'rgba(255, 255, 255, 0.9)',
                        border: '1px solid #ccc',
                        borderRadius: '4px',
                        color: '#000',
                      }}
                    />
                    <Legend />
                    {creditChart?.destinations?.map((dest, index) => {
                         const colors = ["#0f172a", "#334155", "#475569", "#64748b"];
                         const color = colors[index % colors.length];
                         return (
                            <Bar
                                key={dest}
                                dataKey={dest}
                                stackId="a"
                                fill={color}
                                radius={[4, 4, 0, 0]}
                            />
                         )
                    })}
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Existing WAL Monitor Section - nicely integrated */}
        <div className='mt-4'>
           <div className='grid grid-cols-1'>
            <Card>
              <CardHeader className='flex flex-row items-center justify-between'>
                 <div>
                    <CardTitle>WAL Replication Monitor</CardTitle>
                    <CardDescription>Real-time status of replication slots</CardDescription>
                 </div>
                 {/* Optional: Add refresh button or status indicator here */}
              </CardHeader>
              <CardContent>
                 <WALMonitorList />
              </CardContent>
            </Card>
           </div>
        </div>
      </Main>
    </>
  )
}