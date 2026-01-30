import {
  GlassCard,
  GlassCardContent,
  GlassCardHeader,
  GlassCardTitle,
  GlassCardDescription
} from './components/glass-card'
import { ConfigDrawer } from '@/components/config-drawer'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { WALMonitorList } from './components/wal-monitor-list'
import { SystemLoadCard } from './components/system-load-card'
import { SystemHealthWidget } from './components/system-health-widget'
import { SourceHealthCard } from './components/source-health-card'
import { TopTablesChart } from './components/top-tables-chart'
import { ActivityFeed } from './components/activity-feed'
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

  // Keep existing queries for consistency
  const { data: flowChart } = useQuery({
    queryKey: ['dashboard', 'flow-chart'],
    queryFn: () => dashboardRepo.getFlowChart(14),
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

      <Main className="bg-gradient-to-br from-background to-muted/20 min-h-screen">
        <div className='mb-6 flex items-center justify-between space-y-2'>
          <h1 className='text-3xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-primary to-primary/60'>
            Mission Control
          </h1>
        </div>

        {/* Top Stats Row */}
        <div className='grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-6'>
          <SourceHealthCard />
          
          <GlassCard>
            <GlassCardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
              <GlassCardTitle className='text-sm font-medium'>
                Pipeline Health
              </GlassCardTitle>
              <Server className='h-4 w-4 text-muted-foreground' />
            </GlassCardHeader>
            <GlassCardContent>
              <div className='text-2xl font-bold'>
                {summary?.pipelines?.total || 0}
              </div>
              <div className='mt-1 flex text-xs text-muted-foreground'>
                <span className='mr-2 text-emerald-500 font-medium'>
                  {summary?.pipelines?.START || 0} Active
                </span>
                <span className='mr-2 text-amber-500 font-medium'>
                  {summary?.pipelines?.PAUSE || 0} Paused
                </span>
              </div>
            </GlassCardContent>
          </GlassCard>

          <GlassCard>
            <GlassCardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
              <GlassCardTitle className='text-sm font-medium'>
                Data Velocity
              </GlassCardTitle>
              <Activity className='h-4 w-4 text-muted-foreground' />
            </GlassCardHeader>
            <GlassCardContent>
              <div className='text-2xl font-bold'>
                {summary?.data_flow?.today.toLocaleString() || 0}
              </div>
              <p className='text-xs text-muted-foreground'>
                Records processed today
              </p>
              <div className='mt-1 flex items-center text-xs'>
                {flowTrend > 0 ? (
                  <TrendingUp className='mr-1 h-3 w-3 text-emerald-500' />
                ) : (
                  <TrendingDown className='mr-1 h-3 w-3 text-rose-500' />
                )}
                <span
                  className={cn(
                    flowTrend > 0 ? 'text-emerald-500' : 'text-rose-500',
                    'font-medium ml-1'
                  )}
                >
                  {Math.abs(flowTrend).toFixed(1)}%
                </span>
                <span className='ml-1 text-muted-foreground'>vs yesterday</span>
              </div>
            </GlassCardContent>
          </GlassCard>

          <GlassCard>
            <GlassCardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
              <GlassCardTitle className='text-sm font-medium'>
                Est. Cost (Month)
              </GlassCardTitle>
              <CreditCard className='h-4 w-4 text-muted-foreground' />
            </GlassCardHeader>
            <GlassCardContent>
              <div className='text-2xl font-bold'>
                ${summary?.credits?.month_total.toFixed(2) || '0.00'}
              </div>
              <p className='text-xs text-muted-foreground'>Credits used</p>
            </GlassCardContent>
          </GlassCard>
        </div>

        {/* Row 2: Charts Area */}
        <div className='grid gap-6 md:grid-cols-2 lg:grid-cols-7 mb-6'>
          {/* Main Flow Chart - Spans 4 cols */}
          <GlassCard className='col-span-4 h-[400px]'>
            <GlassCardHeader>
              <GlassCardTitle>Data Flow Volume</GlassCardTitle>
              <GlassCardDescription>
                Transaction volume over the last 14 days
              </GlassCardDescription>
            </GlassCardHeader>
            <GlassCardContent className='pl-2'>
              <div className='h-[300px] w-full'>
                <ResponsiveContainer width='100%' height={300}>
                  <AreaChart
                    data={flowChart?.history || []}
                    margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="rgba(255,255,255,0.1)" />
                    <defs>
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
                          backgroundColor: 'rgba(23, 23, 23, 0.9)',
                          border: '1px solid rgba(255,255,255,0.1)',
                          borderRadius: '12px',
                          color: '#fff',
                          backdropFilter: 'blur(10px)'
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
                                stackId="1"
                                stroke={color}
                                fill={color}
                             />
                         )
                    })}
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </GlassCardContent>
          </GlassCard>

          {/* Top Tables - Spans 3 cols (moved here) */}
          <TopTablesChart />
        </div>

        {/* Row 3: Detail Cards */}
        <div className='grid gap-6 md:grid-cols-2 lg:grid-cols-7 mb-6'>
           {/* Activity Feed - Spans 4 cols */}
           <div className='col-span-4'>
              <ActivityFeed />
           </div>

           {/* System Stats - Spans 3 cols */}
           <div className='col-span-3 space-y-6'>
              <SystemLoadCard />
              <SystemHealthWidget />
           </div>
        </div>

        {/* Existing WAL Monitor Section - Full Width */}
        <div className='mt-4'>
            <GlassCard>
              <GlassCardHeader className='flex flex-row items-center justify-between'>
                 <div>
                    <GlassCardTitle>WAL Replication Monitor</GlassCardTitle>
                    <GlassCardDescription>Real-time status of replication slots</GlassCardDescription>
                 </div>
              </GlassCardHeader>
              <GlassCardContent>
                 <WALMonitorList />
              </GlassCardContent>
            </GlassCard>
        </div>
      </Main>
    </>
  )
}