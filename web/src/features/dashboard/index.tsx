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
import { TopNav } from '@/components/layout/top-nav'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { WALMonitorList } from './components/wal-monitor-list'

import { useQuery } from '@tanstack/react-query'
import { systemMetricsRepo } from '@/repo/system-metrics'
import { pipelinesRepo } from '@/repo/pipelines'
import { sourcesRepo } from '@/repo/sources'
import { Activity, Cpu, Database, Server } from 'lucide-react'

export function Dashboard() {
  const { data: metrics } = useQuery({
    queryKey: ['system-metrics', 'latest'],
    queryFn: systemMetricsRepo.getLatest,
    refetchInterval: 5000,
  })

  const { data: pipelines } = useQuery({
    queryKey: ['pipelines', 'all'],
    queryFn: pipelinesRepo.getAll,
    refetchInterval: 5000,
  })

  const { data: sources } = useQuery({
    queryKey: ['sources', 'all'],
    queryFn: sourcesRepo.getAll,
    refetchInterval: 5000,
  })

  return (
    <>
      {/* ===== Top Heading ===== */}
      <Header>
        <TopNav links={topNav} />
        <div className='ms-auto flex items-center space-x-4'>
          <Search />
          <ThemeSwitch />
          <ConfigDrawer />
          <ProfileDropdown />
        </div>
      </Header>

      {/* ===== Main ===== */}
      <Main>
        <div className='mb-2 flex items-center justify-between space-y-2'>
          <h1 className='text-2xl font-bold tracking-tight'>Dashboard</h1>
        </div>
        <div className='space-y-4'>
          <div className='grid gap-4 sm:grid-cols-2 lg:grid-cols-4'>
            <Card>
              <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                <CardTitle className='text-sm font-medium'>
                  CPU Usage
                </CardTitle>
                <Cpu className='h-4 w-4 text-muted-foreground' />
              </CardHeader>
              <CardContent>
                <div className='text-2xl font-bold'>
                  {metrics?.cpu_usage?.toFixed(1) || 0}%
                </div>
                <p className='text-xs text-muted-foreground'>
                  Real-time CPU load
                </p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                <CardTitle className='text-sm font-medium'>
                  Memory Usage
                </CardTitle>
                <Activity className='h-4 w-4 text-muted-foreground' />
              </CardHeader>
              <CardContent>
                <div className='text-2xl font-bold'>
                  {metrics?.memory_usage_percent?.toFixed(1) || 0}%
                </div>
                <p className='text-xs text-muted-foreground'>
                  {((metrics?.used_memory || 0) / 1024 / 1024 / 1024).toFixed(1)} GB / {((metrics?.total_memory || 0) / 1024 / 1024 / 1024).toFixed(1)} GB used
                </p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                <CardTitle className='text-sm font-medium'>Total Sources</CardTitle>
                <Database className='h-4 w-4 text-muted-foreground' />
              </CardHeader>
              <CardContent>
                <div className='text-2xl font-bold'>{sources?.total || 0}</div>
                <p className='text-xs text-muted-foreground'>
                  Configured PostgreSQL sources
                </p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                <CardTitle className='text-sm font-medium'>
                  Total Pipelines
                </CardTitle>
                <Server className='h-4 w-4 text-muted-foreground' />
              </CardHeader>
              <CardContent>
                <div className='text-2xl font-bold'>{pipelines?.total || 0}</div>
                <p className='text-xs text-muted-foreground'>
                  Active replication pipelines
                </p>
              </CardContent>
            </Card>
          </div>
          <div className='grid grid-cols-1 gap-4'>
            <Card className='col-span-1'>
              <CardHeader>
                <CardTitle>WAL Monitor</CardTitle>
                <CardDescription>
                  Real-time WAL replication status per source.
                </CardDescription>
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

const topNav = [
  {
    title: 'Overview',
    href: 'dashboard/overview',
    isActive: true,
    disabled: false,
  },
  {
    title: 'Customers',
    href: 'dashboard/customers',
    isActive: false,
    disabled: true,
  },
  {
    title: 'Products',
    href: 'dashboard/products',
    isActive: false,
    disabled: true,
  },
  {
    title: 'Settings',
    href: 'dashboard/settings',
    isActive: false,
    disabled: true,
  },
]
