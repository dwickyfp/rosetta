import {
  LayoutDashboard,
  Settings,
  Snowflake,
  Workflow,
  Command,
  Database,
  Activity,
} from 'lucide-react'
import { type SidebarData } from '../types'

export const sidebarData: SidebarData = {
  user: {
    name: 'ETL Developer',
    email: 'rosetta@rosetta.com',
    avatar: '',
  },
  teams: [
    {
      name: 'Rosetta',
      logo: Command,
      plan: 'CDC Platform',
    },
  ],
  navGroups: [
    {
      title: 'General',
      items: [
        {
          title: 'Dashboard',
          url: '/',
          icon: LayoutDashboard,
        },
      ],
    },
    {
      title: 'Connections',
      items: [
        {
          title: 'Sources',
          icon: Database,
          url: '/sources',
        },
        {
          title: 'Destinations',
          icon: Snowflake,
          url: '/destinations',
        },
      ],
    },
    {
      title: 'Integrations',
      items: [
        {
          title: 'Pipelines',
          icon: Workflow,
          url: '/pipelines',
        },
      ],
    },
    {
      title: 'Other',
      items: [
        {
          title: 'Settings',
          icon: Settings,
          items: [
            {
              title: 'WAL Monitor',
              url: '/settings/wal-monitor',
              icon: Activity,
            },
          ],
        },
      ],
    },
  ],
}
