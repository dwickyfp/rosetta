import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import { GlassCard, GlassCardContent, GlassCardDescription, GlassCardHeader, GlassCardTitle } from './glass-card'
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

export function TopTablesChart() {
  const { data: topTables } = useQuery({
    queryKey: ['dashboard', 'top-tables'],
    queryFn: () => dashboardRepo.getTopTables(5),
    refetchInterval: 30000,
  })

  return (
    <GlassCard className="col-span-3 h-[400px]">
      <GlassCardHeader>
        <GlassCardTitle>High Volume Tables</GlassCardTitle>
        <GlassCardDescription>
          Top 5 tables by record volume today.
        </GlassCardDescription>
      </GlassCardHeader>
      <GlassCardContent>
        <div className="h-[300px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              layout="vertical"
              data={topTables || []}
              margin={{ top: 10, right: 30, left: 20, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="rgba(255,255,255,0.1)" />
              <XAxis type="number" hide />
              <YAxis
                dataKey="table_name"
                type="category"
                width={100}
                tick={{ fill: '#888888', fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                cursor={{ fill: 'rgba(255,255,255,0.05)' }}
                contentStyle={{
                  backgroundColor: 'rgba(23, 23, 23, 0.9)',
                  border: '1px solid rgba(255,255,255,0.1)',
                  borderRadius: '12px',
                  color: '#fff',
                  backdropFilter: 'blur(10px)'
                }}
                formatter={(value: number | undefined) => [(value || 0).toLocaleString(), 'Records']}
              />
              <Bar
                dataKey="record_count"
                fill="#8884d8"
                radius={[0, 4, 4, 0]}
                barSize={32}
              >
                  {/* Gradient fill */}
                  {/* Recharts Bar supports simpler fill, but we can try to style it if needed. For now simple color. */}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </GlassCardContent>
    </GlassCard>
  )
}
