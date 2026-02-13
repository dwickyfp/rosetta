import { cn } from '@/lib/utils'
import { List, Orbit } from 'lucide-react'

export type SmartTagView = 'list' | 'visualization'

interface FloatingViewToggleProps {
  activeView: SmartTagView
  onViewChange: (view: SmartTagView) => void
}

export function FloatingViewToggle({
  activeView,
  onViewChange,
}: FloatingViewToggleProps) {
  return (
    <div className="fixed bottom-6 left-1/2 z-50 -translate-x-1/2">
      <div className="flex items-center gap-1 rounded-full border bg-background/80 p-1.5 shadow-lg backdrop-blur-md">
        <button
          onClick={() => onViewChange('list')}
          className={cn(
            'flex items-center gap-2 rounded-full px-4 py-2 text-sm font-medium transition-all',
            activeView === 'list'
              ? 'bg-primary text-primary-foreground shadow-sm'
              : 'text-muted-foreground hover:bg-muted hover:text-foreground'
          )}
        >
          <List className="h-4 w-4" />
          List Tags
        </button>
        <button
          onClick={() => onViewChange('visualization')}
          className={cn(
            'flex items-center gap-2 rounded-full px-4 py-2 text-sm font-medium transition-all',
            activeView === 'visualization'
              ? 'bg-primary text-primary-foreground shadow-sm'
              : 'text-muted-foreground hover:bg-muted hover:text-foreground'
          )}
        >
          <Orbit className="h-4 w-4" />
          3D Visualization
        </button>
      </div>
    </div>
  )
}
