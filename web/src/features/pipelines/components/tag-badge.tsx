import { Badge } from '@/components/ui/badge'
import { Hash, X } from 'lucide-react'
import { cn } from '@/lib/utils'

interface TagBadgeProps {
  tag: string
  onRemove?: () => void
  variant?: 'default' | 'outline'
  className?: string
}

export function TagBadge({ tag, onRemove, variant = 'default', className }: TagBadgeProps) {
  return (
    <Badge
      variant={variant}
      className={cn(
        'group flex items-center gap-1.5 h-8 px-3.5 text-sm font-medium rounded-md',
        'bg-gradient-to-r from-blue-50 to-indigo-50 text-blue-700 border-blue-200',
        'dark:from-blue-950/30 dark:to-indigo-950/30 dark:text-blue-300 dark:border-blue-800/50',
        'hover:from-blue-100 hover:to-indigo-100 dark:hover:from-blue-950/50 dark:hover:to-indigo-950/50',
        'transition-all',
        className
      )}
    >
      <Hash className="h-4 w-4 opacity-70" />
      <span className="truncate max-w-[120px]" title={tag}>
        {tag}
      </span>
      {onRemove && (
        <button
          onClick={(e) => {
            e.stopPropagation()
            onRemove()
          }}
          className="ml-1 rounded-full p-0.5 hover:bg-blue-200 dark:hover:bg-blue-800/50 transition-colors"
          aria-label="Remove tag"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      )}
    </Badge>
  )
}
