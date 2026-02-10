import { useState } from 'react'
import { Bell, Inbox, Trash2, AlertCircle, AlertTriangle, Info, CheckCheck, MoreVertical } from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'

import { Button } from '@/components/ui/button'
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from '@/components/ui/popover'
import {
    SidebarMenuButton,
    useSidebar,
} from '@/components/ui/sidebar'
import {
    HoverCard,
    HoverCardContent,
    HoverCardTrigger,
} from "@/components/ui/hover-card"
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { CustomTabs, CustomTabsContent, CustomTabsList, CustomTabsTrigger } from '@/components/ui/custom-tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Badge } from '@/components/ui/badge'
import { useNotifications } from '@/hooks/use-notifications'

export function NotificationPopover() {
    const { isMobile } = useSidebar()
    const { notifications, unreadCount, markAllAsRead, deleteNotification, markAsRead, deleteAllNotifications } = useNotifications()
    const [isOpen, setIsOpen] = useState(false)

    const [tempReadIds, setTempReadIds] = useState<number[]>([])

    const handleOpenChange = (open: boolean) => {
        setIsOpen(open)
        if (!open) {
            if (unreadCount > 0) {
                markAllAsRead()
            }
            // Clear temp read IDs when closing
            setTempReadIds([])
        }
    }

    // Filter unread notifications:
    // Show if it is NOT read OR if it is in tempReadIds (meaning we just marked it as read)
    const unreadNotifications = notifications.filter(n => !n.is_read || tempReadIds.includes(n.id))

    const getIcon = (type: string) => {
        switch (type) {
            case 'ERROR':
                return <AlertCircle className="h-4 w-4 text-red-500" />
            case 'WARNING':
                return <AlertTriangle className="h-4 w-4 text-yellow-500" />
            default:
                return <Info className="h-4 w-4 text-blue-500" />
        }
    }

    const NotificationList = ({ items, emptyMessage }: { items: typeof notifications, emptyMessage: string }) => {
        if (items.length === 0) {
            return (
                <div className="flex flex-col items-center justify-center h-full text-muted-foreground gap-3 p-8 text-center select-none">
                    <div className="bg-muted/30 p-4 rounded-full">
                        <Inbox className="h-10 w-10 stroke-1 opacity-50" />
                    </div>
                    <div className="space-y-1">
                        <p className="font-semibold text-lg text-foreground">{emptyMessage}</p>
                        {items === unreadNotifications && <p className="text-sm">All caught up!</p>}
                    </div>
                </div>
            )
        }

        return (
            <ScrollArea className="h-full">
                <div className="flex flex-col">
                    {items.map((notification) => (
                        <div
                            key={notification.id}
                            className={`flex items-start gap-3 p-4 border-b hover:bg-muted/50 transition-colors relative group ${!notification.is_read ? 'bg-muted/30' : ''
                                }`}
                        >
                            <div className="mt-1 shrink-0">
                                {getIcon(notification.type)}
                            </div>
                            <div className="flex-1 space-y-1 overflow-hidden min-w-0">
                                <div className="flex items-center justify-between gap-2">
                                    <p className={`text-sm font-medium leading-none ${!notification.is_read ? 'text-foreground' : 'text-muted-foreground'}`}>
                                        {notification.title}
                                    </p>
                                    <span className="text-xs text-muted-foreground shrink-0 tabular-nums">
                                        {formatDistanceToNow(new Date(notification.created_at), { addSuffix: true })}
                                    </span>
                                </div>

                                <HoverCard openDelay={200}>
                                    <HoverCardTrigger asChild>
                                        <p className={`text-sm ${!notification.is_read ? 'text-foreground/90' : 'text-muted-foreground'} line-clamp-2 cursor-default`}>
                                            {notification.message}
                                        </p>
                                    </HoverCardTrigger>
                                    <HoverCardContent side="right" align="start" className="w-80 z-50">
                                        <div className="text-sm space-y-2">
                                            <p className="font-semibold">{notification.title}</p>
                                            <p className="text-muted-foreground break-words">{notification.message}</p>
                                        </div>
                                    </HoverCardContent>
                                </HoverCard>
                            </div>

                            {/* Actions Group - Visible on Hover */}
                            <div className="absolute top-2 right-2 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity bg-background/80 backdrop-blur-sm rounded-md shadow-sm border">
                                {!notification.is_read && (
                                    <Button
                                        variant="ghost"
                                        size="icon"
                                        className="h-6 w-6 text-muted-foreground hover:text-primary"
                                        onClick={(e) => {
                                            e.stopPropagation()
                                            markAsRead(notification.id)
                                            setTempReadIds(prev => [...prev, notification.id])
                                        }}
                                        title="Mark as read"
                                    >
                                        <CheckCheck className="h-3 w-3" />
                                    </Button>
                                )}
                                <Button
                                    variant="ghost"
                                    size="icon"
                                    className="h-6 w-6 text-muted-foreground hover:text-destructive"
                                    onClick={(e) => {
                                        e.stopPropagation()
                                        deleteNotification(notification.id)
                                    }}
                                    title="Delete"
                                >
                                    <Trash2 className="h-3 w-3" />
                                </Button>
                            </div>

                            {/* Unread Indicator Dot */}
                            {!notification.is_read && (
                                <div className="absolute left-0 top-0 bottom-0 w-[3px] bg-primary" />
                            )}
                        </div>
                    ))}
                </div>
            </ScrollArea>
        )
    }

    return (
        <Popover open={isOpen} onOpenChange={handleOpenChange}>
            <PopoverTrigger asChild>
                <SidebarMenuButton
                    size='lg'
                    tooltip='Notifications'
                    className='data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground h-8 w-8 rounded-lg aspect-square p-0 flex items-center justify-center shrink-0 relative overflow-visible'
                >
                    <Bell className={`h-4 w-4 ${unreadCount > 0 ? 'animate-bell-ring' : ''}`} />
                    {unreadCount > 0 && (
                        <span className="absolute -top-1.5 -right-1.5 flex h-4 w-4 items-center justify-center rounded-full bg-red-500 text-[10px] font-bold text-white shadow-sm ring-2 ring-background pointer-events-none z-10">
                            {unreadCount > 9 ? '9+' : unreadCount}
                        </span>
                    )}
                </SidebarMenuButton>
            </PopoverTrigger>
            <PopoverContent
                className='w-[420px] p-0 overflow-hidden shadow-xl border-border flex flex-col h-[600px]'
                side={isMobile ? 'bottom' : 'top'}
                align={isMobile ? 'end' : 'start'}
                sideOffset={12}
            >
                <div className="flex items-center justify-between px-6 pt-5 pb-2 bg-background">
                    <h4 className="font-semibold text-lg">Notifications</h4>
                    <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground hover:text-foreground">
                                <MoreVertical className="h-5 w-5" />
                            </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                            <DropdownMenuItem onClick={() => markAllAsRead()}>
                                <CheckCheck className="mr-2 h-4 w-4" />
                                <span>Mark all as read</span>
                            </DropdownMenuItem>
                            <DropdownMenuItem
                                onClick={() => deleteAllNotifications()}
                                className="text-destructive focus:text-destructive"
                            >
                                <Trash2 className="mr-2 h-4 w-4" />
                                <span>Clear all notifications</span>
                            </DropdownMenuItem>
                        </DropdownMenuContent>
                    </DropdownMenu>
                </div>

                <CustomTabs defaultValue="unread" className="w-full flex-1 flex flex-col overflow-hidden bg-background">
                    <div className="px-6 border-b border-border/40">
                        <CustomTabsList className="w-full justify-start h-auto p-0 bg-transparent border-0 gap-6">
                            <CustomTabsTrigger
                                value="unread"
                                className="rounded-none border-b-2 border-transparent data-[state=active]:border-primary data-[state=active]:bg-transparent px-0 py-3 data-[state=active]:shadow-none transition-none"
                            >
                                Unread
                                {unreadCount > 0 && <Badge variant="secondary" className="ml-2 h-5 px-1.5 text-[10px] bg-red-500 text-white hover:bg-red-600 border-0">{unreadCount}</Badge>}
                            </CustomTabsTrigger>
                            <CustomTabsTrigger
                                value="all"
                                className="rounded-none border-b-2 border-transparent data-[state=active]:border-primary data-[state=active]:bg-transparent px-0 py-3 data-[state=active]:shadow-none transition-none"
                            >
                                All
                            </CustomTabsTrigger>
                        </CustomTabsList>
                    </div>

                    <div className="flex-1 bg-background relative overflow-hidden">
                        <CustomTabsContent value="unread" className="absolute inset-0 mt-0 ring-0 focus-visible:ring-0 outline-none h-full">
                            <NotificationList items={unreadNotifications} emptyMessage="No new notifications" />
                        </CustomTabsContent>
                        <CustomTabsContent value="all" className="absolute inset-0 mt-0 ring-0 focus-visible:ring-0 outline-none h-full">
                            <NotificationList items={notifications} emptyMessage="No notifications" />
                        </CustomTabsContent>
                    </div>
                </CustomTabs>
            </PopoverContent>
        </Popover>
    )
}
