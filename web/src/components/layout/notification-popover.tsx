import { Bell, Inbox, MoreVertical } from 'lucide-react'
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
import { CustomTabs, CustomTabsContent, CustomTabsList, CustomTabsTrigger } from '@/components/ui/custom-tabs'

export function NotificationPopover() {
    const { isMobile } = useSidebar()

    return (
        <Popover>
            <PopoverTrigger asChild>
                <SidebarMenuButton
                    size='lg'
                    tooltip='Notifications'
                    className='data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground h-8 w-8 rounded-lg aspect-square p-0 flex items-center justify-center shrink-0'
                >
                    <Bell className="h-4 w-4" />
                </SidebarMenuButton>
            </PopoverTrigger>
            <PopoverContent
                className='w-80 p-0 overflow-hidden'
                side={isMobile ? 'bottom' : 'top'}
                align={isMobile ? 'end' : 'start'}
                sideOffset={4}
            >
                <div className="flex items-center justify-between px-4 py-3 border-b bg-muted/40">
                    <h4 className="font-semibold text-sm">Notifications</h4>
                    <Button variant="ghost" size="icon" className="h-6 w-6">
                        <MoreVertical className="h-4 w-4" />
                    </Button>
                </div>
                <CustomTabs defaultValue="unread" className="w-full">
                    <CustomTabsList className="w-full justify-start border-b px-2">
                        <CustomTabsTrigger value="unread" className="flex-1">
                            Unread
                        </CustomTabsTrigger>
                        <CustomTabsTrigger value="all" className="flex-1">
                            All
                        </CustomTabsTrigger>
                    </CustomTabsList>

                    <div className="h-[300px] relative">
                        <CustomTabsContent value="unread" className="absolute inset-0 mt-0">
                            <div className="flex flex-col items-center justify-center h-full text-muted-foreground gap-2 p-4 text-center">
                                <Inbox className="h-10 w-10 stroke-1 opacity-50" />
                                <p className="font-medium text-sm">No new notifications</p>
                                <p className="text-xs">All caught up!</p>
                            </div>
                        </CustomTabsContent>
                        <CustomTabsContent value="all" className="absolute inset-0 mt-0">
                            <div className="flex flex-col items-center justify-center h-full text-muted-foreground gap-2 p-4 text-center">
                                <Inbox className="h-10 w-10 stroke-1 opacity-50" />
                                <p className="font-medium text-sm">No notifications</p>
                            </div>
                        </CustomTabsContent>
                    </div>
                </CustomTabs>
            </PopoverContent>
        </Popover>
    )
}
