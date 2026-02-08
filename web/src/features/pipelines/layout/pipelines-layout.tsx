import { PipelinesSidebar } from "@/features/pipelines/components/pipelines-sidebar"
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable"
import { useState, useRef } from "react"
import { type PanelImperativeHandle } from "react-resizable-panels"
import { ChevronRight } from "lucide-react"

interface PipelinesLayoutProps {
    children: React.ReactNode
}

export function PipelinesLayout({ children }: PipelinesLayoutProps) {
    const [isCollapsed, setIsCollapsed] = useState(false)
    const panelRef = useRef<PanelImperativeHandle>(null)

    return (
        <div className="relative h-full w-full">
            {isCollapsed && (
                <button
                    onClick={() => panelRef.current?.resize(360)}
                    className="absolute left-0 top-1/2 -translate-y-1/2 z-50 flex h-16 w-4 items-center justify-center rounded-r-md border border-l-0 bg-sidebar border-sidebar-border hover:bg-sidebar-accent transition-all cursor-pointer group"
                    title="Expand sidebar"
                >
                    <ChevronRight className="h-4 w-4 text-muted-foreground group-hover:text-foreground transition-colors" />
                </button>
            )}
            <ResizablePanelGroup
                className="h-full w-full rounded-lg border-t border-border"
                orientation="horizontal"
            >
                <ResizablePanel
                    panelRef={panelRef}
                    defaultSize={360}
                    minSize={0}
                    maxSize={360}
                    collapsible
                    onResize={(size) => {
                        setIsCollapsed(size.asPercentage === 0)
                    }}
                >
                    <PipelinesSidebar />
                </ResizablePanel>

                <ResizableHandle withHandle />

                <ResizablePanel defaultSize={80}>
                    <div className="h-full w-full overflow-y-auto">
                        {children}
                    </div>
                </ResizablePanel>
            </ResizablePanelGroup>
        </div>
    )
}
