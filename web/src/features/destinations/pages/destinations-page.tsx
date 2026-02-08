import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { DestinationsDialogs } from '../components/destinations-dialogs'
import { DestinationsPrimaryButtons } from '../components/destinations-primary-buttons'
import { DestinationsProvider } from '../components/destinations-provider'
import { DestinationsTable } from '../components/destinations-table'
import { destinationsRepo } from '@/repo/destinations'
import { useQuery } from '@tanstack/react-query'
import { useEffect } from 'react'
import { ConfigDrawer } from '@/components/config-drawer'

export function DestinationsPage() {
    const { data } = useQuery({
        queryKey: ['destinations'],
        queryFn: destinationsRepo.getAll,
    })

    useEffect(() => {
        document.title = 'Destinations'
        return () => {
            document.title = 'Rosetta'
        }
    }, [])

    const destinations = data?.destinations || []

    return (
        <DestinationsProvider>
            <Header fixed>
                <Search />
                <div className='ms-auto flex items-center space-x-4'>
                    <ThemeSwitch />
                    <ConfigDrawer />
                </div>
            </Header>

            <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
                <div className='flex flex-wrap items-end justify-between gap-2'>
                    <div>
                        <h2 className='text-2xl font-bold tracking-tight'>Destinations</h2>
                        <p className='text-muted-foreground'>
                            Manage your Snowflake data destinations.
                        </p>
                    </div>
                    <DestinationsPrimaryButtons />
                </div>
                <DestinationsTable data={destinations} />
            </Main>

            <DestinationsDialogs />
        </DestinationsProvider>
    )
}
