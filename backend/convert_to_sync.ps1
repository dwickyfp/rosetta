# PowerShell script to convert all async SQLAlchemy code to sync

$files = @(
    'app\domain\services\source.py',
    'app\domain\services\pipeline.py',
    'app\domain\services\wal_monitor.py',
    'app\domain\services\wal_monitor_service.py',
    'app\api\v1\endpoints\sources.py',
    'app\api\v1\endpoints\pipelines.py',
    'app\api\v1\endpoints\wal_metrics.py',
    'app\api\v1\endpoints\wal_monitor.py'
)

foreach ($file in $files) {
    $fullPath = Join-Path $PSScriptRoot $file
    if (Test-Path $fullPath) {
        Write-Host "Processing $file..."
        
        $content = Get-Content $fullPath -Raw
        
        # Replace imports
        $content = $content -replace 'from sqlalchemy\.ext\.asyncio import AsyncSession', 'from sqlalchemy.orm import Session'
        
        # Replace type hints in __init__
        $content = $content -replace 'def __init__\(self, db: AsyncSession\)', 'def __init__(self, db: Session)'
        
        # Replace type hints in other methods (but not in endpoint monitor_source which needs AsyncSession for parameter)
        $content = $content -replace '(\s+)async def ([a-z_]+)\(self, source: Source, db: AsyncSession\)', '$1async def $2(self, source: Source, db: Session)'
        
        # Remove async from method definitions (but KEEP async in endpoint files)
        if ($file -notlike '*endpoints*') {
            $content = $content -replace '(\s+)async def ', '$1def '
        }
        
        # Remove await from repository and service calls
        $content = $content -replace 'await self\.repository\.', 'self.repository.'
        $content = $content -replace 'await self\.db\.', 'self.db.'
        $content = $content -replace 'await repo\.', 'repo.'
        $content = $content -replace 'await wal_repo\.', 'wal_repo.'
        $content = $content -replace 'await db\.', 'db.'
        
        # For endpoint files, remove await from service calls
        if ($file -like '*endpoints*') {
            $content = $content -replace 'await service\.', 'service.'
            $content = $content -replace '= await ([a-z_]+)\.', '= $1.'
        }
        
        Set-Content $fullPath $content -NoNewline
        Write-Host "  ✓ Completed $file"
    } else {
        Write-Host "  ✗ File not found: $file"
    }
}

Write-Host "`nAll files processed!"
