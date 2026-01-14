import re
from pathlib import Path

# Files to process
files = [
    "app/domain/services/source.py",
    "app/domain/services/pipeline.py",
    "app/domain/services/wal_monitor.py",
    "app/domain/services/wal_monitor_service.py",
    "app/api/v1/endpoints/sources.py",
    "app/api/v1/endpoints/pipelines.py",
    "app/api/v1/endpoints/wal_metrics.py",
    "app/api/v1/endpoints/wal_monitor.py",
]

base_path = Path(__file__).parent

for file_path in files:
    full_path = base_path / file_path
    if not full_path.exists():
        print(f"✗ File not found: {file_path}")
        continue

    print(f"Processing {file_path}...")

    with open(full_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Replace imports
    content = re.sub(
        r"from sqlalchemy\.ext\.asyncio import AsyncSession",
        "from sqlalchemy.orm import Session",
        content,
    )

    # Replace type hints in __init__
    content = re.sub(
        r"def __init__\(self, db: AsyncSession\)",
        "def __init__(self, db: Session)",
        content,
    )

    # Replace type hints in monitor_source
    content = re.sub(
        r"async def monitor_source\(self, source: Source, db: AsyncSession\)",
        "async def monitor_source(self, source: Source, db: Session)",
        content,
    )

    # Remove async from method definitions (but KEEP async in endpoint files)
    if "endpoints" not in str(file_path):
        content = re.sub(r"(\s+)async def ", r"\1def ", content)

    # Remove await from repository and service calls
    content = re.sub(r"await self\.repository\.", "self.repository.", content)
    content = re.sub(r"await self\.db\.", "self.db.", content)
    content = re.sub(r"await repo\.", "repo.", content)
    content = re.sub(r"await wal_repo\.", "wal_repo.", content)
    content = re.sub(r"await db\.", "db.", content)

    # For endpoint files, remove await from service calls
    if "endpoints" in str(file_path):
        content = re.sub(r"await service\.", "service.", content)
        content = re.sub(r"= await ([a-z_]+)\.", r"= \1.", content)

    with open(full_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"  ✓ Completed {file_path}")

print("\nAll files processed!")
