import asyncio
import os
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from dotenv import load_dotenv

# Add the parent directory to sys.path to ensure modules can be imported
sys.path.append(os.path.join(os.getcwd(), 'backend'))

load_dotenv(os.path.join('backend', '.env'))

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("DATABASE_URL not found in .env")
    sys.exit(1)

# Ensure async driver is used
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

async def check_enum_values():
    engine = create_async_engine(DATABASE_URL)
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT id, status FROM pipeline_metadata"))
        rows = result.fetchall()
        print(f"Found {len(rows)} rows in pipeline_metadata")
        for row in rows:
            print(f"ID: {row.id}, Status: '{row.status}'")

if __name__ == "__main__":
    asyncio.run(check_enum_values())
