
import asyncio
import httpx
import sys

URL = "http://0.0.0.0:8001/health"
URL_LOCALHOST = "http://localhost:8001/health"

async def check(url):
    print(f"Checking {url}...")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            print(f"Response: {response.status_code} {response.text}")
    except Exception as e:
        print(f"Failed: {e.__class__.__name__}: {e}")

async def main():
    await check(URL)
    await check(URL_LOCALHOST)

if __name__ == "__main__":
    asyncio.run(main())
