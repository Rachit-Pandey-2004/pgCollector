import aiohttp
from os import getcwd
import asyncio

class DisScanner:
    def __init__(self)->None:
        pass
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc_value, traceback):
        pass





async def main():
    async with DisScanner() as scanner:
        pass

asyncio.run(main())