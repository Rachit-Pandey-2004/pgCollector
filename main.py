from os import getcwd
from sys import path
import asyncio
# import uvloop  # Import uvloop

path.insert(0, f"{getcwd()}/src/pogoMaps/")

from telegram_scanner import TeleScan
from pogomap import Pogocity

async def t1():
    async with TeleScan() as ts:
        await ts.search()

async def t2():
    async with Pogocity() as pgct:
        await pgct.all_monster()

async def activity():
    task1 = asyncio.create_task(t1())
    task2 = asyncio.create_task(t2())
    
    # Ensure both tasks complete
    await asyncio.gather(task1, task2)

if __name__ == "__main__":
    # uvloop.install()  # Install uvloop as the event loop
    asyncio.run(activity())
