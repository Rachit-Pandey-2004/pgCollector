'''
This script has raids and dynamax battel data so we need two different db to store the essential data
'''


from sys import path
from os import getcwd
import asyncio
from json import loads
import random
from time import time
import aiohttp
path.insert(0,"%s/src/db"%getcwd())

from MonsDB import PGDB 

class Pogocity:
    __endpoint = []
    __temp_time = [0, 0, 0, 0, 0]

    def __init__(self) -> None:
        print("Pogomap initiated...")
        with open("%s/src/pogoMaps/configs/mons.json" % getcwd(), "r") as fs:
            data = loads(fs.read())
            self.mons = data["mons"]
            self.header = data["headers"]
            self.mon_names = data["mons_name"]
            Pogocity.__endpoint = data["endpoint"]
        
    async def __aenter__(self):
        self.psql = PGDB()
        await self.psql.__aenter__()
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10, ttl_dns_cache=300))
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.psql.__aexit__(exc_type, exc_value, traceback)  # Ensure database closes properly
        await self.session.close()

    async def __db_dump(self,val):
        data=[]
        #at present due to raid hour no filter is there for eggs
        try:
            print(val)
        except Exception as e:
            print("ERROR while writing at database %s"%e)

    async def _Fetch_mons(self, index=0, since=0, retries=5, backoff=3):
        """Fetch Pokémon data with retry logic on network errors."""
        mons = self.mons
        url = f"{Pogocity.__endpoint[index]}mons={mons}&minIV=0&minLevel=0&alwaysHundo=0&alwaysMighty=0&time={int(time()*1000)}&since={since}"
        headers = self.header

        for attempt in range(retries):
            try:
                async with self.session.get(url, headers=headers[index]) as response:
                    print(f"Status {response.status}")
                    if response.status != 200:
                        raise aiohttp.ClientResponseError(
                            response.request_info, response.history, status=response.status
                        )
                    content = await response.json()
                    await self.__db_dump(content)
                    return content["meta"]["inserted"]
            except (aiohttp.ClientConnectorError, aiohttp.ClientConnectorDNSError) as e:
                print(f"[DNS/Connection Error] {e}. Retrying in {backoff} sec...")
            except aiohttp.ClientOSError as e:
                print(f"[OS Error] {e}. Retrying in {backoff} sec...")
            except aiohttp.ClientResponseError as e:
                print(f"[HTTP Error] {e.status}. Retrying in {backoff} sec...")
            except Exception as e:
                print(f"[Unexpected Error] {e}. Retrying in {backoff} sec...")

            await asyncio.sleep(backoff + random.uniform(0, 1))  # Random delay to avoid spikes
            backoff *= 2  # Exponential backoff

        print("Max retries reached. Skipping request.")
        return since  # Return previous timestamp if request fails
    async def all_monster(self):
        while True:
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(self._Fetch_mons(i, since=Pogocity.__temp_time[i])) for i in range(5)]
            results = await asyncio.gather(*tasks, return_exceptions=True)  # Ensures all tasks complete
            for i, res in enumerate(results):
                if isinstance(res, Exception):
                    print(f"Task {i} failed: {res}")
                else:
                    Pogocity.__temp_time[i] = res  # Update only if successful
            print("\n\n")
            print(Pogocity.__temp_time)
            await asyncio.sleep(60)



async def test():
    async with Pogocity()as pgct:
        await pgct.all_monster()
if __name__=="__main__":
    asyncio.run(test())