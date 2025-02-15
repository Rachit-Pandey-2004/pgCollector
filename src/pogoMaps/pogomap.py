from sys import path
from os import getcwd
import asyncio
from json import loads,dumps
from datetime import datetime
from time import time
import aiohttp
path.insert(0,"%s/src/db"%getcwd())

from dbCollector import PGDB 

class Pogocity:
    __endpoint=[]
    #  collects temporary time for all maps by pogomaps
    __temp_time=[0,0,0,0,0]

    def __init__(self)->None:
        print("Pogomap initiated...")
        with open("%s/src/pogoMaps/configs/mons.json"%getcwd(),"r")as fs:
            data=loads(fs.read())
            self.mons=data['mons']
            self.header=data['headers']
            self.mon_names=data["mons_name"]
            Pogocity.__endpoint=data['endpoint']
            fs.close()
        
    async def __aenter__(self):
        self.psql = PGDB()
        await self.psql.__aenter__()
        return self
        
    async def __aexit__(self,exc_type, exc_value, traceback):
        await self.psql.__aexit__(exc_type, exc_value, traceback)
        pass

    async def __db_dump(self,val):
        data=[]
        try:
            for li in val["pokemons"]:
                # li["attack"] = max(0, min(15, li["attack"]))
                # li["defence"] = max(0, min(15, li["defence"]))
                # li["stamina"] = max(0, min(15, li["stamina"]))
                iv=((li["attack"]+li["defence"]+li["stamina"])/45)*100
                name=self.mon_names[int(li['pokemon_id'])-1]
                if iv<0:iv=-1
                if li['gender']==0 :
                    gender='N'
                elif li['gender']==1 or '♂' in name:
                    gender='M'
                elif li["gender"]==2 or '♀' in name:
                    gender='F'
                else :
                    gender="N"
                #(id, p_name, cp, lvl, gender, iv, coordinates, despawn)
                data.append((li['pokemon_id'],name.lower(),li['cp'],li['level'],gender.upper(),iv,li["lat"],li['lng'],(li["despawn"])))
            await self.psql.insert_mass_data(data)
        except Exception as e:
            print("ERROR while writing at database %s"%e)

    async def _Fetch_mons(self,index=0,since=0):
        mons=self.mons
        #since inserted on last data
        minIV="0"
        minLevel="0"
        alwaysHundo="0"
        alwaysMighty="0"
        url=Pogocity.__endpoint[index]+"mons=%s&minIV=%s&minLevel=%s&alwaysHundo=%s&alwaysMighty=%s&time=%s&since=%s"%(mons,minIV,minLevel,alwaysHundo,alwaysMighty,int(time()*1000),since)
        headers=self.header
        async with aiohttp.ClientSession()as session:
            async with session.get(url,headers=headers[index])as response:
                print("Status:", response.status)
                content=await response.json()
                await (self.__db_dump(content))
                return(content["meta"]["inserted"])
    async def all_monster(self):
        while(True):
            async with asyncio.TaskGroup() as tg:
                task1 = tg.create_task(self._Fetch_mons(0,since=Pogocity.__temp_time[0]))
                task2 = tg.create_task(self._Fetch_mons(1,since=Pogocity.__temp_time[1]))
                task3 = tg.create_task(self._Fetch_mons(2,since=Pogocity.__temp_time[2]))
                task4 = tg.create_task(self._Fetch_mons(3,since=Pogocity.__temp_time[3]))
                task5 = tg.create_task(self._Fetch_mons(4,since=Pogocity.__temp_time[4]))
                
                Pogocity.__temp_time[0]=await task1
                Pogocity.__temp_time[1]=await task2
                Pogocity.__temp_time[2]=await task3
                Pogocity.__temp_time[3]=await task4
                Pogocity.__temp_time[4]=await task5
                print("\n\n")
                print(Pogocity.__temp_time)
                await asyncio.sleep(60*1)


async def test():
    async with Pogocity()as pgct:
        await pgct.all_monster()
# asyncio.run(test())