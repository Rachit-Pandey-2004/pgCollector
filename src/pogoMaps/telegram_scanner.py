from telethon import TelegramClient,events
import asyncio
from os import getcwd
from yaml import safe_load
import re
import time
from json import loads
from sys import path
path.insert(0,"%s/src/db"%getcwd())

from dbCollector import PGDB 
print("Telegram Scanner is ready")

class TeleScan:
    def __init__(self)->None:
        print("TeleScan Initiated...")
        try:
            with open(f"{getcwd()}/src/pogoMaps/configs/credentials.yaml", "r") as fs:
                cred=safe_load(fs)
                fs.close()
            with open("%s/src/pogoMaps/configs/mons.json"%getcwd(),"r")as fs:
                data=loads(fs.read())
                self.mons=data["mons_name"]
                fs.close()
        except Exception as errors:
            print("Error has hit us hard {e}".format(e=errors))
        else:
            self.client=TelegramClient(
                f"{getcwd()}/src/pogoMaps/configs/anon",
                api_id=cred["ApiId"],
                api_hash=cred["ApiHash"],
            )
        
        pass
    async def __aenter__(self):
        if(not self.client.is_connected()):
            print("connected successfully ... ")
            self.psql = PGDB()
            await self.psql.__aenter__()
            await self.client.start()
            return self
    async def __aexit__(self, exc_type, exc_value, traceback):
        if(self.client.is_connected()):
            await self.psql.__aexit__(exc_type, exc_value, traceback)
            print("closing the telegram connection ... ")
            await self.client.disconnect()
        pass
    async def data_handler(self,event):
        message_data = event.message.to_dict()
        
        if(message_data['from_id']['user_id']==6715812082):
            # cea bot
            print(f"-{message_data['message']}-")
            message=message_data['message']
            name_gender = re.search(r'^(\w+)(?:\s+(♂|♀)?\s*M)?', message, re.MULTILINE)
            name = name_gender.group(1) if name_gender else None
            gender_symbol = name_gender.group(2) if name_gender and name_gender.group(2) else None
        
            # Determine gender
            if gender_symbol == '♂':
                gender = 'M'
            elif gender_symbol == '♀':
                gender = 'F'
            elif 'M' in message.split('\n')[0]:  # Check if 'M' is present in the first line
                gender = 'N'
            else:
                gender = 'U'  # Unknown or unspecified
        
            # Extract CP and Level
            cp_lvl = re.search(r'CP:\s*(\d+)\s*LVL:\s*(\d+)', message)
            cp = cp_lvl.group(1) if cp_lvl else None
            lvl = cp_lvl.group(2) if cp_lvl else None
        
            # Extract IV (always 100% in these examples)
            iv = '100'
        
            # Extract coordinates
            coords = re.search(r'([-\d.]+),([-\d.]+)', message)
            #coordinates = f"{coords.group(1)},{coords.group(2)}" if coords else None
        
        
            # return (name, cp, lvl, gender, iv, coordinates, despawn)
            
            # Calculate despawn timestamp
            time_match = re.search(r'LV\d+ ⏰ (\d+)m (\d+)s', message)
            dsp = re.search(r'DSP:\s*\(?(\d+)m\s*(\d+)s\)?', message)
            if dsp:
                minutes = int(dsp.group(1))
                seconds = int(dsp.group(2))
                despawn = f"{minutes}m {seconds}s"
            else:
                minutes = None
                seconds = None
                despawn = None
            current_time = int(time.time())
            total_seconds = (minutes * 60) + seconds
            despawn_time = current_time + total_seconds 
            id=(self.mons).index(name)+1
            #(id, p_name, cp, lvl, gender, iv, coordinates, despawn)
            insert_data=(id,name.lower(),int(cp),int(lvl),gender.upper(),int(iv),float(coords.group(1)),float(coords.group(2)),despawn_time)
            print("@@ cea bot")
            try:
                print(insert_data)
                await self.psql.insert_single_data(insert_data)
            except Exception as e:
                print("ERROR while writing at database %s"%e)

        elif(message_data['from_id']['user_id']==928190532):
            print("##LugiaBot")
            print(message_data['message'])
            m=message_data['message']
            name_match = re.search(r'🅛 .*?([A-Za-z]+) IV💯', m)
            iv_match = re.search(r'IV💯', m)
            cp_match = re.search(r'CP(\d+) - LV(\d+)', m)
            time_match = re.search(r'LV\d+ ⏰ (\d+)m (\d+)s', m)
            coord_match = re.search(r'©️©️ ([\d.-]+),([\d.-]+)', m)
            
            if not (name_match and iv_match and cp_match and time_match and coord_match):
                print('failure')
                return None  # Return None if any essential data is missing
            if '♀' in m:
                gender= 'F'
            elif '♂' in m:
                gender= 'M'
            else:
                gender= 'N'
            p_name = name_match.group(1)
            iv = 100.0  # Since IV is always 100% based on pattern
            cp = int(cp_match.group(1))
            level = int(cp_match.group(2))
            lat, lon = map(float, coord_match.groups())
            
            # Calculate despawn timestamp
            minutes, seconds = map(int, time_match.groups())
            current_time = int(time.time())
            total_seconds = (minutes * 60) + seconds
            despawn_time= current_time + total_seconds 
            # Generate unique id (assuming CP as a placeholder, this may need modification)
            id=(self.mons).index(p_name)+1
            #(id, p_name, cp, lvl, gender, iv, coordinates, despawn)
            insert_data=(id,p_name.lower(),int(cp),int(level),gender.upper(),iv,float(lat),float(lon),despawn_time)
            print(insert_data)
            try:
                print(insert_data)
                await self.psql.insert_single_data(insert_data)
            except Exception as e:
                print("ERROR while writing at database %s"%e)
            pass


    async def search(self):
        entity = await self.client.get_entity([-1001261574447,-1002157122382])
        self.client.add_event_handler(self.data_handler,events.NewMessage(chats=entity))
        await self.client.run_until_disconnected()
        
        pass
async def t1():
    async with TeleScan() as ts:
        await ts.search()
# asyncio.run(t1())