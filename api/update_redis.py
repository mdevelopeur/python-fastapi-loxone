from urllib.parse import unquote
from api.functions import delete_chat
import httpx
import re
import os
from dotenv import load_dotenv
import asyncio
import asyncpg
import time
#import aioredis
import redis

load_dotenv(dotenv_path=".env")
api = os.getenv("api")
connection_string = os.getenv("postgresql")
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")

async def redis_update_handler():
    print(api, connection_string, redis_url)
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    timestamp = int(time.time())
    lines = await get_lines(timestamp)
    statement = "SELECT * FROM chats"
    keys = r.keys()
    list = []
    pipeline = r.pipeline()
    for key in keys:
        #list.append(f"{key}-time, {key}-user, {key}-line")
        if key.find('-') == -1:
            list.append(key)
            pipeline.hgetall(key)
    string = "MGET " + ', '.join(list)
    print(string)
    mget_time = int(round(time.time()*10000))
    output = pipeline.execute()
    print('pipeline execution time: ', int(round(time.time()*10000)) - mget_time)
    #for row in output:
        #row["id"] = 
    for row, key in zip(output, list):
            hgetall_time = int(round(time.time()*10000))
            #row = dict(r.hgetall(key))
            #print('hgetall time: ', int(round(time.time()*10000)) - hgetall_time)
            #print(row)
            #print('execution time: ', timestamp - time.time())
            #print(timestamp - int(row["time"]))
            #print('row time: ', row["time"])
            if timestamp - int(row["time"]) > 240 and timestamp - int(row["time"]) < 1000:
                print('key: ', type(key))
                print('secs: ', int(row["time"]) - timestamp)
                print('queue: ', lines[row["line"]])
                user = row["user"]
                queue = lines[row["line"]]
                print('user :', user)
                for line in lines[row["line"]]:
                    print(line, user, user == line)
                if user in queue and len(queue) > 0:
                    queue.remove(str(user))
                user = queue[0]
                print('line: ', lines[row["line"]])
                print('user: ', user)
                r.hset(key, mapping={"time": str(timestamp),"user": str(user), "line": str(row["line"])})
                try:
                    await change_user(key, user)
                except Exception as e:
                    print('call exception: ', e)
                
                #await conn.execute(f"UPDATE chats SET time = '{str(timestamp)}', user_id = '{str(user)}' WHERE id = '{row["id"]}'")
            #elif timestamp - int(row["time"]) < 400:
               # r.delete(
    
async def change_user(chat, user):
    print("change user: started..")
    async with httpx.AsyncClient() as client:
      print("change user: connection..")
      data = {"CHAT_ID": chat, "TRANSFER_ID": user}
      try:
          print("change user: posting..")
          response = await client.post('https://bitrix.abramovteam.ru/rest/1/0bwuq2j93zpaxkie/imopenlines.operator.transfer', data=data)
          response = response.json()
          print('transfer response: ', response)
      except Exception as e:
          print('transfer exception: ', e)
        
async def get_lines(timestamp):
    async with httpx.AsyncClient() as client:
      lines = {}
      response = await client.post('https://bitrix.abramovteam.ru/rest/1/0bwuq2j93zpaxkie/imopenlines.config.list.get')
      json = response.json()
      print('execution time: ', timestamp - time.time())
      for line in json["result"]:
          #print(line)
          data = {"CONFIG_ID": line["ID"]}
          response = await client.post('https://bitrix.abramovteam.ru/rest/1/0bwuq2j93zpaxkie/imopenlines.config.get', data=data)
          result = response.json()["result"]
          lines[result["ID"]] = result["QUEUE"]
          print(lines)
          print('execution time: ', timestamp - int(time.time()))
      return lines
