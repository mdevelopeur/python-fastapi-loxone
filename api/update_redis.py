from urllib.parse import unquote
from api.functions import delete_chat, update_chat, chat_id
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
delay = int(os.getenv("delay"))*60
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
    unsorted = None
    pipeline = r.pipeline()
    for key in keys:
        if key == 'unsorted':
            await handle_unsorted()
        elif key.find('-') == -1:
            list.append(key)
            pipeline.hgetall(key)
    string = "MGET " + ', '.join(list)
    print(string)
    mget_time = int(round(time.time()*10000))
    output = pipeline.execute()
    print('pipeline execution time: ', int(round(time.time()*10000)) - mget_time)
    for row, key in zip(output, list):
            #print("row: ", key, row)
            data = await get_data(key)
            connector = data["entity_id"].split("|")[0]
            if "group" in connector:
                print("group skipped")
                continue
            if "line" not in row:
                print("skipped for no line in the row")
                continue
            #print(lines)
            #hgetall_time = int(round(time.time()*10000))        
            queue = lines[row["line"]]
            #print(queue)
            #if timestamp - int(row["time"]) > delay and timestamp - int(row["time"]) < delay * 100 and len(queue) > 1:
            if "origin" in row and len(queue) > 1:
                statuses = {}
                #print(statuses, "!")
                print(queue, row)
                for user in queue:
                    status = await get_status(user)
                    statuses[user] = status 
                print(statuses)
                if False in statuses.values():
                    for user, status in statuses.items():
                        print("#54: ", user, status, row["user"])
                        if status and user != row["user"]:
                            await update_chat(key, row["line"], user)
                            await change_user(key, user)
                        
                elif "origin" in row:
                    if row["user"] != row["origin"]:
                        await update_chat(key, row["line"], row["origin"])
                        await change_user(key, row["origin"]) 
                    else:
                        print("user is origin")
                #r.hset(key, mapping={"time": str(timestamp),"user": str(user), "line": str(row["line"])})
                '''
                try: 
                    status = True
                    try:
                        status = await get_status(user)
                        print('status: ', status)
                    except Exception as e:
                        status = True
                    if not status:
                        if (user in queue) and (len(queue) > 1):
                            #print('queue len: ', len(queue) > 1)
                            queue.remove(str(user))
                        user = queue[0]
                        status = await get_status(queue[0])
                        if status:
                            await update_chat(key, row["line"], user)
                            await change_user(key, user)
                except Exception as e:
                    print('call exception: ', e)
                '''
async def change_user(chat, user):
    print("change user: started..")
    async with httpx.AsyncClient() as client:
      print("change user: connection..")
      data = {"CHAT_ID": chat, "TRANSFER_ID": user}
      try:
          print("change user: posting..")
          response = await client.post(api + 'imopenlines.operator.transfer', data=data)
          response = response.json()
          print('transfer response: ', response)
      except Exception as e:
          print('transfer exception: ', e)
        
async def get_lines(timestamp):
    async with httpx.AsyncClient() as client:
      lines = {}
      response = await client.post(api + 'imopenlines.config.list.get')
      json = response.json()
      print('execution time: ', timestamp - time.time())
      for line in json["result"]:
          #print(line)
          data = {"CONFIG_ID": line["ID"]}
          response = await client.post(api + 'imopenlines.config.get', data=data)
          result = response.json()["result"]
          lines[result["ID"]] = result["QUEUE"]
          print(lines)
          print('execution time: ', timestamp - int(time.time()))
      return lines

async def get_status(user):
    async with httpx.AsyncClient() as client:
        data = {"USER_ID": user}
        response = await client.post(api + 'timeman.status', data=data)
        json = response.json()
        status = json["result"]["STATUS"]
        if status == "OPENED":
            return True
        else:
            return False 

async def handle_unsorted():
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    unsorted = r.hgetall('unsorted')
    print(unsorted)
    for key in unsorted.keys():
        try:
            chat = unsorted[key]
            data = await get_data(chat)
            line = data["entity_id"].split('|')[1]
            owner = data["owner"]
            print(owner)
            if int(owner) != 0:
               #hash = r.hget(chat)
               #print(hash)
               r.hset(chat, mapping={"line": line, "user": owner, "origin": owner})
               r.hdel('unsorted', key)
               print("origin set: ", chat, owner)
        except Exception as e:
            print(f"{unsorted[key]} has not been deleted for {e}")
            
async def get_data(chat):
    async with httpx.AsyncClient() as client:
        data = {"CHAT_ID": chat}
        print(data)
        try:
            response = await client.post(api +'imopenlines.dialog.get', data=data)
            response = response.json()
        
            return(response["result"])
        except Exception as e:
            print("dialog get exception: ", e)
async def get_saved_chat(chat):
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    data = r.hget(chat)
    return data
