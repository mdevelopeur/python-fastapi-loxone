from urllib.parse import unquote
import httpx
import re
import asyncio
#import aioredis
import asyncpg
import time
import os
import redis
import emoji
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
api = os.getenv("api")
connection_string = os.getenv("postgresql")
slist = os.getenv("list").split(',')
#connection_string = 'postgresql://neondb_owner:npg_rzqOTvaJiP01@ep-frosty-morning-a2z2rgqi-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
#connection_string = 'postgresql://neondb_owner:npg_ZEKV2AOWjyp9@ep-raspy-rice-a26lcgy9-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")
#connection_string = 'postgresql://neondb_owner:npg_rzqOTvaJiP01@ep-frosty-morning-a2z2rgqi-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'
#connection_string = 'postgresql://neondb_owner:npg_ZEKV2AOWjyp9@ep-raspy-rice-a26lcgy9-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require'

async def hook_handler(request):
  request = unquote(request).strip().replace("\\","")
  event = re.search('event=(.+?)&', request).group(1)
  print(type(event), event)
  print(slist)
  #event = true if event
  #chat = re.search('\[message\]\[chat_id\]=(.+?)&', request).group(1)
  #user = re.search('\[message\]\[user_id\]=(.+?)&', request).group(1)
  if event == 'ONSESSIONFINISH':
    try:
      await finish_handler(request)
    except Exception as e:
      print(e)
  elif event == 'ONOPENLINEMESSAGEADD':
    try:
      await add_handler(request)
    except Exception as e:
      print(e)  
    
def chat_code(request):
  data = {}
  request = unquote(request)
  data['id'] = re.search('\[connector_id\]=(.+?)&', request).group(1)
  data['line'] = re.search('\[connector\]\[line_id\]=(.+?)&', request).group(1)
  data['chat'] = re.search('\[connector\]\[chat_id\]=(.+?)&', request).group(1)
  data['user'] = re.search('data\[DATA\]\[connector\]\[user_id\]=(.+?)&', request).group(1)
  code = '|'.join(data.values())
  print('code: ', code)
  return code

async def chat_id(code):
  async with httpx.AsyncClient() as client:
    data = {"USER_CODE": code}
    response = await client.post(api +'imopenlines.dialog.get', data=data)
    response = response.json()
    print('chatId: ', response)
    return {"chat": str(response["result"]["id"]), "user": str(response["result"]["owner"])}
    
async def update_chat(chat, line, user):
    pool = await asyncpg.create_pool(connection_string)
    r = redis.Redis.from_url(redis_url)
    timestamp = int(time.time())
    timestamp = str(int(time.time()))
    statement = f"""
      INSERT INTO chats (id, time, line, user_id, active)
      VALUES ('{chat}', '{timestamp}', '{line}', '{user}', 'Y')
      ON CONFLICT (id)
      DO UPDATE SET id = '{chat}', time = '{timestamp}', line = '{line}', user_id = '{user}', active = 'Y';
    """
    r.hset(chat, mapping={"time": timestamp, "line": line, "user": user})
    #r.mset(mapping={f"{chat}-time": timestamp, f"{chat}-line": line, f"{chat}-user": user})
    async with pool.acquire() as conn:
    # Execute a statement to create a new table.
        await conn.execute(statement)
    await pool.close()  

async def add_handler(request):
  code = chat_code(request)
  data = await chat_id(code)
  chat = data["chat"]
  print("message text: ")
  text = re.search('\[message\]\[text\]=(.+?)&', request, re.DOTALL).group(1).lower()
  print(text)
  #if text
  emojis = emoji.emoji_list(text)
  
  for i in emojis:
    print(i["emoji"])
  emojis = len(emojis) > 0
  for i in slist:
    if text.find(i) > -1:
      emojis = True
      break
    
  #emojis = emoji.emoji_list(text)
  #chat = re.search('\[message\]\[chat_id\]=(.+?)&', request)
  user = re.search('\[message\]\[user_id\]=(.+?)&', request)
  print('message user: ', user, data["user"] == str(user))
  if user:
    user = user.group(1)
  if data["user"] == str(user) or emojis:
    await delete_chat(chat)
    
  else:
    ##code = chat_code(request)
    #data = await chat_id(code)
    
    print('add_handler: ')
    #chat = re.search('\[chat_id\]=(.+?)&', request).group(1)
    print('chat: ', chat)
    line = re.search('\[connector\]\[line_id\]=(.+?)&', request).group(1)
    print('line: ', line)
    user = data["user"]
    #user = re.search('\[user_id\]=(.+?)&', request).group(1)
    print('usee: ', user)
    if user != '0':
        await update_chat(chat, line, user)
    
async def finish_handler(request):
  chat = re.search('\[chat_id\]=(.+?)&', request)
  if chat:
    await delete_chat(chat.group(1))
    
async def delete_chat(chat):
  pool = await asyncpg.create_pool(connection_string)
  r = redis.Redis.from_url(redis_url)
  print ('deleting chat ', chat)
  dres = r.delete(str(chat))
  print(dres)
  statement = f"UPDATE chats SET active = 'N' WHERE id = '{chat}'"
  print(statement)
  #if chat == '79':
    #statement = "DELETE FROM chats WHERE id = '79'"
  async with pool.acquire() as conn:
    response = await conn.execute(statement)
    #response = await conn.fetchall("SELECT * FROM chats")
    print(response)
  await pool.close()
  print('chat ', chat, ' deleted')
#def find(array, term):
  #for i in array:
    #if 
