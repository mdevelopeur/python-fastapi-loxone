from urllib.parse import unquote
from datetime import datetime, timedelta
import httpx
import re
import math
import numbers
import time
import os
import redis
import secrets
import string
import random 
from dotenv import load_dotenv

password = os.getenv("password")
default_password = os.getenv("default_password")
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")
api = os.getenv("api")
target_store = 59
eapi = "https://eapi.pcloud.com/"
token = "AT2fZ89VHkDT7OaQZMlMlVkZdslpGwQPJNbTKpnbvQtbO8yBYcny"
headers = {"Authorization": f"Bearer admin:{password}"}

async def set_time(time):  
  print(datetime.now())
  print(time)
  r = redis.Redis.from_url(redis_url, decode_responses=True)
  print(redis_url)
  password = generate_password()
  print(password)
  time = datetime.strptime(time, "%d.%m.%Y %H:%M") 
  seconds = time.second
  time = time - timedelta(seconds=seconds)
  print(time)
  timestamp = "loxone:" + str(int(time.timestamp()))
  result = r.hset(timestamp, mapping={"password": password})
  print(result)
  time = time + timedelta(minutes=10)
  timestamp = "loxone:" + str(int(time.timestamp()))
  print(timestamp)
  r.hset(timestamp, mapping={"password": default_password})
  
  return password 


async def update():
  time = datetime.now()
  seconds = time.second
  time = time - timedelta(seconds=seconds)
  timestamp = "loxone:" + str(int(datetime.now().timestamp()))
  data = r.hgetall(timestamp)
  if data is not None:
    output = await set_password(data.password)
    r.delete(timestamp)
    return output

async def set_password(password):
  url = f"http://62.152.24.120:51087/jdev/sps/updateuserpwdh/1f73ebbb-03c4-1e8c-ffff504f94a213c3/{password}"
  async with httpx.AsyncClient() as client:
    response = await client.get(url, headers=headers)
    return response

def generate_password():
  password = ''.join(secrets.choice(string.digits) for i in range(10))
  return password
