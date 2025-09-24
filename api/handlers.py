from urllib.parse import unquote
from datetime import datetime
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
  r = redis.Redis.from_url(redis_url, decode_responses=True)
  password = generate_password()
  time = datetime.strptime(time, "")
  timestamp = "loxone:" + str(int(time.timestamp()))[0:7]
  r.hset(timestamp, mapping={"password": password})
  time = time + timedelta(hours=1)
  timestamp = "loxone:" + str(int(time.timestamp()))[0:7]
  r.hset(timestamp, mapping={"password": default_password})
  
  return password 


async def update():
  timestamp = "loxone:" + str(int(datetime.now().timestamp()))
  data = r.hgetall(timestamp)
  if data is not None:
    output = await set_password(data.password)
    return output

async def set_password(password):
  url = 
  async with httpx.AsyncClient() as client:
    output = await client.get(url, headers=headers)
    return output

def generate_password():
  password = ''.join(secrets.choice(string.digits) for i in range(10))
  return password
