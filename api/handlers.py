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
import hashlib
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
admin_password = os.getenv("password")
default_password = os.getenv("default_password")
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")
api = os.getenv("api")
target_store = 59
eapi = "https://eapi.pcloud.com/"
token = "AT2fZ89VHkDT7OaQZMlMlVkZdslpGwQPJNbTKpnbvQtbO8yBYcny"
auth = httpx.BasicAuth(username="admin", password=admin_password)
#headers = {"Authorization": f"Bearer admin:{password}"}

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
  result = r.hset(timestamp, mapping={"password": str(password)})
  print(result)
  time = time + timedelta(minutes=10)
  timestamp = "loxone:" + str(int(time.timestamp()))
  print(timestamp)
  r.hset(timestamp, mapping={"password": default_password})
  
  return password


async def update():
  r = redis.Redis.from_url(redis_url, decode_responses=True)
  time = datetime.now()
  print(time)
  seconds = time.second
  time = time - timedelta(seconds=seconds) + timedelta(hours=3)
  print(time)
  timestamp = "loxone:" + str(int(time.timestamp()))
  print(timestamp)
  data = r.hgetall(timestamp)
  print(data)
  if data is not None:
    if "password" in data:
      async with httpx.AsyncClient() as client:
        hashing_data = await get_hashing_data(client)
        password = hash_password(password, hashing_data["hashAlg"], hashing_data["salt"])
        output = await set_password(client, password)
        result = r.delete(timestamp)
        print(result)
        return output

async def set_password(client, password):
  url = f"http://62.152.24.120:51087/jdev/sps/updateuserpwdh/1f73ebbb-03c4-1e8c-ffff504f94a213c3/{password}"
  response = await client.get(url, auth=auth)
  response = response.json()
  print(response)
  return response

def generate_password():
  password = ''.join(secrets.choice(string.digits) for i in range(10))
  return int(password)

async def clear_keys():
  r = redis.Redis.from_url(redis_url, decode_responses=True)
  for key in r.scan_iter("loxone:*"):
    # delete the key
    r.delete(key)

async def get_hashing_data(client):
  url = f"http://62.152.24.120:51087/jdev/sys/getKey2?User1"
  response = await client.get(url, auth=auth)
  response = response.json()
  print(response)
  return response["LL"]["value"]
  
def hash_password(password, hash_algorithm, salt):
  if hash_algorithm == 'SHA256':
    hashed_password = hashlib.sha256(f"{password}:{salt}".encode()).hexdigest().upper()
  elif hash_algorithm == 'SHA512':
    hashed_password = hashlib.sha512(f"{password}:{salt}".encode()).hexdigest().upper()
  else:
    raise Exception('Unsupported hash algorithm.')
  return hashed_password
