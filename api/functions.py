from urllib.parse import unquote
import httpx
import re
import asyncio
#import asyncpg
import time
import os
import redis
import random 
from dotenv import load_dotenv
import pandas as pd
import openpyxl
import io


load_dotenv(dotenv_path=".env")
api = os.getenv("api")
connection_string = os.getenv("postgresql")
slist = os.getenv("list").split(',')
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")
token = "AT2fZ89VHkDT7OaQZMlMlVkZdslpGwQPJNbTKpnbvQtbO8yBYcny"
headers = {"Authorization": f"Bearer {token}"}

async def hook_handler(request):
  async with httpx.AsyncClient() as client:
    url = await get_link(client, "71220424309")
    response = await client.get(url)
    file = io.BytesIO(response.content)
    try:
      df = pd.read_excel(file, engine='openpyxl')
      print(df.head())
    except Exception as e:
      print(f"Error reading Excel file: {e}")


async def get_link(client, fileid):
  url = "https://eapi.pcloud.com/getfilelink?fileid=" + fileid
  response = await client.get(url, headers=headers)
  response = response.json()
  return response["hosts"][0] + response["path"]
