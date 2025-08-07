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
eapi = "https://eapi.pcloud.com/"
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
  url = eapi + "getfilelink?fileid=" + fileid
  response = await client.get(url, headers=headers)
  response = response.json()
  return "https://" + response["hosts"][0] + response["path"]

async def list_folders(client):
  url = eapi + "listfolder?path=/Отчеты КАМ"
  response = await client.get(url, headers=headers)
  response = response.json()
  folders = list(filter(lambda folder: folder["isfolder"], response["metadata"]["contents"]))
  folders = list(map(get_folder_data, folders))
  return folders 
  
def get_folder_data(folder):
  updated = folder["modified"].split(' ')
  updated = ' '.join(updated[1:5])
  updated = datetime.strptime(updated, "%d %B %Y %H %M %S")
  print(updated)
  return {"id": folder["id"], "name": folder["name"], "updated": updated}
  
