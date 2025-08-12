from urllib.parse import unquote
from datetime import datetime
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
import xlrd
import openpyxl
import io

load_dotenv(dotenv_path=".env")
api = os.getenv("api")
connection_string = os.getenv("postgresql")
#slist = os.getenv("list").split(',')
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")
eapi = "https://eapi.pcloud.com/"
token = "AT2fZ89VHkDT7OaQZMlMlVkZdslpGwQPJNbTKpnbvQtbO8yBYcny"
headers = {"Authorization": f"Bearer {token}"}

async def main():  
  async with httpx.AsyncClient() as client:
    last_date = datetime(2025, 7, 1)
    files = []
    data_frames = []
    #df = pd.DataFrame()
    folders = await list_folders(client)
    for folder in folders:
      inner_files = await get_files(client, folder["id"], last_date)
      files.extend(inner_files)
    for file in files:
      inner_df = await file_handler(client, file["fileid"])
      data_frames.append(inner_df)
    df = pd.concat(data_frames)
    await dframe_handler(client, df)

async def dframe_handler(client, df):
    last_date = datetime(2025, 7, 1)
    dates = await get_dates()
    df = df.sort_values(by=["инн", "Дата последнего посещения"])
    pd.options.display.max_rows = 999
    for date in df["Дата последнего посещения"]:
      ...
      #check_date(date)
    for inn in list(set(df["инн"].tolist())):
      rows = df[df["инн"] == inn]
      print(rows)
      for row in rows:
         print("row: ", row["Дата последнего посещения"])
         if isinstance(row["Дата последнего посещения"], pd.Timestamp):
           print("timestamp")
        
async def file_handler(client, fileid):
    url = await get_link(client, fileid)
    response = await client.get(url)
    file = io.BytesIO(response.content)
    try:
      df = pd.read_excel(file, engine='openpyxl')
      print(df.head())
      return df
    except Exception as e:      
      print(f"Error reading Excel file: {e}")
      try:
        df = pd.read_excel(file, engine='xlrd')
        print(df.head())
        return df
      except Exception as e:      
        print(f"Error reading Excel file: {e}")
        df = pd.DataFrame()
        return df
        
async def get_link(client, fileid):
  url = eapi + "getfilelink?fileid=" + str(fileid)
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
  updated = get_time(folder["modified"])
  print(updated)
  return {"id": folder["folderid"], "name": folder["name"], "updated": updated}
  
async def get_files(client, folder_id, last_date):
  url = eapi + "listfolder?folderid=" + str(folder_id)
  response = await client.get(url, headers=headers)
  response = response.json()
  files = response["metadata"]["contents"]
  files = list(filter(lambda file: get_time(file["modified"]) > last_date, files))
  return files
    
def get_time(time):
  updated = time.split(' ')
  updated = ' '.join(updated[1:5])
  updated = datetime.strptime(updated, "%d %b %Y %H:%M:%S")
  return updated
  
async def get_requsites(client):
  url = api + "crm.requisite.list"
  body = {"select": ["ENTITY_ID", "RQ_INN"]}
  response = client.post(url, json=body)
  response = response.json()
  return response["result"]

async def set_comments(client, companies):
  url = api + "batch"
  cmd = {}
  for company in companies:
    #comments = company["COMMENTS"]
    request = f"crm.company.update?id={company['ID']}&fields[COMMENTS]={company['COMMENTS']}"
    cmd[company['ID']] = request 
  body = { "cmd": cmd }
  response = client.post(url, json=body)
  response = response.json()
  return response["result"]["result"]

async def get_comments(client, companies):
  url = api + "crm.company.list"
  body = {"select": ["ID", "COMMENTS"]}
  response = client.post(url, json=body)
  response = response.json()
  return response["result"]
  
def check_date(date):
  print(date, type(date))

async def get_dates():
  r = redis.from_url(redis_url, decode_responses=True)
  pipeline = r.pipeline()
  keys = r.keys()
  for key in keys:
    pipeline.hgetall(key)
  data = pipeline.execute()
  dates = {}
  for item in data:
    date = datetime.fromtimestamp(int(item["time"]))
    dates[item["id"]] = date
  
  return dates
