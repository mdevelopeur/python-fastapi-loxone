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
    data = await dframe_handler(client, df)
    await process_data(client, data)
    
async def dframe_handler(client, df):
    check_date = datetime(2025, 7, 1)
    dates = await get_dates()
    df = df.sort_values(by=["ИНН", "ДАТА ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"])
    pd.options.display.max_rows = 999
    for date in df["ДАТА ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"]:
      ...
      #check_date(date)
    data = {}
    for inn in list(set(df["ИНН"].tolist())):
      rows = df[df["ИНН"] == inn]
      #print("ИНН: ", inn)
      data[inn] = []
      for index, row in rows.iterrows():
         #print("last visit: ", row["Дата последнего посещения"], isinstance(row["Дата последнего посещения"], datetime), type(row["Дата последнего посещения"]))
         parse = parse_row(row)
         if parse:
           data[inn].append(parse)
    #print("data: ", list(data.keys()))  
    
    for key in data.keys():
      #print(data[key])
      data[key] = [item for item in data[key] if item]
      #print(key
    return data
    
async def file_handler(client, fileid):
    try:
      url = await get_link(client, fileid)
    except Exception as e:
      print(e)
      df = pd.DataFrame()
      #format_headers(df)
      return df
    response = await client.get(url)
    file = io.BytesIO(response.content)
    try:
      df = pd.read_excel(file, engine='openpyxl')
      #print(df.head())
      df = format_headers(df)
      return df
    except Exception as e:      
      print(f"Error reading Excel file: {e}")
      try:
        df = pd.read_excel(file, engine='xlrd')
        #print(df.head())
        df = format_headers(df)
        return df
      except Exception as e:      
        print(f"Error reading Excel file: {e}")
        df = pd.DataFrame()
        #format_headers(df)
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
  #print(updated)
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
  
async def get_companies(client):
  url = api + "crm.requisite.list"
  body = {"select": ["ENTITY_ID", "RQ_INN"]}
  response = await client.post(url, json=body)
  response = response.json()
  companies = {}
  for item in response["result"]:
    companies[item["RQ_INN"]] = item["ENTITY_ID"]
  #companies = list(map(lambda item: {item["RQ_INN"]: item["ENTITY_ID"]}, response["result"]))
  return companies 

async def set_comments(client, companies):
  url = api + "batch"
  cmd = {}
  for company in companies:
    #comments = company["COMMENTS"]
    request = f"crm.company.update?id={company['ID']}&fields[COMMENTS]={company['COMMENTS']}"
    cmd[company['ID']] = request 
  body = { "cmd": cmd }
  response = await client.post(url, json=body)
  response = response.json()
  return response["result"]["result"]

async def get_comments(client):
  url = api + "crm.company.list"
  body = {"select": ["ID", "COMMENTS"]}
  response = await client.post(url, json=body)
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
  print("Dates: ", dates)
  return dates

def convert_date(date):
  if isinstance(date, pd.Timestamp):
    return date.to_pydatetime()
  elif isinstance(date, datetime):
    return date
  else:
    return 0

def parse_row(row):
  dict = {}
  last_date = convert_date(row["ДАТА ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"])
  next_date = convert_date(row["ДАТА СЛЕДУЮЩЕГО ПОСЕЩЕНИЯ"])
  if last_date and next_date:
    #print(last_date, next_date)
    dict["last_visit"] = last_date
    dict["next_visit"] = next_date
  else:
    return False
  dict["report"] = row["ОТЧЕТ ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"]
  dict["plan"] = row["ПЛАН ДЛЯ СЛЕДУЮЩЕГО ПОСЕЩЕНИЯ"]
  return dict

def format_headers(df):
  headers = list(df.keys())
  headers = df.columns.tolist()
  formatted_headers = list(map(lambda header: header.strip().upper(), headers))
  df = df.rename(columns=dict(zip(headers, formatted_headers)))
  return df

async def process_data(client, data):
  companies = await get_companies(client)
  comments = await get_comments(client)
  keys = list(data.keys())
  dates = await get_dates()
  print(companies.keys())
  for key in keys:
    print("ИНН: ", key)
    try:
      company = companies.get(key)
      date = dates.get(key)
      print(data[key])
      reports = list(filter(lambda item: isinstance(item["last_visit"], datetime), data[key]))
      reports.sort(key=lambda item: item["last_visit"])
      print("reports length: ", len(reports))
      print(reports[0])
      print(isinstance(reports[0]["last_visit"], datetime))
    except Exception as e:
      print("Data processing exception: ", e)
      continue
