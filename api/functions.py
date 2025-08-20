from urllib.parse import unquote
from datetime import datetime
import httpx
import re
import math
import numbers
import asyncio
import time
import os
import redis
import random 
from dotenv import load_dotenv
import pandas as pd
import xlrd
import openpyxl
import io
import unicodedata

load_dotenv(dotenv_path=".env")
api = os.getenv("api")
connection_string = os.getenv("postgresql")
load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")
eapi = "https://eapi.pcloud.com/"
token = "AT2fZ89VHkDT7OaQZMlMlVkZdslpGwQPJNbTKpnbvQtbO8yBYcny"
headers = {"Authorization": f"Bearer {token}"}

async def main():  
  async with httpx.AsyncClient() as client:
    files = []
    data_frames = []
    folders = await list_folders(client)
    for folder in folders:
      inner_files = await get_files(client, folder["id"])
      files.extend(inner_files)
    for file in files:
      print(file["name"])
      inner_df = await file_handler(client, file["fileid"])
      data_frames.append(inner_df)
    df = pd.concat(data_frames)
    data = await dframe_handler(client, df)
    await process_data(client, data)
    
async def dframe_handler(client, df):
    check_date = datetime(2025, 7, 1)
    df = df.sort_values(by=["ИНН", "ДАТА ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"])
    pd.options.display.max_rows = 999
    for date in df["ДАТА ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"]:
      ...
    data = {}
    print("ИНН в df: ", list(set(df["ИНН"].tolist())))
    for inn in list(set(df["ИНН"].tolist())):
      rows = df[df["ИНН"] == inn]
      inn = check_rq(inn)
      if inn:       
        data[inn] = {"reports": [], "plans": []}
        for index, row in rows.iterrows():
          report = get_report(row)
          plan = get_plan(row)
          data[inn]["reports"].extend(report)
          data[inn]["plans"].extend(plan)

    return data

def check_rq(rq):
    try:
      rq = str(int(rq))
    except Exception as e:
      print(rq, e)
      return False
    if len(rq) == 10:
      return rq
    else:
      return False
      
async def file_handler(client, fileid):
    try:
      url = await get_link(client, fileid)
    except Exception as e:
      print(e)
      df = pd.DataFrame()
      return df
    response = await client.get(url)
    file = io.BytesIO(response.content)
    try:
      df = pd.read_excel(file, engine='openpyxl')
      df = format_headers(df)
      return df
    except Exception as e:      
      print(f"Error reading Excel file: {e}")
      try:
        df = pd.read_excel(file, engine='xlrd')
        df = format_headers(df)
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
  return {"id": folder["folderid"], "name": folder["name"], "updated": updated}
  
async def get_files(client, folder_id):
  url = eapi + "listfolder?folderid=" + str(folder_id)
  response = await client.get(url, headers=headers)
  response = response.json()
  files = response["metadata"]["contents"]
  return files
    
def get_time(time):
  updated = time.split(' ')
  updated = ' '.join(updated[1:5])
  updated = datetime.strptime(updated, "%d %b %Y %H:%M:%S")
  return updated

async def get_companies(client, rq):
  url = api + "crm.requisite.list"
  body = {"select": ["ENTITY_ID", "RQ_INN"], "filter": {"ENTITY_TYPE_ID": 4, "RQ_INN": rq}}
  response = await client.post(url, json=body)
  response = response.json()
  print(response["result"])
  companies = list(map(lambda item: item["ENTITY_ID"], response["result"]))
  return companies
  
'''   
async def get_companies(client):
  url = api + "crm.requisite.list"
  body = {"select": ["ENTITY_ID", "RQ_INN"]}
  response = await client.post(url, json=body)
  response = response.json()
  companies = {}
  for item in response["result"]:
    try:
      companies[int(item["RQ_INN"])] = item["ENTITY_ID"]
    except Exception as e:
      continue 
  print(companies)
  return companies 
'''
async def set_comments(client, companies):
  url = api + "batch"
  cmd = {}
  for company in companies:
    request = f"crm.company.update?id={company['ID']}&fields[COMMENTS]={company['COMMENTS']}"
    cmd[company['ID']] = request 
  body = { "cmd": cmd }
  response = await client.post(url, json=body)
  response = response.json()
  return response["result"]["result"]
  
def check_date(date):
  print(date, type(date))

async def get_all_dates(r):
  pipeline = r.pipeline()
  keys = r.keys()
  for key in keys:
    pipeline.hgetall(key)
  data = pipeline.execute()
  dates = {}
  for item in data:
    dates[item["id"]] = item
  print("Dates: ", dates)
  return dates

def get_dates(dates, inn):
  if inn not in dates:
    dates[inn] = {}
  print(inn, "dates: ", dates[inn])
  dates[inn]["last"] = datetime.fromtimestamp(int(float(dates[inn].get("last", 0))))
  dates[inn]["next"] = datetime.fromtimestamp(int(float(dates[inn].get("next", 0))))
  return dates[inn]

def convert_date(date):
  if pd.isna(date):
    return False
  if isinstance(date, pd.Timestamp):
    return date.to_pydatetime()
  elif isinstance(date, datetime):
    return date
  else:
    return False

def get_report(row):
  dict = {}
  last_date = convert_date(row["ДАТА ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"])
  report = row["ОТЧЕТ ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"]
  if last_date and isinstance(report, str):
    dict["date"] = last_date
    last_date = last_date.strftime("%d.%m.%y")    
    dict["text"] = f"{last_date}:\n{report}"
    return [dict]
  else:
    return []

def get_plan(row):
  dict = {}
  next_date = convert_date(row["ДАТА ПОСЛЕДНЕГО ПОСЕЩЕНИЯ"])
  plan = row["ПЛАН ДЛЯ СЛЕДУЮЩЕГО ПОСЕЩЕНИЯ"]
  if next_date and isinstance(plan, str):
    dict["date"] = next_date
    date = next_date.strftime("%d.%m.%y")    
    dict["text"] = f"{date}:\n{plan}"
    return [dict]
  else:
    return []
    
def format_headers(df):
  headers = list(df.keys())
  headers = df.columns.tolist()
  formatted_headers = list(map(lambda header: header.strip().upper(), headers))
  df = df.rename(columns=dict(zip(headers, formatted_headers)))
  return df

async def process_data(client, data):
  r = redis.from_url(redis_url, decode_responses=True)
  keys = list(data.keys())
  all_dates = await get_all_dates(r)
  print("Keys: ", keys)
  for key in keys:
    print("ИНН: ", key)    
    print(data[key])
    companies = await get_companies(client, key)
    if companies: 
      for company in companies:
        dates = get_dates(all_dates, company)
        reports = list(filter(lambda item: isinstance(item["date"], datetime) and not pd.isna(item["date"]) and item["date"] > dates["last"], data[key]["reports"]))
        plans = list(filter(lambda item: isinstance(item["date"], datetime) and not pd.isna(item["date"]) and item["date"] > dates["next"], data[key]["plans"]))
        if reports or plans:
          try: 
            plan_processed = report_processed = False
            if reports:
              #print(dates)           
              #for report in reports:
              #print(report["date"], dates["last"], report["date"] > dates["last"]) 
              reports.sort(key=lambda item: item["date"])          
              report_processed = await process_report(client, reports[0], company)
              dates["last"] = reports[0]["date"]
            if plans:
              
              plans.sort(key=lambda item: item["date"])          
              plan_processed = await process_plan(client, plans[0], company)
              dates["next"] = plans[0]["date"]
      
            if report_processed or plan_processed:      
              response = r.hset(company, mapping={"id": company, "last": int(dates["last"].timestamp()), "next": int(dates["next"].timestamp())})
              print("Redis response: ", response)
          except Exception as e:
            print("Data processing exception: ", e)

async def process_report(client, report, company):
    #print(report)
    date = report["date"]
    url = api + "crm.timeline.comment.add"
    body = {"fields":{"ENTITY_ID": company,"ENTITY_TYPE": "company","COMMENT": report["text"]}}
    #print(body)
    response = await client.post(url, json=body) 
    response = response.json()
    print(response)
    if response.get("result") is not None:
      return True 
    else:
      return False
      
async def process_plan(client, plan, company):
  #print(plan)
  url = api + "crm.activity.todo.add"
  deadline = plan["date"].strftime("%Y-%m-%dT%H:%M:%S")
  body = {"ownerTypeId": 4, "ownerId": int(company), "deadline": deadline, "title": "Проверка", "description": plan["text"]}
  response = await client.post(url, json=body)
  response = response.json()
  print(response)
  if response.get("result") is not None:
    if "id" in response["result"]:
      return True 
  
  return False  

async def clear_redis():
  r = redis.from_url(redis_url, decode_responses=True)
  r.flushdb()

async def deduplicate():
  r = redis.from_url(redis_url, decode_responses=True)
  keys = r.keys()
  async with httpx.AsyncClient() as client:
    for key in keys:
      comments = await get_comments(client, key)
      for comment in comments:
        duplicates = list(filter(lambda item: item["ENTITY_ID"] == comment["ENTITY_ID"] and item["COMMENT"] == comment["COMMENT"], comments))
        if len(duplicates) > 1 and duplicates[0] not in deleted:
          await delete_comment(client, comment["ID"])
          print(duplicates)
          return 
        

async def get_comments(client, id):
    url = api + "crm.timeline.comment.list"
    body = {"filter":{"ENTITY_TYPE": "company", "ENTITY_ID": id}, "select": ["ID", "COMMENT", "ENTITY_ID"]}
    response = await client.post(url, json=body)
    response = response.json()
    return response["result"]
  
async def delete_comment(client, id):
    url = api + "crm.timeline.comment.delete"
    body = {"ID": id}
    response = await client.post(url, json=body)
    response = response.json()
    return response["result"]
