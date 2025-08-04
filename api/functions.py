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

async def hook_handler(request):
  async with httpx.AsyncClient() as client:
    url = "https://edef5.pcloud.com/DLZFWa8d6Z1pCUbu7ZAT2fZZP6vlVkZf7ZZlK4ZZFsFZjpZBJZhRZcDnYEyFWkG7qo6mR3Bf3UpWzrtRy/%D0%9E%D1%82%D1%87%D0%B5%D1%82%20-%20%D0%9A%D0%90%D0%9C%20%D0%9A%D0%B0%D0%BB%D0%B8%D1%87%D0%BA%D0%B8%D0%BD%20%D0%94.%D0%A1.xlsx"
    response = await client.get(url)
    file = io.BytesIO(response.content)
    try:
      df = pd.read_excel(file, engine='openpyxl')
      print(df.head())
    except Exception as e:
      print(f"Error reading Excel file: {e}")


async def chat_id(code):
  async with httpx.AsyncClient() as client:
    data = {"USER_CODE": code}
    response = await client.post(api +'imopenlines.dialog.get', data=data)
    response = response.json()
    print('chatId: ', response)
    connector = response["result"]["entity_id"].split("|")[0]
    return {"chat": str(response["result"]["id"]), "user": str(response["result"]["owner"]), "connector": connector}
    
