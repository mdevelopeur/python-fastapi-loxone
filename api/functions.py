from urllib.parse import unquote
from datetime import datetime
import httpx
import re
import math
import numbers
import time
import os
import redis
import random 
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")
redis_url = os.getenv("REDIS_URL")
api = os.getenv("api")
eapi = "https://eapi.pcloud.com/"
token = "AT2fZ89VHkDT7OaQZMlMlVkZdslpGwQPJNbTKpnbvQtbO8yBYcny"
headers = {"Authorization": f"Bearer {token}"}

async def main():  
  async with httpx.AsyncClient() as client:


async def get_products(client, deal):
  url = api + "crm.deal.productrows.get"
  body = {"id": deal}
  response = await client.post(url, json=body)
  response = response.json()
  return response["result"]

async def get_remaining_amounts(client, products):
  url = api + "batch"
  cmd = {}
  for product in products:
    [product["id"]] = f"catalog.storeproduct.list?filter[productid]={product["id"]}&order[]=amount"
  body = {"cmd": cmd}
  response = await client.post(url, json=body)
  response = response.json()
  responses = response["result"]["result"]
  return responses
  
