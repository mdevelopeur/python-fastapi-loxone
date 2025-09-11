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
target_store = 59
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
    cmd[product["id"]] = f"catalog.storeproduct.list?filter[productid]={product["id"]}&order[]=amount"
  body = {"cmd": cmd}
  response = await client.post(url, json=body)
  response = response.json()
  responses = response["result"]["result"]
  return responses

async def create_documents(client, count):
  url = api + "batch"
  cmd = {}
  for i in range(1, count):
    cmd[i] = f"catalog.document.add?fields"
  body = {"cmd": cmd}
  response = await client.post(url, json=body)
  response = response.json()
  responses = response["result"]["result"]
  return responses

async def create_document(client):
  url = api + "catalog.document.add"
  body = {"DOC_TYPE": "S", "RESPONSIBLE_ID": 1, "CURRENCY": "RUB", "DATE_DOCUMENT": date, "COMMENTARY":""}
  response = await client.post(url, json=body)
  response = response.json()
  
  return response["result"]["id"]

async def add_products(client, products):
  url = api + "batch" 
  cmd = {}
  for product in products:
    for store in product["storeAmounts"]:
      fields = {"docId": store["document"], "storeTo": target_store, "elementId": product["PRODUCT_ID"], "amount": store["amount"], "purchasingPrice":1}
      if store["store"] != -1:
        fields["storeFrom"] = store["store"]
      fields = get_fields_string(fields)
      cmd[f"{product["id"]}:{store["store"]}"] = f"catalog.document.element.add?{fields}"
  body = {"cmd": cmd}
  response = await client.post(url, json=body)
  response = response.json()
  responses = response["result"]["result"]
  return responses
  
def get_fields_string(fields):
  strings = []
  for field in fields.keys():
    strings.append(f"fields[{field}]={fields[field]}")
  return "&".join(strings)
    
def filter_remainings(data):
  for key in data.keys():
    data[key] = list(filter(lambda i: i["amount"] is not None, item["storeProducts"]))
  return data

def process_product(product, remainings):
  for store in remainings:
    available = store["amount"] - store["reserved"]
    amount = product["QUANTITY"] if product["QUANTITY"] <= available else available
    product["storeAmounts"].append({"store": store["storeId"], "amount": amount})
    product["QUANTITY"] -= amount 
  if product["QUANTITY"] > 0:
    product["storeAmounts"].append({"store": -1, "amount": product["QUANTITY"]})
  return product 

