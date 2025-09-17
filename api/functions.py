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

async def main(deal):  
  async with httpx.AsyncClient() as client:
    print(deal)
    status = await check_status(client, deal)
    print(status)
    if status:
      products = await get_products(client, deal)
      remainings = await get_remaining_amounts(client, products)
      remainings = filter_remainings(remainings)
      #print(remainings[products[0]["PRODUCT_ID"]])
      for product in products:
        
        product = process_product(product, remainings[str(product["PRODUCT_ID"])])
      total = sum(list(map(lambda item: item["total"], products)))
      documents = await get_documents(client, products)
      await add_products(client, products, documents)
      if "S" in documents:
        ...
        #await update_document(client, documents["S"], total)
      #time.sleep(10)
      await confirm_documents(client, documents)
    client.close()
    
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
    cmd[product["PRODUCT_ID"]] = f"catalog.storeproduct.list?filter[product_id]={product["PRODUCT_ID"]}&order[]=amount"
  body = {"cmd": cmd}
  response = await client.post(url, json=body)
  response = response.json()
  responses = response["result"]["result"]
  print(type(responses))
  print(responses)
  return responses

async def create_documents(client, products):
  for product in products:
    ...
async def create_document(client, type):
  print("Creating document...")
  url = api + "catalog.document.add"
  fields = {"docType": type, "responsibleId": 1, "currency": "RUB", "COMMENTARY":""}
  body = {"fields": fields}
  response = await client.post(url, json=body)
  response = response.json()
  print(response)
  return response["result"]["document"]["id"]

async def add_products(client, products, documents):
  print("Adding products...")
  url = api + "batch" 
  cmd = {}
  for product in products:
    for store in product["storeAmounts"]:
      fields = {"storeTo": product["STORE_ID"], "elementId": product["PRODUCT_ID"], "amount": store["amount"], "purchasingPrice":1}
      if store["store"] != -1:
        print("-1: ", store)
        fields["storeFrom"] = store["store"]
        fields["docId"] = documents["M"]
      else:
        #print
        fields["docId"] = documents["S"]
      fields = get_fields_string(fields)
      cmd[f"{product["ID"]}:{store["store"]}"] = f"catalog.document.element.add?{fields}"
  body = {"cmd": cmd}
  response = await client.post(url, json=body)
  response = response.json()
  responses = response["result"]["result"]
  print(response)
  return responses
  
def get_fields_string(fields):
  strings = []
  for field in fields.keys():
    strings.append(f"fields[{field}]={fields[field]}")
    
  return "&".join(strings)
    
def filter_remainings(data):
  for key in data.keys():
    data[key] = list(filter(lambda item: item["amount"] is not None, data[key]["storeProducts"]))
  return data

def process_product(product, remainings):
  print("Product: ", product["PRODUCT_ID"])
  product["storeAmounts"] = []
  for store in remainings:
    reserved = store["quantityReserved"]
    
    available = store["amount"] - reserved if reserved is not None else store["amount"]
    if available is not None and available:
      amount = product["QUANTITY"] if product["QUANTITY"] <= available else available
      product["storeAmounts"].append({"product": product["PRODUCT_ID"], "store": store["storeId"], "amount": amount, "document": "M"})
      product["QUANTITY"] -= amount 
      #incomeDocRequired = True
  if product["QUANTITY"] > 0:
    product["storeAmounts"].append({"store": -1, "amount": product["QUANTITY"],  "document": "S"})
  product["total"] = int(product["QUANTITY"])
    #transferDocRequired = True 
  return product 

async def get_documents(client, products):
  documents = {}
  for product in products:
    if "S" not in documents:
      if list(filter(lambda store: store["store"] == -1, product["storeAmounts"])):     
        document = await create_document(client, "S")
        documents["S"] = document
        print(documents)
    if "M" not in documents:
      if list(filter(lambda store: store["store"] != -1, product["storeAmounts"])):
        document = await create_document(client, "M")
        documents["M"] = document
        print(documents)
    if len(documents.keys()) > 1:
      break

  return documents 

async def update_document(client, document, total):
  print("Updating document...")
  url = api + "catalog.document.update"
  body = {"id": document, "fields": {"total": total}}
  response = await client.post(url, json=body)
  response = response.json()
  responses = response["result"]
  return responses

async def confirm_documents(client, documents):
  print("Confirming...")
  url = api + "batch" 
  cmd = {}
  for document in documents.values():
    cmd[document] = f"catalog.document.confirm?id={document}"
  body = {"cmd": cmd}
  response = await client.post(url, json=body)
  response = response.json()
  responses = response["result"]["result"]
  print(responses)
  return responses

async def check_status(client, id):
  statuses = {"0":"7", "4":"C4:UC_BZT361", "6":"C6:EXECUTING"}
  url = api + f"crm.deal.get?id={id}"
  body = {"id": int(id)}
  print(body)
  response = await client.get(url)
  response = response.json()
  print(response)
  category = response["result"]["CATEGORY_ID"]
  stage = response["result"]["STAGE_ID"]
  if statuses[category] == stage:
    return True
  else: 
    return False
  #return response["result"]

#async def get_status(client, deal)
