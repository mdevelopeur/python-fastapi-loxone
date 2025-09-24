from fastapi import FastAPI, Request
import multipart
import re
from api.handlers import set, update
from urllib.parse import unquote, urlparse

app = FastAPI()


@app.get('/api/set')
async def set(date: str, time: str):
    try:
        output = await set(f"{date} {time}")
        print(output)
        return output
    except Exception as e:
        print(e)
        return e

@app.get('/api/update')
async def update(request: Request):
    try:
        output = await update()
        print(output)
        return output 
    except Exception as e:
        print(e)
        return e

