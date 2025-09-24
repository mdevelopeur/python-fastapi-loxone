from fastapi import FastAPI, Request
import multipart
import re
from api.handlers import set_time, update
from urllib.parse import unquote, urlparse

app = FastAPI()


@app.get('/api/set_time')
async def set_time(date: str, time: str):
    try:
        output = await set_time(f"{date} {time}")
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

