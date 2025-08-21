from fastapi import FastAPI, Request
#from tgbot.main import tgbot
from api.functions import main, clear_redis, deduplicate
from urllib.parse import unquote, urlparse

app = FastAPI()


@app.get('/api/update')
async def update(request: Request):
    try:
        await main()
    except Exception as e:
        print(e)

@app.get('/api/clear_redis')
async def update(request: Request):
    try:
        ...
        #await clear_redis()
    except Exception as e:
        print(e)

@app.get('/api/deduplicate')
async def update(request: Request):
    try:
        await deduplicate()
    except Exception as e:
        print(e)
