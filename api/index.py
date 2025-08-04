from fastapi import FastAPI, Request
#from tgbot.main import tgbot
from api.functions import hook_handler
from urllib.parse import unquote, urlparse

app = FastAPI()


@app.get('/api/update')
async def update(request: Request):
    try:
        await imap_handler()
    except Exception as e:
        print(e)

