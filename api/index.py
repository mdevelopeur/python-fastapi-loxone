from fastapi import FastAPI, Request
#from tgbot.main import tgbot
from api.functions import main
from urllib.parse import unquote, urlparse

app = FastAPI()


@app.get('/api/update')
async def update(request: Request):
    try:
        await main()
    except Exception as e:
        print(e)

@app.post('/api/hook')
async def update(request: Request):
    try:
        request = await request.data()
        print(request)
        return request 
        ...
        #await clear_redis()
    except Exception as e:
        print(e)


