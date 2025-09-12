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
        form = await request.form()
        print(form)
        await main(form['data[FIELDS][ID]'])
        return request 
        ...
        #await clear_redis()
    except Exception as e:
        print(e)


