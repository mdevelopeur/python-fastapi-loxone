from fastapi import FastAPI, Request
import multipart
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
        data = await request.body()
        data = unquote(data.decode("utf-8"))
        print(data)
        id = form['data[FIELDS][ID]']
        print(id)
        await main(id)
        return request 
        ...
        #await clear_redis()
    except Exception as e:
        print(e)


