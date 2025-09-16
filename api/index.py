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
        form = await request.body()
        print(form.decode("utf-8"))
        id = form['data[FIELDS][ID]']
        print(id)
        await main(id)
        return request 
        ...
        #await clear_redis()
    except Exception as e:
        print(e)


