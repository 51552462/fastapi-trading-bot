from fastapi import FastAPI, Request
import json

app = FastAPI()

@app.post("/signal")
async def receive_signal(req: Request):
    data = await req.json()
    print("📩 시그널 수신:", data)
    return {"status": "ok"}
