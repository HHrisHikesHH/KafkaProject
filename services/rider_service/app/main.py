from fastapi import FastAPI
from aiokafka import AIOKafkaProducer
import asyncio
import uvicorn

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "rider_requests"

producer = None

@app.on_event("startup")
async def startup_event():
    global producer
    loop = asyncio.get_event_loop()
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()

@app.post("/request_ride/")
async def request_ride(rider_id: str, pickup: str, destination: str):
    event = {
        "rider_id": rider_id,
        "pickup": pickup,
        "destination": destination,
    }
    await producer.send_and_wait(TOPIC, str(event).encode("utf-8"))
    return {"status": "ride_requested", "event": event}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
