import asyncio
from aiokafka import AIOKafkaProducer, errors
from fastapi import FastAPI
import json
import uvicorn
from pydantic import BaseModel

class LocationUpdate(BaseModel):
    driver_id: str
    location: str
app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "driver_locations"

producer = None

async def start_producer_with_retry():
    global producer
    loop = asyncio.get_event_loop()
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    while True:
        try:
            await producer.start()
            print("Kafka Producer started successfully")
            break
        except errors.KafkaConnectionError:
            print("Kafka unavailable, retrying in 3 seconds...")
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    await start_producer_with_retry()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()

@app.post("/update_location/")
async def update_location(request: LocationUpdate):
    event = request.dict()
    await producer.send_and_wait(TOPIC, json.dumps(event).encode("utf-8"))
    return {"status": "location_updated", "event": event}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
