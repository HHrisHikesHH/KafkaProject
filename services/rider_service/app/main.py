from fastapi import FastAPI
from aiokafka import AIOKafkaProducer, errors
import asyncio
import uvicorn
import json
from pydantic import BaseModel
import logging

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "rider_requests"

logging.basicConfig(level=logging.INFO)

class Location(BaseModel):
    latitude: float
    longitude: float
    address: str

class RideRequest(BaseModel):
    rider_id: str
    pickup: Location
    destination: Location
    pickup_time: str  # ISO format datetime
    passenger_count: int
    special_notes: str | None = None

app = FastAPI()

producer = None

async def start_producer_with_retry():
    global producer
    loop = asyncio.get_event_loop()
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    while True:
        try:
            await producer.start()
            logging.info("Kafka Producer connected")
            break
        except errors.KafkaConnectionError:
            logging.warning("Kafka unavailable, retrying in 3 seconds...")
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    await start_producer_with_retry()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()

@app.post("/request_ride/")
async def request_ride(request: RideRequest):
    event = request.dict()
    logging.info(f"Sending event: {event}")
    await producer.send_and_wait(TOPIC, json.dumps(event).encode("utf-8"))
    logging.info("Event sent successfully")
    return {"status": "ride_requested", "event": event}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
