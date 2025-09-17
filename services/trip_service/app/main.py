from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, errors
import asyncio
import json
from enum import Enum
from datetime import datetime
from pydantic import BaseModel
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TRIP_UPDATES_TOPIC = "trip_updates"
RIDER_TOPIC = "rider_requests"
DRIVER_TOPIC = "driver_locations"

class TripStatus(str, Enum):
    """Available trip status values. All values must be in lowercase."""
    created = "created"
    driver_assigned = "driver_assigned"
    driver_arrived = "driver_arrived"
    trip_started = "trip_started"
    trip_completed = "trip_completed"
    cancelled = "cancelled"

class Location(BaseModel):
    latitude: float
    longitude: float
    address: Optional[str] = None

class Trip(BaseModel):
    trip_id: str
    rider_id: str
    driver_id: Optional[str] = None
    pickup: Location
    destination: Location
    status: TripStatus
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    fare: Optional[float] = None
    current_location: Optional[Location] = None

app = FastAPI()
producer = None
active_trips = {}  # In-memory storage for active trips

async def start_kafka_producer():
    global producer
    while True:
        try:
            producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            await producer.start()
            logger.info("Kafka producer started successfully")
            break
        except errors.KafkaConnectionError:
            logger.warning("Kafka unavailable, retrying in 3 seconds...")
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    await start_kafka_producer()
    # Start consuming trip-related events
    asyncio.create_task(consume_trip_events())

@app.on_event("shutdown")
async def shutdown_event():
    if producer:
        await producer.stop()

async def consume_trip_events():
    consumer = AIOKafkaConsumer(
        TRIP_UPDATES_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="trip_service_group",
        auto_offset_reset="earliest"
    )
    
    while True:
        try:
            await consumer.start()
            logger.info("Kafka consumer started successfully")
            break
        except errors.KafkaConnectionError:
            logger.warning("Kafka unavailable, retrying in 3 seconds...")
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            trip_id = data.get("trip_id")
            if trip_id:
                active_trips[trip_id] = Trip(**data)
    finally:
        await consumer.stop()

@app.post("/trips/")
async def create_trip(trip: Trip):
    trip_data = trip.dict()
    await producer.send_and_wait(
        TRIP_UPDATES_TOPIC,
        json.dumps(trip_data).encode("utf-8")
    )
    active_trips[trip.trip_id] = trip
    return {"status": "success", "trip": trip_data}

@app.get("/trips/{trip_id}")
async def get_trip(trip_id: str):
    if trip_id not in active_trips:
        raise HTTPException(status_code=404, detail="Trip not found")
    return active_trips[trip_id]

class TripStatusUpdate(BaseModel):
    status: TripStatus
    location: Optional[Location] = None

@app.put("/trips/{trip_id}/status")
async def update_trip_status(trip_id: str, update: TripStatusUpdate):
    if trip_id not in active_trips:
        raise HTTPException(status_code=404, detail="Trip not found")
    
    trip = active_trips[trip_id]
    trip.status = update.status
    if update.location:
        trip.current_location = update.location
    
    if update.status == TripStatus.trip_started:
        trip.start_time = datetime.utcnow().isoformat()
    elif update.status == TripStatus.trip_completed:
        trip.end_time = datetime.utcnow().isoformat()
        # TODO: Implement fare calculation based on distance and time
    
    await producer.send_and_wait(
        TRIP_UPDATES_TOPIC,
        json.dumps(trip.dict()).encode("utf-8")
    )
    return trip

@app.get("/trips/")
async def list_active_trips():
    return list(active_trips.values())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=True)
