import asyncio
import json
import math
from datetime import datetime
from typing import Dict, List
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, errors
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
RIDER_TOPIC = "rider_requests"
DRIVER_TOPIC = "driver_locations"
TRIP_UPDATES_TOPIC = "trip_updates"

# In-memory storage
available_drivers: Dict[str, dict] = {}
pending_requests: Dict[str, dict] = {}

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points using Haversine formula"""
    R = 6371  # Earth's radius in kilometers

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def find_best_driver(rider_location: dict, max_distance: float = 5.0) -> str:
    """Find the closest available driver within max_distance km"""
    best_driver_id = None
    min_distance = float('inf')

    for driver_id, driver_data in available_drivers.items():
        if driver_data["current_status"] != "available":
            continue

        distance = calculate_distance(
            rider_location["latitude"],
            rider_location["longitude"],
            driver_data["location"]["latitude"],
            driver_data["location"]["longitude"]
        )

        if distance <= max_distance and distance < min_distance:
            min_distance = distance
            best_driver_id = driver_id

    return best_driver_id

async def handle_ride_request(producer: AIOKafkaProducer, request_data: dict):
    """Process a new ride request"""
    rider_id = request_data["rider_id"]
    pickup_location = request_data["pickup"]

    # Store the request
    pending_requests[rider_id] = request_data

    # Find the best available driver
    best_driver_id = find_best_driver(pickup_location)

    if best_driver_id:
        # Create a new trip
        trip_data = {
            "trip_id": f"TRIP-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{rider_id[:4]}",
            "rider_id": rider_id,
            "driver_id": best_driver_id,
            "pickup": request_data["pickup"],
            "destination": request_data["destination"],
            "status": "driver_assigned",
            "start_time": None,
            "end_time": None,
            "fare": None,
            "current_location": available_drivers[best_driver_id]["location"]
        }

        # Update driver status
        available_drivers[best_driver_id]["current_status"] = "busy"

        # Publish trip creation event
        await producer.send_and_wait(
            TRIP_UPDATES_TOPIC,
            json.dumps(trip_data).encode("utf-8")
        )
        logger.info(f"Matched rider {rider_id} with driver {best_driver_id}")
        
        # Remove from pending requests
        del pending_requests[rider_id]
    else:
        logger.warning(f"No available drivers found for rider {rider_id}")

async def handle_driver_location(data: dict):
    """Process driver location update"""
    driver_id = data["driver_id"]
    available_drivers[driver_id] = data

    # If driver is available, check pending requests
    if data["current_status"] == "available" and pending_requests:
        # Process oldest pending request
        rider_id, request_data = next(iter(pending_requests.items()))
        await handle_ride_request(producer, request_data)

async def consume():
    # Initialize Kafka producer for sending trip updates
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    # Initialize consumer
    consumer = AIOKafkaConsumer(
        RIDER_TOPIC,
        DRIVER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="matching_service_group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    
    # Retry loop for Kafka connection
    while True:
        try:
            await consumer.start()
            logger.info("Matching Service Kafka consumer started")
            break
        except errors.KafkaConnectionError:
            logger.warning("Kafka unavailable, retrying in 3 seconds...")
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            topic = msg.topic
            data = json.loads(msg.value.decode("utf-8"))
            
            if topic == RIDER_TOPIC:
                logger.info(f"Processing ride request: {data['rider_id']}")
                await handle_ride_request(producer, data)
            elif topic == DRIVER_TOPIC:
                logger.info(f"Processing driver location: {data['driver_id']}")
                await handle_driver_location(data)
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(consume())
