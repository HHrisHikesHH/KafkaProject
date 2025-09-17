import asyncio
from aiokafka import AIOKafkaConsumer
import json

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPICS = ["rider_requests", "driver_locations"]

async def consume():
    consumer = AIOKafkaConsumer(
        *TOPICS,
        loop=asyncio.get_event_loop(),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="matching_service_group",
        auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value.decode("utf-8"))
            print(f"[Matching] Topic={msg.topic} Event={event}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())
