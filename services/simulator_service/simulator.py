import asyncio
import json
import random
import logging
from datetime import datetime
from typing import Dict, List
import aiohttp
from faker import Faker
from geopy import distance
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Faker for generating random data
fake = Faker()

# Service endpoints
DRIVER_SERVICE = "http://driver_service:8001"
RIDER_SERVICE = "http://rider_service:8000"
TRIP_SERVICE = "http://trip_service:8002"

# Bangalore area boundaries (approximately)
BANGALORE_BOUNDS = {
    "min_lat": 12.8640,
    "max_lat": 13.0730,
    "min_lng": 77.4850,
    "max_lng": 77.7520
}

class Driver:
    def __init__(self, driver_id: str, location: Dict[str, float], vehicle_type: str):
        self.driver_id = driver_id
        self.location = location
        self.vehicle_type = vehicle_type
        self.status = "available"
        self.current_trip = None
        
    def move_randomly(self, distance_km: float = 0.1):
        """Move the driver randomly within a small radius"""
        current = (self.location["latitude"], self.location["longitude"])
        # Generate random bearing
        bearing = random.uniform(0, 360)
        # Calculate new position
        d = distance.distance(kilometers=distance_km)
        new_lat, new_lng = d.destination(current, bearing)
        self.location = {
            "latitude": new_lat,
            "longitude": new_lng,
            "address": fake.street_address()
        }

class RideSimulator:
    def __init__(self, num_drivers: int = 50):
        self.drivers: Dict[str, Driver] = {}
        self.active_trips: Dict[str, dict] = {}
        # Define vehicle types with their probability distribution
        self.vehicle_types = {
            "mini": 0.4,      # 40% chance - most common
            "sedan": 0.3,     # 30% chance
            "suv": 0.2,       # 20% chance
            "premium": 0.1    # 10% chance - least common
        }
        
        # Initialize drivers with weighted vehicle distribution
        for i in range(num_drivers):
            driver_id = f"DRIVER{i:03d}"
            location = self._random_location()
            # Use probability distribution for vehicle types
            vehicle_type = random.choices(
                list(self.vehicle_types.keys()),
                weights=list(self.vehicle_types.values())
            )[0]
            self.drivers[driver_id] = Driver(driver_id, location, vehicle_type)

    def _random_location(self) -> Dict[str, float]:
        """Generate a random location within Bangalore bounds"""
        return {
            "latitude": random.uniform(BANGALORE_BOUNDS["min_lat"], BANGALORE_BOUNDS["max_lat"]),
            "longitude": random.uniform(BANGALORE_BOUNDS["min_lng"], BANGALORE_BOUNDS["max_lng"]),
            "address": fake.street_address()
        }

    async def update_driver_location(self, driver: Driver):
        """Send driver location update to the service"""
        url = f"{DRIVER_SERVICE}/update_location/"
        payload = {
            "driver_id": driver.driver_id,
            "location": driver.location,
            "current_status": driver.status,
            "vehicle": {
                "vehicle_type": driver.vehicle_type,
                "plate_number": f"KA{random.randint(1,99):02d}M{random.randint(1000,9999)}",
                "model": fake.vehicle_make_model(),
                "color": fake.color_name()
            },
            "last_updated": datetime.utcnow().isoformat()
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"Updated location for driver {driver.driver_id}")
                    else:
                        logger.error(f"Failed to update driver {driver.driver_id} location: {response.status}")
            except Exception as e:
                logger.error(f"Error updating driver location: {e}")

    async def request_ride(self):
        """Simulate a ride request with realistic patterns"""
        current_hour = datetime.now().hour
        
        # Adjust trip distance based on time of day
        if 7 <= current_hour <= 10:  # Morning commute
            min_dist, max_dist = 5, 15  # Longer trips to work
            # More trips from residential to business areas
        elif 16 <= current_hour <= 19:  # Evening commute
            min_dist, max_dist = 5, 15  # Longer trips back home
            # More trips from business to residential areas
        elif 23 <= current_hour or current_hour <= 4:  # Late night
            min_dist, max_dist = 3, 8   # Shorter trips, usually around nightlife areas
        else:  # Normal hours
            min_dist, max_dist = 2, 10  # Mixed trip lengths
        
        pickup = self._random_location()
        current = (pickup["latitude"], pickup["longitude"])
        
        # Generate more realistic bearing based on road grid pattern
        # Bangalore's road grid is roughly aligned to cardinal directions
        bearing = random.choice([
            random.uniform(0, 20),      # North-ish
            random.uniform(90, 110),    # East-ish
            random.uniform(180, 200),   # South-ish
            random.uniform(270, 290)    # West-ish
        ])
        
        dist = random.uniform(min_dist, max_dist)
        d = distance.distance(kilometers=dist)
        dest_lat, dest_lng = d.destination(current, bearing)
        
        # Ensure destination is within Bangalore bounds
        dest_lat = max(min(dest_lat, BANGALORE_BOUNDS["max_lat"]), BANGALORE_BOUNDS["min_lat"])
        dest_lng = max(min(dest_lng, BANGALORE_BOUNDS["max_lng"]), BANGALORE_BOUNDS["min_lng"])
        
        destination = {
            "latitude": dest_lat,
            "longitude": dest_lng,
            "address": fake.street_address()
        }
        
        # Generate passenger count with realistic distribution
        passenger_weights = [0.5, 0.3, 0.15, 0.05]  # Most rides are 1-2 passengers
        passenger_count = random.choices([1, 2, 3, 4], weights=passenger_weights)[0]
        
        payload = {
            "rider_id": f"RIDER{random.randint(1,999):03d}",
            "pickup": pickup,
            "destination": destination,
            "pickup_time": datetime.utcnow().isoformat(),
            "passenger_count": passenger_count
        }
        
        url = f"{RIDER_SERVICE}/request_ride/"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"Created ride request for {payload['rider_id']}")
                    else:
                        logger.error(f"Failed to create ride request: {response.status}")
            except Exception as e:
                logger.error(f"Error creating ride request: {e}")

    async def simulate_traffic(self):
        """Main simulation loop"""
        while True:
            current_hour = datetime.now().hour
            
            # Adjust request rate based on time of day
            if 7 <= current_hour <= 10:  # Morning rush hour
                base_request_rate = 5
            elif 16 <= current_hour <= 19:  # Evening rush hour
                base_request_rate = 4
            elif 23 <= current_hour or current_hour <= 4:  # Late night
                base_request_rate = 0.5
            else:  # Normal hours
                base_request_rate = 2
            
            # Update all driver locations with time-based behavior
            for driver in self.drivers.values():
                if driver.status == "available":
                    # Drivers move more during peak hours
                    movement_distance = 0.2 if (7 <= current_hour <= 10 or 16 <= current_hour <= 19) else 0.1
                    driver.move_randomly(distance_km=movement_distance)
                    await self.update_driver_location(driver)
            
            # Generate ride requests based on time-of-day adjusted Poisson distribution
            num_requests = np.random.poisson(base_request_rate)
            for _ in range(num_requests):
                await self.request_ride()
            
            # Adjust sleep time based on time of day
            if 7 <= current_hour <= 19:  # Daytime - more frequent updates
                await asyncio.sleep(3)
            else:  # Nighttime - less frequent updates
                await asyncio.sleep(8)

async def main():
    # Create simulator with 50 drivers
    simulator = RideSimulator(50)
    
    # Start simulation
    logger.info("Starting ride-hailing simulation...")
    await simulator.simulate_traffic()

if __name__ == "__main__":
    asyncio.run(main())
