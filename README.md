# Ride-Hailing Microservices with Kafka

## Project Overview

This project implements a simple ride-hailing system using microservices that communicate asynchronously via Apache Kafka. It includes:

- **Rider Service:** Accepts ride booking requests and publishes ride request events to Kafka.
- **Driver Service:** Accepts driver location updates and publishes driver location events to Kafka.
- **Matching Service:** (To be implemented) Consumes rider and driver events from Kafka to match rides.
- **Kafka & Zookeeper:** Provide message brokering and coordination for reliable event streaming.

This architecture demonstrates event-driven design, loose coupling between services, and scalability with Kafka.

---

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed on your machine.
- Basic familiarity with command line.

### Clone the Repository

```bash
git clone <repository_url>
cd KafkaProject
```

### Start Services with Docker Compose

Launch all services including Kafka and Zookeeper:

```bash
sudo docker-compose up --build
```

This command builds service images and starts the containers:

- Rider Service → [http://localhost:8000](http://localhost:8000)
- Driver Service → [http://localhost:8001](http://localhost:8001)
- Kafka Broker → port **9092**
- Zookeeper → port **2181**

---

## Running the Application

After starting with Docker Compose:

- Rider and Driver services will connect to Kafka and be ready to receive API requests.
- Kafka topics `rider_requests` and `driver_locations` are auto-created or can be managed externally.

---

## Testing the Services

### Rider Service

Send a ride request using curl or Postman:

```bash
curl -X POST "http://localhost:8000/request_ride/" \
     -H "Content-Type: application/json" \
     -d '{"rider_id":"rider1","pickup":"LocationA","destination":"LocationB"}'
```

Expected response:

```json
{
  "status": "ride_requested",
  "event": {
    "rider_id": "rider1",
    "pickup": "LocationA",
    "destination": "LocationB"
  }
}
```

### Driver Service

Send a driver location update:

```bash
curl -X POST "http://localhost:8001/update_location/" \
     -H "Content-Type: application/json" \
     -d '{"driver_id":"driver123","location":"LocationX"}'
```

Expected response:

```json
{
  "status": "location_updated",
  "event": {
    "driver_id": "driver123",
    "location": "LocationX"
  }
}
```

---

## Verifying Events in Kafka Topics

To view messages sent to Kafka topics, use the Kafka console consumer inside the Kafka container:

```bash
sudo docker exec -it kafkaproject_kafka_1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic rider_requests \
  --from-beginning \
  --timeout-ms 10000
```

Replace `rider_requests` with `driver_locations` to check driver events.

---

## Additional Notes

- Kafka connection retry logic is implemented in services to ensure they wait for Kafka readiness.  
- The Matching Service will consume and process these events for ride matching (to be implemented).  
- Ensure Docker network configuration allows communication between containers by their service names.

---

This project provides a robust starting point for building scalable, event-driven ride-hailing microservices using Kafka.