# API Documentation

This directory contains Postman collections and environments for testing the Ride Hailing Service APIs.

## Collections

- `ride-hailing-service.postman_collection.json`: Contains all API endpoints for the ride-hailing service

## Environments

- `local-environment.postman_environment.json`: Environment variables for local development

## How to Use

1. Import the collection into Postman
2. Import the environment
3. Select the environment in Postman
4. Start making API requests

## Available Endpoints

### Driver Service (Port 8001)
- POST `/update_location/` - Update driver's location and status

### Rider Service (Port 8000)
- POST `/request_ride/` - Request a new ride

### Trip Service (Port 8002)
- GET `/trips/` - Get all active trips
- GET `/trips/{trip_id}` - Get specific trip details
- PUT `/trips/{trip_id}/status` - Update trip status
