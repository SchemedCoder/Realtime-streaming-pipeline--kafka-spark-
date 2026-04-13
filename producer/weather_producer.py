import requests
import time
import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

# Load configuration ONCE at startup (Efficient)
load_dotenv()
API_KEY = os.getenv("API_KEY")
CITIES_RAW = os.getenv("CITIES")
CITIES = json.loads(CITIES_RAW) if CITIES_RAW else []

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Use a Session for faster API calls (Professional/Future-Proof)
session = requests.Session()

def get_weather(city):
    """Fetches weather using a persistent session and safe params."""
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city.strip(), 
        "appid": API_KEY, 
        "units": "metric"
    }
    try:
        response = session.get(url, params=params, timeout=10)
        return response.json()
    except Exception as e:
        print(f"Error fetching {city}: {e}")
        return None

def run_producer():
    print(f"Producer started. Tracking {len(CITIES)} cities...")
    
    while True:
        start_time = datetime.now()
        print(f"\n--- Starting Batch at {start_time.strftime('%H:%M:%S')} ---")
        
        for city in CITIES:
            data = get_weather(city)

            # Check if the API returned valid data
            if data and data.get("main"):
                weather_payload = {
                    "city": city,
                    "country": data["sys"].get("country", "Unknown"),
                    "temperature": data["main"]["temp"],
                    "humidity": data["main"]["humidity"],
                    "timestamp": str(datetime.now())
                }

                # Send to Kafka
                producer.send("weather-data", value=weather_payload)
                print(f" [✓] Sent: {city}")
            else:
                print(f" [✗] Failed: {city}")

        # Ensure all messages are sent before sleeping
        producer.flush()
        
        print(f"Batch complete. Sleeping for 180 seconds...")
        time.sleep(180)

if __name__ == "__main__":
    run_producer()
