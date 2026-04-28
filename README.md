# Weather-streaming-pipeline-
 Real-Time Global Weather Streaming Pipeline
This project is an end-to-end data engineering pipeline that tracks live weather data for 36 global cities (including Delhi, Lucknow, and Bangalore). It moves data from a live API into a cloud warehouse for real-time analytics.

The goal was to move away from simple "static" data and build a Medallion Architecture that handles streaming data with a 1-minute latency.

 The Architecture
The pipeline follows a modern "Lakehouse" pattern:
OpenWeather API → Kafka → Spark Bridge → Snowflake (Bronze/Silver/Gold) → Streamlit Dashboard

Tech Stack
Ingestion: Python (Requests) & Apache Kafka

Processing: PySpark (Structured Streaming)

Storage: Snowflake (using Dynamic Tables for automated ELT)

Visualization: Streamlit (Local & Cloud versions)

Environment: Python 3.11, Docker, .env for secret management

 Project Structure
producer/: The "Heartbeat" script that polls the API and pushes to Kafka.

spark_job/: The bridge that picks up Kafka streams and writes them to Snowflake.

sql/: The Medallion setup. This is where the raw JSON becomes clean, structured data.

dashboard/: A Streamlit UI to visualize temperature and humidity trends.

screenshots/: Visual proof of the pipeline working in Snowflake and the UI.

 The "Medallion" Logic
I implemented a three-layer storage strategy in Snowflake to ensure data quality:

Bronze (Raw): Every JSON message from the API is dumped here as-is. No data is ever lost.

Silver (Cleaned): A Dynamic Table that extracts the JSON fields (Temp, Humidity, City) and filters out "ghost data" (e.g., temperatures outside -60°C to 60°C).

Gold (Analytics): The final layer that calculates 1-minute rolling averages. This is what powers the dashboard.

 How to Run It
1. Prereqs
Kafka running locally.

Snowflake account with a WEATHER_WH warehouse.

OpenWeather API Key.

2. Execution Order
Launch Kafka: Run scripts/start_kafka.bat.

Start the Bridge: python spark_job/weather_to_snowflake.py

Fire up the Producer: python producer/weather_producer.py

Open the Dashboard: streamlit run dashboard/app.py

 Challenges I Solved
Schema Evolution: I originally built this for temperature only. I later updated the entire pipeline (Producer to Gold layer) to include Humidity without breaking the existing data.

Resource Management: Implemented requests.Session() in the producer to handle 36 API calls more efficiently, reducing network overhead.

Data Persistence: Configured the dashboard to pull from the Gold layer so that users can view the last known weather even if the local Kafka stream is offline.
