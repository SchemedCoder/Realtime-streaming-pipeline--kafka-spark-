@echo off
:: Replace with your actual UUID
set KAFKA_UUID=4g-DUZxbRVOYEXDRfF6G8g

echo [1/4] Stopping existing Java/Kafka processes...
taskkill /F /IM java.exe /T 2>nul

echo [2/4] Wiping local data folder to prevent Windows file-locks...
rd /s /q "C:\kafka\data"
mkdir "C:\kafka\data"

echo [3/4] Re-formatting storage with UUID...
call C:\kafka\bin\windows\kafka-storage.bat format -t %KAFKA_UUID% -c C:\kafka\config\kraft\server.properties

echo [4/4] Launching Kafka Server...
call C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\kraft\server.properties
