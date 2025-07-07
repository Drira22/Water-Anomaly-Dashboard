#!/bin/bash

# Ensure logs directory exists
mkdir -p logs

# === Start Backend ===
echo "🌐 Starting FastAPI backend..."
nohup uvicorn dashboard.backend.main:app --host 0.0.0.0 --port 8000 > logs/backend.log 2>&1 &

# === Start Frontend ===
echo "🖥️ Starting React frontend..."
cd dashboard/frontend
nohup npm run dev > ../logs/frontend.log 2>&1 &
cd ../..

# === Start Kafka Producer ===
echo "📤 Starting Kafka producer..."
nohup python -m kafka_app.producer.producer_manager > logs/producer.log 2>&1 &

# === Start Kafka Consumer ===
echo "📥 Starting Kafka consumer..."
nohup python -m kafka_app.consumer.dma_consumer > logs/consumer.log 2>&1 &

# === Open 4 GNOME terminal windows to follow logs ===
echo "🔍 Opening terminal windows to follow logs..."
gnome-terminal -- bash -c "tail -f logs/backend.log; exec bash" &
gnome-terminal -- bash -c "tail -f logs/frontend.log; exec bash" &
gnome-terminal -- bash -c "tail -f logs/producer.log; exec bash" &
gnome-terminal -- bash -c "tail -f logs/consumer.log; exec bash" &

echo "✅ All services started and logs are being tailed in new terminals."
