# test_websocket.py
import asyncio
import websockets
import json

async def test_websocket():
    try:
        uri = "ws://localhost:8000/ws"
        async with websockets.connect(uri) as websocket:
            print("✅ WebSocket connected successfully!")
            
            # Send a test message
            await websocket.send("Hello from test client")
            
            # Wait for response
            response = await websocket.recv()
            print(f"📨 Received: {response}")
            
    except Exception as e:
        print(f"❌ WebSocket connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket())