from fastapi import WebSocket
from typing import Dict, List

class WebSocketManager:
    def __init__(self):
        # Stores connections by key = region_dma_id (e.g., "e3_222")
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, region: str, dma_id: str):
        try:
            key = f"{region.lower()}_{dma_id}"
            self.active_connections.setdefault(key, []).append(websocket)
            print(f"[WebSocketManager] ✅ Connected client to {key}")
        except Exception as e:
            print(f"[WebSocketManager] ❌ Error during connect: {e}")


    async def disconnect(self, websocket: WebSocket, region: str, dma_id: str):
        try:
            key = f"{region.lower()}_{dma_id}"
            if key in self.active_connections:
                if websocket in self.active_connections[key]:
                    self.active_connections[key].remove(websocket)
                    print(f"[WebSocketManager] ❌ Disconnected client from {key}")
                    if not self.active_connections[key]:
                        del self.active_connections[key]
        except Exception as e:
            print(f"[WebSocketManager] ❌ Error during disconnect: {e}")

    async def broadcast(self, region: str, dma_id: str, message: dict):
        key = f"{region.lower()}_{dma_id}"
        connections = self.active_connections.get(key, [])
        print(f"[WebSocketManager] 📡 Active connections for {key}: {len(connections)}")
        print(f"[WebSocketManager] 🔍 Connection Keys: {list(self.active_connections.keys())}")

        for conn in connections[:]:  # copy list to avoid modification during iteration
            try:
                await conn.send_json(message)
            except Exception as e:
                print(f"[WebSocketManager] ❌ Failed to send: {e}")
                self.active_connections[key].remove(conn)
                if not self.active_connections[key]:
                    del self.active_connections[key]

# Singleton instance
manager = WebSocketManager()