import { useEffect, useRef } from 'react';

const useWebSocket = (url, onMessage) => {
  const ws = useRef(null);
  const pingInterval = useRef(null);

  useEffect(() => {
    console.log('[Hook] useWebSocket hook initialized');

    const connect = () => {
      console.log(`[WebSocket] Connecting to ${url}`);
      ws.current = new WebSocket(url);

      ws.current.onopen = () => {
        console.log('[WebSocket] ✅ Connected to', url);
        ws.current.send('hello'); 
        // Start keep-alive ping every 10 seconds
        pingInterval.current = setInterval(() => {
          if (ws.current && ws.current.readyState === WebSocket.OPEN) {
            ws.current.send('ping');
            console.log('[WebSocket] 📤 Sent keep-alive ping');
          }
        }, 10000);
      };
      
      ws.current.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          onMessage(data);
        } catch (err) {
          console.warn('[WebSocket] 🔍 Non-JSON message:', event.data);
        }
      };

      ws.current.onerror = (event) => {
        console.error('[WebSocket] ❌ Error:', event);
      };

      ws.current.onclose = () => {
        console.warn('[WebSocket] ❌ Connection closed. Reconnecting in 2s...');
        clearInterval(pingInterval.current);
        setTimeout(connect, 2000);
      };
    };

    connect();

    return () => {
      console.log('[Hook] useWebSocket hook cleanup');
      ws.current?.close();
      clearInterval(pingInterval.current);
    };
  }, [url, onMessage]);

  return ws; // Optionally return ref for external send
};

export default useWebSocket;
