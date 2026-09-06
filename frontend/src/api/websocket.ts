

import { useEffect, useRef, useCallback } from "react";
import { useSimulationStore } from "../store/simulationStore";
import type { SimulationSnapshot, HistoryEntry } from "../types/simulation";


const WS_URL = window.location.hostname === "localhost" && window.location.port !== "3000"
  ? "ws://localhost:8001/ws"
  : `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.host}/ws`;

const PING_INTERVAL_MS  = 30_000;  // send ping every 30s to keep connection alive
const INITIAL_RECONNECT = 1_000;   // start at 1s
const MAX_RECONNECT     = 30_000;  // cap at 30s

export function useSimulationWebSocket() {
  const wsRef           = useRef<WebSocket | null>(null);
  const reconnectDelay  = useRef(INITIAL_RECONNECT);
  const reconnectTimer  = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pingTimer       = useRef<ReturnType<typeof setInterval> | null>(null);
  const mountedRef      = useRef(true);


  const connect = useCallback(() => {
    const { applySnapshot, applyHistory, setConnected } = useSimulationStore.getState();
    if (!mountedRef.current) return;

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      if (!mountedRef.current) { ws.close(); return; }
      reconnectDelay.current = INITIAL_RECONNECT;
      setConnected(true);
      console.log("[WS] Connected to", WS_URL);

      // Keepalive ping
      pingTimer.current = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "ping" }));
        }
      }, PING_INTERVAL_MS);
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);

        if (msg.type === "history") {
       
          applyHistory(msg.entries as HistoryEntry[]);
        } else if (msg.type === "pong") {

        } else {

          applySnapshot(msg as SimulationSnapshot);
        }
      } catch (err) {
        console.error("[WS] Failed to parse message:", err);
      }
    };

    ws.onclose = (event) => {
      if (pingTimer.current) clearInterval(pingTimer.current);
      useSimulationStore.getState().setConnected(false);

      if (!mountedRef.current) return;

      console.log(
        `[WS] Disconnected (code=${event.code}). Reconnecting in ${reconnectDelay.current}ms...`
      );

      reconnectTimer.current = setTimeout(() => {
        reconnectDelay.current = Math.min(
          reconnectDelay.current * 2,
          MAX_RECONNECT
        );
        connect();
      }, reconnectDelay.current);
    };

    ws.onerror = (err) => {
      console.error("[WS] Error:", err);
      ws.close();
    };
  }, []);  // stable: actions read from store.getState() inside, no deps needed

  useEffect(() => {
    mountedRef.current = true;
    connect();

    return () => {
      mountedRef.current = false;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (pingTimer.current)      clearInterval(pingTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);
}


const API_BASE = window.location.port === "3000"
  ? ""                         // nginx on :3000 proxies /chaos/* to backend
  : "http://localhost:8001";   // local dev: direct to backend port

export async function killService(service: string) {
  return fetch(`${API_BASE}/chaos/kill-service`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ service }),
  });
}

export async function healAll() {
  return fetch(`${API_BASE}/chaos/heal-all`, { method: "POST" });
}

export async function addLatency(service: string, extra_ms: number) {
  return fetch(`${API_BASE}/chaos/add-latency`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ service, extra_ms }),
  });
}
