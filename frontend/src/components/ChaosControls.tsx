

import { useState } from "react";
import { killService, healAll, addLatency } from "../api/websocket";
import type { SimulationSnapshot } from "../types/simulation";

interface Props {
  snapshot: SimulationSnapshot | null;
}

const SERVICES = ["gateway", "auth", "payment", "inventory", "notification"];

export function ChaosControls({ snapshot }: Props) {
  const [loading, setLoading]       = useState<string | null>(null);
  const [lastAction, setLastAction] = useState<string>("");

  async function doAction(label: string, fn: () => Promise<Response>) {
    setLoading(label);
    setLastAction("");
    try {
      const res = await fn();
      if (res.ok) {
        setLastAction(`✓ ${label}`);
      } else {
        const body = await res.text().catch(() => "");
        setLastAction(`✗ ${label}: ${res.status} ${body.slice(0, 80)}`);
      }
    } catch (err: any) {
      setLastAction(`✗ ${label}: ${err.message}`);
    } finally {
      setLoading(null);
    }
  }

  return (
    <div className="space-y-4">
      {}
      <div>
        <p className="text-xs text-gray-400 mb-2 font-mono">Kill Service</p>
        <div className="flex flex-wrap gap-2">
          {SERVICES.map((svc) => {
            const state = snapshot?.services[svc];
            const alive = state?.is_alive ?? true;
            return (
              <button
                key={svc}
                onClick={() => doAction(`kill:${svc}`, () => killService(svc))}
                disabled={!!loading || !alive}
                className="px-2 py-1 text-xs font-mono rounded border transition-colors"
                style={{
                  background:   alive ? "#7f1d1d" : "#1f2937",
                  borderColor:  alive ? "#ef4444" : "#374151",
                  color:        alive ? "#fca5a5" : "#6b7280",
                  cursor:       loading ? "not-allowed" : alive ? "pointer" : "not-allowed",
                }}
              >
                ☠ {svc}
              </button>
            );
          })}
        </div>
      </div>

      {/* Add latency */}
      <div>
        <p className="text-xs text-gray-400 mb-2 font-mono">Add 500ms Latency</p>
        <div className="flex flex-wrap gap-2">
          {SERVICES.map((svc) => (
            <button
              key={svc}
              onClick={() => doAction(`+latency:${svc}`, () => addLatency(svc, 500))}
              disabled={!!loading}
              className="px-2 py-1 text-xs font-mono rounded border transition-colors"
              style={{
                background:  "#713f12",
                borderColor: "#f97316",
                color:       "#fed7aa",
                cursor:      loading ? "not-allowed" : "pointer",
              }}
            >
              ⏱ {svc}
            </button>
          ))}
        </div>
      </div>

      {/* Heal all */}
      <div>
        <button
          onClick={() => doAction("heal-all", healAll)}
          disabled={!!loading}
          className="px-3 py-1.5 text-xs font-mono rounded border transition-colors w-full"
          style={{
            background:  "#14532d",
            borderColor: "#22c55e",
            color:       "#86efac",
            cursor:      loading ? "not-allowed" : "pointer",
          }}
        >
          ✓ Heal All Services
        </button>
      </div>

      {/* Last action feedback */}
      {lastAction && (
        <p
          className="text-xs font-mono px-2 py-1 rounded"
          style={{
            background: lastAction.startsWith("✓") ? "#14532d" : "#7f1d1d",
            color:      lastAction.startsWith("✓") ? "#86efac" : "#fca5a5",
          }}
        >
          {lastAction}
        </p>
      )}

      {/* Info note */}
      <p className="text-xs text-gray-600 font-mono">
        Kill = reject all reqs · Latency = add 500ms · Heal = restore all
      </p>
    </div>
  );
}
