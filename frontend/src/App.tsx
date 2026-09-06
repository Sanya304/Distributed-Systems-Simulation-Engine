import { useSimulationStore, selectServicesDict, selectHistory, selectStorms } from "./store/simulationStore";
import { useSimulationWebSocket } from "./api/websocket";
import { ServiceGraph }    from "./components/ServiceGraph";
import { ServiceTable }    from "./components/ServiceTable";
import { LatencyChart }    from "./components/LatencyChart";
import { RetryStormChart } from "./components/RetryStormChart";
import { LiveFeed }        from "./components/LiveFeed";
import { ChaosControls }   from "./components/ChaosControls";
import { type ReactNode, useMemo }  from "react";

export default function App() {
  useSimulationWebSocket();
  const connected     = useSimulationStore((s) => s.connected);
  const snapshot      = useSimulationStore((s) => s.snapshot);
  const servicesDict  = useSimulationStore(selectServicesDict);
  const history       = useSimulationStore(selectHistory);
  const storms        = useSimulationStore(selectStorms);
  // Convert services dict to array inside render (memoized on dict reference)
  const services      = useMemo(() => Object.values(servicesDict), [servicesDict]);
  const system    = snapshot?.system;
  const errPct    = system?.error_pct ?? 0;
  const errColor  = errPct < 5 ? "#22c55e" : errPct < 15 ? "#eab308" : "#ef4444";

  return (
    <div style={{ background: "#030712", minHeight: "100vh", color: "#f9fafb", fontFamily: "monospace" }}>
      <div style={{ background: "#111827", borderBottom: "1px solid #1f2937", padding: "10px 20px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <span style={{ color: "#38bdf8", fontWeight: 700, fontSize: "16px" }}>◈ Distributed Simulation Engine</span>
          <span style={{ color: "#6b7280", fontSize: "12px", marginLeft: "16px" }}>Stage 6 — React Dashboard</span>
        </div>
        <div style={{ display: "flex", gap: "24px", alignItems: "center", fontSize: "13px" }}>
          {system && <>
            <Stat label="Requests"   value={system.total_requests.toLocaleString()} color="#d1d5db" />
            <Stat label="Errors"     value={system.total_errors.toLocaleString()}   color="#fca5a5" />
            <Stat label="Error Rate" value={`${errPct.toFixed(1)}%`}               color={errColor} />
          </>}
          {snapshot && <Stat label="Seq" value={`#${snapshot.seq}`} color="#6b7280" />}
          <ConnBadge connected={connected} />
        </div>
      </div>

      <div style={{ padding: "12px", display: "flex", flexDirection: "column", gap: "12px" }}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", height: "300px" }}>
          <Panel title="Service Graph" subtitle="green=CLOSED · red=OPEN · arc=CPU%">
            <ServiceGraph snapshot={snapshot} />
          </Panel>
          <Panel title="Service Metrics" subtitle="live per-service stats">
            <div style={{ height: "100%", overflowY: "auto" }}><ServiceTable services={services} /></div>
          </Panel>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", height: "220px" }}>
          <Panel title="Latency (60s)" subtitle="avg response time per service">
            <LatencyChart history={history} />
          </Panel>
          <Panel title="Error Rate & Storms" subtitle="error% over time · ⚡ = active storm">
            <RetryStormChart history={history} activeStorms={storms} />
          </Panel>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 280px", gap: "12px", height: "200px" }}>
          <Panel title="Live Event Feed" subtitle="real-time event stream">
            <LiveFeed events={snapshot?.recent_events ?? []} />
          </Panel>
          <Panel title="Chaos Controls" subtitle="inject failures (active in Stage 7)">
            <div style={{ height: "100%", overflowY: "auto" }}><ChaosControls snapshot={snapshot} /></div>
          </Panel>
        </div>

        {snapshot && (snapshot.incidents.length > 0 || snapshot.autoscaler.recent_events.length > 0) && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
            {snapshot.incidents.length > 0 && (
              <Panel title="Active Incidents" subtitle={`${snapshot.incidents.length} open`}>
                <div style={{ overflowY: "auto", maxHeight: "120px" }}>
                  {snapshot.incidents.map((inc) => (
                    <div key={inc.id} style={{ display: "flex", gap: "8px", alignItems: "center", padding: "4px 0", fontSize: "12px", borderBottom: "1px solid #1f2937" }}>
                      <span style={{ padding: "1px 6px", borderRadius: "4px", fontSize: "10px", background: inc.severity === "high" ? "#7f1d1d" : "#713f12", color: inc.severity === "high" ? "#fca5a5" : "#fde68a" }}>{inc.severity.toUpperCase()}</span>
                      <span style={{ color: "#d1d5db" }}>{inc.service}</span>
                      <span style={{ color: "#9ca3af" }}>{inc.type}</span>
                      <span style={{ color: "#6b7280", marginLeft: "auto" }}>{inc.age_s}s ago</span>
                    </div>
                  ))}
                </div>
              </Panel>
            )}
            {snapshot.autoscaler.recent_events.length > 0 && (
              <Panel title="Autoscaler Events" subtitle={`↑${snapshot.autoscaler.total_scale_ups} · ↓${snapshot.autoscaler.total_scale_downs}`}>
                <div style={{ overflowY: "auto", maxHeight: "120px" }}>
                  {snapshot.autoscaler.recent_events.map((ev, i) => (
                    <div key={i} style={{ display: "flex", gap: "8px", fontSize: "12px", padding: "3px 0", borderBottom: "1px solid #1f2937" }}>
                      <span style={{ color: ev.direction === "UP" ? "#38bdf8" : "#94a3b8" }}>{ev.direction === "UP" ? "↑" : "↓"}</span>
                      <span style={{ color: "#d1d5db" }}>{ev.service}</span>
                      <span style={{ color: "#9ca3af" }}>{ev.old_replicas}→{ev.new_replicas}</span>
                      <span style={{ color: "#6b7280", fontSize: "11px" }}>{ev.reason.slice(0, 40)}</span>
                    </div>
                  ))}
                </div>
              </Panel>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function Panel({ title, subtitle, children }: { title: string; subtitle?: string; children: ReactNode }) {
  return (
    <div style={{ background: "#111827", border: "1px solid #1f2937", borderRadius: "8px", padding: "10px", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div style={{ marginBottom: "8px", flexShrink: 0 }}>
        <span style={{ color: "#d1d5db", fontWeight: 600, fontSize: "13px" }}>{title}</span>
        {subtitle && <span style={{ color: "#6b7280", fontSize: "11px", marginLeft: "8px" }}>{subtitle}</span>}
      </div>
      <div style={{ flex: 1, overflow: "hidden" }}>{children}</div>
    </div>
  );
}

function Stat({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div style={{ textAlign: "center" }}>
      <div style={{ color, fontWeight: 700 }}>{value}</div>
      <div style={{ color: "#4b5563", fontSize: "10px" }}>{label}</div>
    </div>
  );
}

function ConnBadge({ connected }: { connected: boolean }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px" }}>
      <div style={{ width: "8px", height: "8px", borderRadius: "50%", background: connected ? "#22c55e" : "#ef4444", boxShadow: connected ? "0 0 6px #22c55e" : "none" }} />
      <span style={{ color: connected ? "#86efac" : "#fca5a5" }}>{connected ? "LIVE" : "CONNECTING..."}</span>
    </div>
  );
}
