

import type { SimulationSnapshot } from "../types/simulation";

interface Props {
  snapshot: SimulationSnapshot | null;
}

const CIRCUIT_COLOR: Record<string, string> = {
  CLOSED:    "#22c55e",
  OPEN:      "#ef4444",
  HALF_OPEN: "#eab308",
};


const POSITIONS: Record<string, [number, number]> = {
  gateway:      [200, 50],
  auth:         [80,  160],
  payment:      [320, 160],
  inventory:    [230, 270],
  notification: [410, 270],
};


const EDGES: [string, string][] = [
  ["gateway", "auth"],
  ["gateway", "payment"],
  ["payment", "inventory"],
  ["payment", "notification"],
];

export function ServiceGraph({ snapshot }: Props) {
  if (!snapshot) {
    return (
      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#4b5563", fontSize: "13px" }}>
        Waiting for data...
      </div>
    );
  }

  const services = snapshot.services;

  return (
    <svg
      viewBox="0 0 480 310"
      style={{ width: "100%", height: "100%", background: "#111827", borderRadius: "6px" }}
    >
      {/* Arrow marker */}
      <defs>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L8,3 z" fill="#4b5563" />
        </marker>
      </defs>

      {/* Edges */}
      {EDGES.map(([src, tgt]) => {
        const [x1, y1] = POSITIONS[src] ?? [0, 0];
        const [x2, y2] = POSITIONS[tgt] ?? [0, 0];

        // Shorten line so arrowhead doesn't overlap node circle
        const dx  = x2 - x1;
        const dy  = y2 - y1;
        const len = Math.sqrt(dx * dx + dy * dy);
        const R   = 30; // node radius
        const ex  = x2 - (dx / len) * R;
        const ey  = y2 - (dy / len) * R;
        const sx  = x1 + (dx / len) * R;
        const sy  = y1 + (dy / len) * R;

        return (
          <line
            key={`${src}-${tgt}`}
            x1={sx} y1={sy} x2={ex} y2={ey}
            stroke="#4b5563"
            strokeWidth={1.5}
            strokeDasharray="5,3"
            markerEnd="url(#arrow)"
          />
        );
      })}

      {/* Nodes */}
      {Object.entries(POSITIONS).map(([name, [cx, cy]]) => {
        const svc      = services[name];
        const circuit  = svc?.circuit_state ?? "CLOSED";
        const color    = CIRCUIT_COLOR[circuit] ?? "#22c55e";
        const alive    = svc?.is_alive ?? true;
        const storming = svc?.is_storming ?? false;
        const cpuPct   = svc?.cpu_pct ?? 0;
        const errPct   = svc?.error_pct ?? 0;
        const latency  = svc?.avg_latency ?? 0;
        const replicas = svc?.replicas ?? 1;
        const defRep   = svc?.default_replicas ?? replicas;


        const showGlow = circuit === "OPEN";

        return (
          <g key={name} opacity={alive ? 1 : 0.4}>
            {/* Glow ring */}
            {showGlow && (
              <circle cx={cx} cy={cy} r={36} fill="rgba(239,68,68,0.15)" />
            )}

            {/* Node circle */}
            <circle
              cx={cx} cy={cy} r={30}
              fill={color}
              stroke="#1f2937"
              strokeWidth={2}
            />

            {/* Service name */}
            <text
              x={cx} y={cy - 4}
              textAnchor="middle"
              fill="#f9fafb"
              fontSize={10}
              fontWeight="700"
            >
              {name}
            </text>

            {/* Latency */}
            <text
              x={cx} y={cy + 9}
              textAnchor="middle"
              fill="#1f2937"
              fontSize={9}
            >
              {latency.toFixed(0)}ms
            </text>

            {/* Error % below node */}
            <text
              x={cx} y={cy + 44}
              textAnchor="middle"
              fill={errPct > 10 ? "#fca5a5" : "#6b7280"}
              fontSize={9}
            >
              {errPct.toFixed(1)}% err
            </text>

            {/* Replica badge (only if scaled) */}
            {replicas !== defRep && (
              <text
                x={cx + 26} y={cy - 22}
                textAnchor="middle"
                fill={replicas > defRep ? "#38bdf8" : "#94a3b8"}
                fontSize={9}
                fontWeight="700"
              >
                {replicas}x
              </text>
            )}

            {/* Storm indicator */}
            {storming && (
              <text x={cx} y={cy - 38} textAnchor="middle" fontSize={14}>
                ⚡
              </text>
            )}

            {/* CPU arc overlay — thin arc showing cpu% */}
            {cpuPct > 5 && (
              <circle
                cx={cx} cy={cy} r={30}
                fill="none"
                stroke={cpuPct > 80 ? "#ef4444" : cpuPct > 60 ? "#eab308" : "#38bdf8"}
                strokeWidth={3}
                strokeDasharray={`${(cpuPct / 100) * 188.5} 188.5`}
                strokeLinecap="round"
                transform={`rotate(-90 ${cx} ${cy})`}
                opacity={0.6}
              />
            )}
          </g>
        );
      })}

      {/* Legend */}
      <g transform="translate(8, 8)">
        {[["CLOSED", "#22c55e"], ["OPEN", "#ef4444"], ["HALF_OPEN", "#eab308"]].map(([label, color], i) => (
          <g key={label} transform={`translate(0, ${i * 16})`}>
            <circle cx={6} cy={6} r={5} fill={color} />
            <text x={14} y={10} fill="#6b7280" fontSize={9}>{label}</text>
          </g>
        ))}
      </g>
    </svg>
  );
}
