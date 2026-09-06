

import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import type { HistoryEntry, RetryStorm } from "../types/simulation";

interface Props {
  history:      HistoryEntry[];
  activeStorms: RetryStorm[];
}

const STORM_COLORS: Record<string, string> = {
  gateway:      "#38bdf8",
  auth:         "#a78bfa",
  payment:      "#f97316",
  inventory:    "#22d3ee",
  notification: "#86efac",
};

export function RetryStormChart({ history, activeStorms }: Props) {

  const data = history.map((entry, i) => {
    const row: Record<string, number | string> = {
      t: i - history.length + 1,
    };
    for (const [name, metrics] of Object.entries(entry.services)) {

      row[name] = Math.round(metrics.error_rate * 1000) / 10;
    }
    return row;
  });

  const serviceNames = history[0] ? Object.keys(history[0].services) : [];
  const hasStorms    = activeStorms.length > 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      {/* Storm badges */}
      {hasStorms && (
        <div style={{ display: "flex", gap: "8px", marginBottom: "8px", flexWrap: "wrap" }}>
          {activeStorms.map((storm) => (
            <span
              key={storm.service}
              style={{ padding: "2px 8px", borderRadius: "4px", fontSize: "11px", fontFamily: "monospace", background: "#7f1d1d", color: "#fca5a5", border: "1px solid #ef4444" }}
            >
              ⚡ {storm.service} {storm.retry_rate.toFixed(1)}/s · {storm.amplification.toFixed(1)}x amp
            </span>
          ))}
        </div>
      )}

      {history.length === 0 ? (
        <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: "#6b7280", fontSize: "13px" }}>
          Waiting for retry data...
        </div>
      ) : (
        <div style={{ flex: 1 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="t"
                stroke="#6b7280"
                tick={{ fill: "#9ca3af", fontSize: 10 }}
                tickFormatter={(v) => `${v}s`}
              />
              <YAxis
                stroke="#6b7280"
                tick={{ fill: "#9ca3af", fontSize: 10 }}
                tickFormatter={(v) => `${v}%`}
                width={40}
              />
              <Tooltip
                contentStyle={{ background: "#1f2937", border: "1px solid #374151", borderRadius: "6px" }}
                labelStyle={{ color: "#9ca3af" }}
                formatter={(v: unknown) => [`${Number(v).toFixed(1)}% err`, ""]}
                labelFormatter={(v) => `${v}s ago`}
              />
              <Legend wrapperStyle={{ fontSize: "11px" }} />
              {serviceNames.map((name) => (
                <Area
                  key={name}
                  type="monotone"
                  dataKey={name}
                  stroke={STORM_COLORS[name] ?? "#9ca3af"}
                  fill={STORM_COLORS[name] ? `${STORM_COLORS[name]}33` : "#9ca3af33"}
                  strokeWidth={1.5}
                  isAnimationActive={false}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
