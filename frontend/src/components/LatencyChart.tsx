

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import type { HistoryEntry } from "../types/simulation";

interface Props {
  history: HistoryEntry[];
}

const SERVICE_COLORS: Record<string, string> = {
  gateway:      "#38bdf8",  // sky
  auth:         "#a78bfa",  // violet
  payment:      "#f97316",  // orange
  inventory:    "#22d3ee",  // cyan
  notification: "#86efac",  // green
};

export function LatencyChart({ history }: Props) {
  if (history.length === 0) {
    return <EmptyState label="Waiting for latency data..." />;
  }


  const data = history.map((entry, i) => {
    const row: Record<string, number | string> = {
      t: i - history.length + 1,  // negative = seconds ago
    };
    for (const [name, metrics] of Object.entries(entry.services)) {
      row[name] = Math.round(metrics.avg_latency * 10) / 10;
    }
    return row;
  });

  const serviceNames = history[0] ? Object.keys(history[0].services) : [];

  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
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
          tickFormatter={(v) => `${v}ms`}
          width={50}
        />
        <Tooltip
          contentStyle={{ background: "#1f2937", border: "1px solid #374151", borderRadius: "6px" }}
          labelStyle={{ color: "#9ca3af" }}
          formatter={(v: unknown) => [`${Number(v).toFixed(1)}ms`, ""]}
          labelFormatter={(v) => `${v}s ago`}
        />
        <Legend wrapperStyle={{ fontSize: "11px" }} />
        {serviceNames.map((name) => (
          <Line
            key={name}
            type="monotone"
            dataKey={name}
            stroke={SERVICE_COLORS[name] ?? "#9ca3af"}
            dot={false}
            strokeWidth={1.5}
            isAnimationActive={false}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

function EmptyState({ label }: { label: string }) {
  return (
    <div className="flex items-center justify-center h-full text-gray-500 text-sm">
      {label}
    </div>
  );
}
