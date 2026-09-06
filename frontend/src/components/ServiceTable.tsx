

import type { ServiceSnapshot } from "../types/simulation";

interface Props {
  services: ServiceSnapshot[];
}

const CIRCUIT_BADGE: Record<string, { bg: string; text: string }> = {
  CLOSED:    { bg: "#14532d", text: "#86efac" },
  OPEN:      { bg: "#7f1d1d", text: "#fca5a5" },
  HALF_OPEN: { bg: "#713f12", text: "#fde68a" },
};

export function ServiceTable({ services }: Props) {
  if (services.length === 0) {
    return <div className="text-gray-500 text-sm p-4">No services connected.</div>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm font-mono">
        <thead>
          <tr className="text-left text-gray-500 border-b border-gray-700">
            <th className="pb-2 pr-4">Service</th>
            <th className="pb-2 pr-4">Circuit</th>
            <th className="pb-2 pr-4">Queue</th>
            <th className="pb-2 pr-4">Latency</th>
            <th className="pb-2 pr-4">Error%</th>
            <th className="pb-2 pr-4">CPU%</th>
            <th className="pb-2 pr-4">Rep</th>
            <th className="pb-2">RPS</th>
          </tr>
        </thead>
        <tbody>
          {services.map((svc) => {
            const badge    = CIRCUIT_BADGE[svc.circuit_state];
            const latColor = svc.avg_latency < 100  ? "#86efac"
                           : svc.avg_latency < 400  ? "#fde68a"
                           :                          "#fca5a5";
            const errColor = svc.error_pct < 5   ? "#86efac"
                           : svc.error_pct < 20  ? "#fde68a"
                           :                       "#fca5a5";
            const cpuColor = svc.cpu_pct < 70    ? "#86efac"
                           : svc.cpu_pct < 90    ? "#fde68a"
                           :                       "#fca5a5";
            const queuePct = svc.queue_saturation * 100;
            const queueColor = queuePct < 50   ? "#86efac"
                             : queuePct < 80   ? "#fde68a"
                             :                   "#fca5a5";
            const repColor = svc.replicas > svc.default_replicas ? "#38bdf8"
                           : svc.replicas < svc.default_replicas ? "#94a3b8"
                           :                                        "#d1d5db";

            return (
              <tr
                key={svc.name}
                className="border-b border-gray-800 hover:bg-gray-800 transition-colors"
                style={{ opacity: svc.is_alive ? 1 : 0.4 }}
              >
                {/* Service name */}
                <td className="py-2 pr-4">
                  <span className="text-gray-100 font-semibold">{svc.name}</span>
                  {svc.is_storming && <span className="ml-1 text-yellow-400">⚡</span>}
                  {!svc.is_alive  && <span className="ml-1 text-red-400">☠</span>}
                </td>

                {/* Circuit state badge */}
                <td className="py-2 pr-4">
                  <span
                    className="px-1.5 py-0.5 rounded text-xs font-semibold"
                    style={{ background: badge.bg, color: badge.text }}
                  >
                    {svc.circuit_state}
                  </span>
                </td>

                {/* Queue bar */}
                <td className="py-2 pr-4">
                  <div className="flex items-center gap-1.5">
                    <div className="w-16 h-2 bg-gray-700 rounded-full overflow-hidden">
                      <div
                        className="h-full rounded-full transition-all"
                        style={{
                          width:      `${Math.min(queuePct, 100)}%`,
                          background: queueColor,
                        }}
                      />
                    </div>
                    <span className="text-gray-400 text-xs">{svc.queue_depth}</span>
                  </div>
                </td>

                {/* Latency */}
                <td className="py-2 pr-4" style={{ color: latColor }}>
                  {svc.avg_latency.toFixed(1)}ms
                </td>

                {/* Error % */}
                <td className="py-2 pr-4" style={{ color: errColor }}>
                  {svc.error_pct.toFixed(1)}%
                </td>

                {/* CPU % */}
                <td className="py-2 pr-4" style={{ color: cpuColor }}>
                  {svc.cpu_pct.toFixed(1)}%
                </td>

                {/* Replicas */}
                <td className="py-2 pr-4" style={{ color: repColor }}>
                  {svc.replicas}x
                </td>

                {/* Throughput */}
                <td className="py-2 text-gray-400">
                  {svc.throughput_rps.toFixed(1)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
