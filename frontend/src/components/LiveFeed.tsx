

import { useEffect, useRef } from "react";
import type { LogEntry } from "../types/simulation";

interface Props {
  events: LogEntry[];
}

const COLOR_MAP: Record<string, string> = {
  green:   "#86efac",
  red:     "#fca5a5",
  yellow:  "#fde68a",
  magenta: "#e879f9",
  cyan:    "#67e8f9",
  white:   "#f9fafb",
  grey:    "#6b7280",
};

export function LiveFeed({ events }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [events.length]);

  if (events.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-500 text-sm font-mono">
        Waiting for events...
      </div>
    );
  }

  return (
    <div className="h-full overflow-y-auto font-mono text-xs space-y-0.5 p-1">
      {events.map((entry, i) => (
        <div key={i} className="flex gap-2 hover:bg-gray-800 px-1 py-0.5 rounded">
          <span className="text-gray-500 flex-shrink-0">{entry.time}</span>
          <span style={{ color: COLOR_MAP[entry.color] ?? "#f9fafb" }}>
            {entry.message}
          </span>
        </div>
      ))}
      <div ref={bottomRef} />
    </div>
  );
}
