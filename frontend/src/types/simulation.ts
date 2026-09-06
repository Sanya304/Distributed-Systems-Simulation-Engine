

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export interface ServiceSnapshot {
  name: string;
  replicas: number;
  default_replicas: number;
  is_alive: boolean;

  avg_latency: number;
  base_latency: number;

  error_rate: number;
  error_pct: number;
  total_errors: number;
  total_requests: number;
  active_requests: number;

  cpu_usage: number;
  cpu_pct: number;
  queue_depth: number;
  queue_saturation: number;
  max_queue_size: number;
  throughput_rps: number;

  circuit_state: CircuitState;
  cb_error_rate: number;
  cb_window_count: number;

  extra_latency_ms: number;
  retry_rate: number;
  is_storming: boolean;
  amplification: number;
}

export interface SystemMetrics {
  total_requests: number;
  total_errors: number;
  error_rate: number;
  error_pct: number;
}

export interface Incident {
  id: string;
  service: string;
  type: string;
  description: string;
  severity: "low" | "medium" | "high";
  started_at: number;
  age_s: number;
}

export interface RetryStorm {
  service: string;
  retry_rate: number;
  amplification: number;
  duration_s: number;
  peak_rate: number;
}

export interface ScalingEvent {
  service: string;
  direction: "UP" | "DOWN";
  old_replicas: number;
  new_replicas: number;
  reason: string;
  timestamp: number;
}

export interface AutoscalerStats {
  total_scale_ups: number;
  total_scale_downs: number;
  current_replicas: Record<string, number>;
  recent_events: ScalingEvent[];
}

export interface DependencyEdge {
  source: string;
  target: string;
}

export interface LogEntry {
  time: string;
  message: string;
  color: string;
}

export interface SimulationSnapshot {
  seq: number;
  timestamp: number;
  services: Record<string, ServiceSnapshot>;
  circuit_states: Record<string, CircuitState>;
  system: SystemMetrics;
  incidents: Incident[];
  retry_storms: RetryStorm[];
  autoscaler: AutoscalerStats;
  recent_events: LogEntry[];
  dependency_graph: DependencyEdge[];
}

export interface HistoryEntry {
  timestamp: number;
  services: Record<string, {
    avg_latency: number;
    error_rate: number;
    cpu_pct: number;
    queue_depth: number;
    throughput_rps: number;
  }>;
  system_error_rate: number;
}
