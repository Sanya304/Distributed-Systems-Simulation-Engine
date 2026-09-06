"""Core data models for the simulation engine."""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time
import uuid


# Enums

class CircuitState(str, Enum):

    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class EventType(str, Enum):

    REQUEST  = "request"
    RESPONSE = "response"
    ERROR    = "error"
    RETRY    = "retry"
    TIMEOUT  = "timeout"


class TrafficPattern(str, Enum):

    STEADY = "steady"
    BURST  = "burst"
    SPIKE  = "spike"
    RAMP   = "ramp"


# Event

@dataclass
class Event:

    source_service: str
    target_service: str
    event_type: EventType

    trace_id:     str   = field(default_factory=lambda: str(uuid.uuid4()))
    request_id:   str   = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp:    float = field(default_factory=time.time)

    region:       str   = "us-east"
    retry_count:  int   = 0
    payload_size: int   = 256
    metadata:     dict  = field(default_factory=dict)

    def to_dict(self) -> dict:

        return {
            "trace_id":       self.trace_id,
            "request_id":     self.request_id,
            "source_service": self.source_service,
            "target_service": self.target_service,
            "event_type":     self.event_type.value,
            "timestamp":      self.timestamp,
            "region":         self.region,
            "retry_count":    self.retry_count,
            "payload_size":   self.payload_size,
            "metadata":       self.metadata,
        }


@dataclass
class ServiceConfig:

    name:                str
    base_latency_ms:     float
    failure_probability: float
    max_queue_size:      int
    timeout_ms:          float
    dependencies:        list[str]
    replicas:            int   = 1
    cpu_threshold:       float = 0.8




@dataclass
class ServiceState:

    name:             str
    active_requests:  int          = 0
    queue_depth:      int          = 0
    avg_latency:      float        = 0.0
    error_rate:       float        = 0.0
    cpu_usage:        float        = 0.0
    circuit_state:    CircuitState = CircuitState.CLOSED
    replicas:         int          = 1
    total_requests:   int          = 0
    total_errors:     int          = 0
    is_alive:         bool         = True
    extra_latency_ms: float        = 0.0




@dataclass
class SimulationSnapshot:

    timestamp:        float
    services:         dict[str, dict]
    circuit_states:   dict[str, str]
    active_incidents: list[dict]
    retry_storms:     list[dict]
    total_throughput: float = 0.0
    total_error_rate: float = 0.0
