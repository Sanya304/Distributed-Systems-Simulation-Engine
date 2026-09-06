"""Traffic shape definitions."""

import math
import time
from typing import Generator


PatternTick = tuple[float, str]


def steady_pattern(
    rps:        float = 5.0,
    duration_s: float = float("inf"),
) -> Generator[PatternTick, None, None]:
    """Constant request rate."""
    start = time.monotonic()
    while time.monotonic() - start < duration_s:
        yield rps, f"STEADY {rps:.0f} req/s"


def ramp_pattern(
    start_rps:  float = 1.0,
    end_rps:    float = 30.0,
    duration_s: float = 60.0,
) -> Generator[PatternTick, None, None]:
    """Linearly increases request rate from start_rps to end_rps."""
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= duration_s:
            return  # ramp complete

        progress = elapsed / duration_s
        current_rps = start_rps + (end_rps - start_rps) * progress

        yield max(0.1, current_rps), f"RAMP {current_rps:.1f} req/s"


def burst_pattern(
    base_rps:   float = 5.0,
    burst_rps:  float = 50.0,
    burst_s:    float = 5.0,
    cooldown_s: float = 35.0,
) -> Generator[PatternTick, None, None]:
    """Alternates between baseline and burst traffic in cycles."""
    start = time.monotonic()
    cycle = burst_s + cooldown_s

    while True:
        phase = (time.monotonic() - start) % cycle

        if phase < cooldown_s:
            yield base_rps, f"STEADY {base_rps:.0f} req/s"
        else:
            yield burst_rps, f"BURST {burst_rps:.0f} req/s ⚡"


def spike_pattern(
    base_rps:   float = 5.0,
    peak_rps:   float = 80.0,
    spike_s:    float = 3.0,
    recover_s:  float = 57.0,
) -> Generator[PatternTick, None, None]:
    """Instant jump to peak, hold briefly, then drop back to base."""
    start = time.monotonic()
    cycle = spike_s + recover_s

    while True:
        phase = (time.monotonic() - start) % cycle

        if phase < spike_s:
            yield peak_rps, f"SPIKE {peak_rps:.0f} req/s ⚡⚡"
        else:
            yield base_rps, f"RECOVER {base_rps:.0f} req/s"


def sine_pattern(
    min_rps:   float = 2.0,
    max_rps:   float = 20.0,
    period_s:  float = 60.0,
) -> Generator[PatternTick, None, None]:
    """Sinusoidal traffic wave."""
    start = time.monotonic()
    amplitude = (max_rps - min_rps) / 2
    midpoint = min_rps + amplitude

    while True:
        elapsed = time.monotonic() - start
        current_rps = midpoint + amplitude * math.sin(
            2 * math.pi * elapsed / period_s
        )
        yield max(0.1, current_rps), f"SINE {current_rps:.1f} req/s"


def demo_pattern() -> Generator[PatternTick, None, None]:
    """Scripted sequence demonstrating all behaviors."""
    start = time.monotonic()

    while True:
        elapsed = time.monotonic() - start

        if elapsed < 20:
            yield 5.0,  "STEADY 5 req/s"
        elif elapsed < 25:
            yield 20.0, "BURST  20 req/s"
        elif elapsed < 40:
            yield 5.0,  "STEADY 5 req/s"
        elif elapsed < 45:
            yield 40.0, "SPIKE  40 req/s"
        elif elapsed < 60:
            yield 3.0,  "STEADY 3 req/s"
        elif elapsed < 90:
            progress = (elapsed - 60) / 30
            rps = 3.0 + (25.0 - 3.0) * progress
            yield rps, f"RAMP   {rps:.1f} req/s"
        else:
            phase = (elapsed - 90) % 40
            if phase < 30:
                yield 5.0,  "STEADY 5 req/s"
            else:
                yield 25.0, "BURST  25 req/s"
