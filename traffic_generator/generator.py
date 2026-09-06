"""Async traffic generator."""

import asyncio
import time
import random
import logging
from typing import AsyncGenerator

import httpx

from shared.models import Event, EventType
from shared.event_bus import EventBus
from shared.state_store import StateStore
from traffic_generator.patterns import demo_pattern, PatternTick


logger = logging.getLogger("traffic_generator")


class TrafficGenerator:

    def __init__(
        self,
        gateway_url:    str   = "http://localhost:8000",
        use_http:       bool  = False,
        bus:            EventBus  = None,
        store:          StateStore = None,
        event_log:      list  = None,
    ):
        self.gateway_url = gateway_url
        self.use_http    = use_http
        self.bus         = bus
        self.store       = store
        self.event_log   = event_log or []

        self.total_sent     = 0
        self.total_accepted = 0
        self.total_rejected = 0
        self.total_errors   = 0

        self.current_rps    = 0.0
        self.current_mode   = "idle"
        self.start_time     = time.time()
        self._running       = False

    async def run(self, pattern_gen=None) -> None:

        if pattern_gen is None:
            pattern_gen = demo_pattern()

        self._running = True
        self.start_time = time.time()

        logger.info(
            f"Traffic generator starting | mode={'http' if self.use_http else 'direct'} "
            f"| target={self.gateway_url if self.use_http else 'EventBus'}"
        )

        async with httpx.AsyncClient(
            base_url = self.gateway_url,
            timeout  = httpx.Timeout(2.0),
        ) as client:

            for rps, mode in pattern_gen:
                if not self._running:
                    break

                self.current_rps  = rps
                self.current_mode = mode

                sleep_s = 1.0 / max(rps, 0.1)

                await self._send_one(client)
                await asyncio.sleep(sleep_s)

        self._running = False
        logger.info("Traffic generator stopped.")

    async def _send_one(self, client: httpx.AsyncClient) -> None:
        """Send a single request via HTTP or EventBus."""
        self.total_sent += 1

        if self.use_http:
            await self._send_http(client)
        else:
            await self._send_direct()

    async def _send_http(self, client: httpx.AsyncClient) -> None:

        try:
            payload = {
                "user_id":    f"user_{random.randint(1000, 9999)}",
                "action":     random.choice(["checkout", "view", "add_to_cart"]),
                "amount":     round(random.uniform(9.99, 299.99), 2),
                "session_id": f"sess_{random.randint(100000, 999999)}",
            }

            response = await client.post(
                "/request",
                json    = payload,
                headers = {
                    "X-Region":   random.choice(["us-east", "us-west", "eu-west"]),
                    "User-Agent": "SimulationTrafficGenerator/1.0",
                },
            )

            if response.status_code == 202:
                self.total_accepted += 1
                data = response.json()
                self._log(
                    f"HTTP 202  trace={data.get('trace_id', '')[:8]}  "
                    f"queue={data.get('queue_depth', 0)}",
                    "green"
                )

            elif response.status_code == 429:
                self.total_rejected += 1
                self._log(f"HTTP 429  rate limited by gateway", "yellow")

            elif response.status_code == 503:
                self.total_rejected += 1
                data = response.json()
                self._log(
                    f"HTTP 503  {data.get('detail', {}).get('service', 'unknown')} queue full",
                    "red"
                )

            else:
                self.total_errors += 1
                self._log(f"HTTP {response.status_code}  unexpected response", "red")

        except httpx.ConnectError:
            self.total_errors += 1
            self._log("HTTP ERR  cannot connect to gateway (is it running?)", "red")

        except httpx.TimeoutException:
            self.total_errors += 1
            self._log("HTTP TIMEOUT  gateway took > 2s to respond", "yellow")

        except Exception as exc:
            self.total_errors += 1
            self._log(f"HTTP ERR  {exc}", "red")

    async def _send_direct(self) -> None:
        """Inject event directly into the EventBus."""
        if self.bus is None:
            return

        event = Event(
            source_service = "user",
            target_service = "gateway",
            event_type     = EventType.REQUEST,
            region         = random.choice(["us-east", "us-west", "eu-west"]),
            payload_size   = random.randint(128, 4096),
            metadata       = {
                "user_id": f"user_{random.randint(1000, 9999)}",
                "action":  random.choice(["checkout", "view", "add_to_cart"]),
            },
        )

        accepted = await self.bus.publish("gateway", event)
        if accepted:
            self.total_accepted += 1
        else:
            self.total_rejected += 1
            self._log(
                f"DROPPED  gateway queue full  mode={self.current_mode}",
                "red"
            )

    def stop(self) -> None:
        self._running = False

    def stats(self) -> dict:
        elapsed = max(time.time() - self.start_time, 0.001)
        return {
            "current_rps":    round(self.current_rps, 1),
            "current_mode":   self.current_mode,
            "total_sent":     self.total_sent,
            "total_accepted": self.total_accepted,
            "total_rejected": self.total_rejected,
            "total_errors":   self.total_errors,
            "acceptance_rate": round(
                self.total_accepted / max(self.total_sent, 1), 3
            ),
            "effective_rps":  round(self.total_accepted / elapsed, 2),
            "elapsed_s":      round(elapsed, 1),
        }

    def _log(self, message: str, color: str = "white") -> None:
        self.event_log.append({
            "time":    time.strftime("%H:%M:%S"),
            "message": f"TGEN  {message}",
            "color":   color,
        })
        if len(self.event_log) > 200:
            self.event_log.pop(0)
