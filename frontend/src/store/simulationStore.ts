

import { create } from "zustand";
import type {
  SimulationSnapshot,
  HistoryEntry,
  ServiceSnapshot,
} from "../types/simulation";

const MAX_HISTORY = 60; // 60 seconds of 1-second snapshots

interface SimulationState {

  snapshot: SimulationSnapshot | null;


  history: HistoryEntry[];


  connected: boolean;
  lastSeq: number;
  missedUpdates: number;


  applySnapshot: (snapshot: SimulationSnapshot) => void;
  applyHistory:  (entries: HistoryEntry[]) => void;
  setConnected:  (connected: boolean) => void;
}

export const useSimulationStore = create<SimulationState>((set, get) => ({
  snapshot:     null,
  history:      [],
  connected:    false,
  lastSeq:      0,
  missedUpdates: 0,

  applySnapshot: (snapshot) => {
    const state = get();

    // Detect missed updates (seq jumped by more than 1)
    const missed = snapshot.seq > 1
      ? Math.max(0, snapshot.seq - state.lastSeq - 1)
      : 0;

    // Build a history entry from the snapshot
    const entry: HistoryEntry = {
      timestamp: snapshot.timestamp,
      services:  Object.fromEntries(
        Object.entries(snapshot.services).map(([name, svc]) => [
          name,
          {
            avg_latency:    svc.avg_latency,
            error_rate:     svc.error_rate,
            cpu_pct:        svc.cpu_pct,
            queue_depth:    svc.queue_depth,
            throughput_rps: svc.throughput_rps,
          },
        ])
      ),
      system_error_rate: snapshot.system.error_rate,
    };

    set((s) => ({
      snapshot,
      lastSeq:      snapshot.seq,
      missedUpdates: s.missedUpdates + missed,
      history: [...s.history.slice(-(MAX_HISTORY - 1)), entry],
    }));
  },

  applyHistory: (entries) => {
    // Called once on connect with pre-filled 60-second history from server
    set({
      history: entries.slice(-MAX_HISTORY),
    });
  },

  setConnected: (connected) => set({ connected }),
}));


export const selectService = (name: string) =>
  (state: SimulationState): ServiceSnapshot | null =>
    state.snapshot?.services[name] ?? null;


export const selectServicesDict = (state: SimulationState) =>
  state.snapshot?.services ?? EMPTY_SERVICES;

export const selectHistory = (state: SimulationState) => state.history;

export const selectIncidents = (state: SimulationState) =>
  state.snapshot?.incidents ?? EMPTY_INCIDENTS;

export const selectStorms = (state: SimulationState) =>
  state.snapshot?.retry_storms ?? EMPTY_STORMS;

export const selectSystem = (state: SimulationState) =>
  state.snapshot?.system ?? null;

const EMPTY_SERVICES:  Record<string, ServiceSnapshot> = {};
const EMPTY_INCIDENTS: SimulationSnapshot["incidents"]    = [];
const EMPTY_STORMS:    SimulationSnapshot["retry_storms"] = [];
