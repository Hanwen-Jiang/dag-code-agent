import type { DagManifest, DagReport, DagRunResponse, HarnessDemoResponse, HarnessRunRequest, HarnessSessionsResponse, ModelProviderStatus } from "./types";

const API_BASE = import.meta.env.VITE_API_BASE ?? "";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(String(body.error ?? `Request failed (${response.status})`));
  }
  return response.json() as Promise<T>;
}

export const api = {
  health: () => request<{ status: string; service: string }>("/health"),
  modelStatus: () => request<ModelProviderStatus>("/api/model/status"),
  harnessDemo: () => request<HarnessDemoResponse>("/api/harness/demo"),
  harnessSessions: () => request<HarnessSessionsResponse>("/api/harness/runs"),
  createHarnessRun: (input: HarnessRunRequest) => request<HarnessDemoResponse>("/api/harness/runs", { method: "POST", body: JSON.stringify(input) }),
  harnessRun: (taskId: string) => request<HarnessDemoResponse>(`/api/harness/runs/${encodeURIComponent(taskId)}`),
  harnessEvents: (taskId: string) => request<{ task_id: string; events: HarnessDemoResponse["events"]; count: number }>(`/api/harness/runs/${encodeURIComponent(taskId)}/events`),
  harnessEventsURL: (taskId: string, since = 0) => `${API_BASE}/api/harness/runs/${encodeURIComponent(taskId)}/events?since=${since}&follow=1`,
  resumeHarnessRun: (taskId: string, message: string) => request<HarnessDemoResponse>(`/api/harness/runs/${encodeURIComponent(taskId)}/messages`, { method: "POST", body: JSON.stringify({ message }) }),
  cancelHarnessRun: (taskId: string) => request<HarnessDemoResponse>(`/api/harness/runs/${encodeURIComponent(taskId)}/cancel`, { method: "POST", body: "{}" }),
  dagManifest: () => request<DagManifest>("/api/dag/manifest"),
  startDag: (scenario: "happy" | "rework" | "crash") =>
    request<DagRunResponse>("/api/dag/start", {
      method: "POST",
      body: JSON.stringify({ scenario }),
    }),
  getDagRun: (runId: string) =>
    request<DagReport>("/api/dag/runs/" + encodeURIComponent(runId)),
  decideDag: (runId: string, outcome: "PASS" | "REJECT", actor: string) =>
    request<DagReport>(
      "/api/dag/runs/" + encodeURIComponent(runId) + "/decision",
      {
        method: "POST",
        body: JSON.stringify({ outcome, actor }),
      },
    ),
};
