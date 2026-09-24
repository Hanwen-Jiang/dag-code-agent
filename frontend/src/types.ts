export type HarnessEvent = {
  task_id: string;
  sequence: number;
  event_type: string;
  payload: Record<string, unknown>;
  previous_hash: string;
  event_hash: string;
  created_at: string;
  task_version: number;
};

export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = Record<string, unknown>;

export type HarnessMetrics = {
  event_count: number;
  model_rounds: number;
  steps: number;
  tool_calls: number;
  [key: string]: number;
};

export type HarnessToolSnapshot = {
  name: string;
  description: string;
  input_schema: JsonObject;
  required_scopes?: string[];
  side_effect?: string;
  idempotency_required?: boolean;
  timeout_seconds?: number;
  max_retries?: number;
  tags?: string[];
};

export type HarnessSnapshot = {
  fixture: { name: string; mode: string; provider_boundary: string };
  request: JsonObject;
  auth: JsonObject;
  model: JsonObject & { provider_status?: ModelProviderStatus };
  context: JsonObject;
  tools: HarnessToolSnapshot[];
  mcp: JsonObject & { tools?: JsonObject[] };
  skills: JsonObject[];
  memory: JsonObject[];
  cache: JsonObject;
  durability: JsonObject;
  contract: {
    agent_loop: string[];
    policy: string[];
    long_horizon: string[];
    evaluation: string[];
  };
};

export type ModelProviderStatus = {
  provider: string;
  mode: "offline" | "remote" | string;
  model: string;
  base_url?: string;
  configured: boolean;
  key_configured: boolean;
  detail: string;
};

export type HarnessDemoResponse = {
  status: string;
  task_id: string;
  final_answer: string;
  failure_category?: string;
  state?: JsonObject;
  metrics: HarnessMetrics;
  events: HarnessEvent[];
  snapshot: HarnessSnapshot;
};

export type HarnessRunRequest = {
  task_id?: string;
  goal: string;
  project_id?: string;
  project_name?: string;
  tenant_id?: string;
  subject_id?: string;
  model?: string;
  model_revision?: string;
  reasoning_effort?: string;
  policy_version?: string;
  context_window_tokens?: number;
  max_steps?: number;
  max_tool_calls?: number;
  constraints?: string[];
  metadata?: JsonObject;
};

export type HarnessSessionSummary = {
  task_id: string;
  goal: string;
  project_id?: string;
  project_name?: string;
  status: string;
  step: number;
  model_round: number;
  tool_calls: number;
  retry_count: number;
  last_error?: JsonObject;
};

export type HarnessSessionsResponse = {
  sessions: HarnessSessionSummary[];
  count: number;
};

export type DagContract = {
  contract_id: string;
  artifact_kind: string;
  media_type?: string;
  required: boolean;
  delivery_mode?: string;
  max_tokens?: number;
  trust_class?: string;
};

export type DagNode = {
  node_id: string;
  title: string;
  objective: string;
  work_type: string;
  executor_role: string;
  input_contracts: DagContract[];
  output_contracts: DagContract[];
  activation: Record<string, unknown>;
  gate_policy: Record<string, unknown>;
  execution_policy: Record<string, unknown>;
  tool_profile?: unknown;
  write_scope?: Record<string, unknown>;
};

export type DagEdge = {
  edge_id: string;
  from_node: string;
  to_node: string;
  edge_type: string;
  gating: boolean;
  condition: {
    predicate?: {
      source: string;
      subject_id: string;
      operator: string;
      value: string;
    };
  };
  artifact_contract_ids: string[];
  description?: string;
};

export type DagManifest = {
  schema_version: string;
  manifest_id: string;
  spec_hash: string;
  plan: {
    plan_id?: string;
    revision?: number;
    title?: string;
    objective?: string;
    status?: string;
    requirements?: Array<Record<string, unknown>>;
    constraints?: string[];
    flags?: Record<string, unknown>;
    hub_policy?: Record<string, unknown>;
    concurrency_policy?: Record<string, unknown>;
  };
  nodes: DagNode[];
  edges: DagEdge[];
};

export type DagAttempt = {
  attempt_id: string;
  node_id: string;
  agent_instance_id: string;
  ordinal: number;
  state: string;
  error_code?: string;
  started_at: string;
  ended_at?: string;
};

export type DagArtifact = {
  artifact_id: string;
  contract_id: string;
  producer_node_id: string;
  producer_attempt_id: string;
  sha256: string;
  media_type: string;
  content?: Record<string, unknown>;
};

export type DagEvidence = {
  evidence_id: string;
  criterion_id?: string;
  producer_node_id: string;
  producer_attempt_id: string;
  artifact_id?: string;
  verdict: string;
  method: string;
  observed?: string;
};

export type DagReport = {
  run_id: string;
  task_id: string;
  status: string;
  node_states: Record<string, string>;
  attempts: DagAttempt[];
  artifacts: DagArtifact[];
  evidence: DagEvidence[];
  completed_order: string[];
  waiting_node_id?: string;
  failure?: Record<string, unknown>;
  events: HarnessEvent[];
};

export type DagRunResponse = {
  scenario: "happy" | "rework" | "crash";
  recovered_from_crash: boolean;
  report: DagReport;
};
