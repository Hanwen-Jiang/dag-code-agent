import { useMemo, useState } from "react";
import type { ComponentType, ReactNode } from "react";
import {
  Activity,
  ArrowUpRight,
  BrainCircuit,
  CheckCircle2,
  CircleDot,
  Clock3,
  Copy,
  Database,
  FileCheck2,
  GitCommitHorizontal,
  Hash,
  Layers3,
  LockKeyhole,
  Play,
  Plus,
  RefreshCw,
  Route,
  ShieldCheck,
  Sparkles,
  TerminalSquare,
  UserRound,
  Wrench,
} from "lucide-react";
import { Button, Chip } from "@heroui/react";
import { KPI } from "@heroui-pro/react/kpi";
import { KPIGroup } from "@heroui-pro/react/kpi-group";
import { Segment } from "@heroui-pro/react/segment";

import type { HarnessDemoResponse, HarnessEvent, HarnessRunRequest, HarnessSessionSummary, JsonObject } from "../types";
import {
  InlineError,
  JsonBlock,
  LoadingState,
  PageHeader,
  Panel,
  StatusChip,
  formatDate,
  shortHash,
} from "../components/Primitives";

export type HarnessViewKey = "chat" | "settings" | "workbench" | "trajectory" | "context" | "tools" | "skills" | "subagents" | "dag" | "evaluation";

type HarnessWorkbenchProps = {
  data: HarnessDemoResponse | null;
  loading: boolean;
  error: string;
  sessions: HarnessSessionSummary[];
  activeTaskId: string | null;
  onCreateRun: (input: HarnessRunRequest) => Promise<void>;
  onSelectSession: (taskId: string) => Promise<void>;
  onRun: () => void;
  onNavigate: (view: HarnessViewKey) => void;
};

const eventMeta: Record<string, { label: string; tone: "success" | "accent" | "warning" | "default"; icon: ComponentType<{ className?: string }> }> = {
  TASK_ACCEPTED: { label: "Task accepted", tone: "accent", icon: GitCommitHorizontal },
  PLAN_COMPILED: { label: "Plan compiled", tone: "accent", icon: Route },
  TURN_STARTED: { label: "Turn opened", tone: "accent", icon: Activity },
  STEP_STARTED: { label: "Step started", tone: "accent", icon: CircleDot },
  CONTEXT_COMPILED: { label: "Context compiled", tone: "accent", icon: Layers3 },
  MODEL_REQUESTED: { label: "Model request logged", tone: "accent", icon: BrainCircuit },
  MODEL_PROPOSED: { label: "Model proposal received", tone: "warning", icon: BrainCircuit },
  TOOL_INVOKED: { label: "Tool invocation admitted", tone: "warning", icon: Wrench },
  TOOL_OBSERVED: { label: "Tool observation committed", tone: "success", icon: CheckCircle2 },
  TOOLS_EXECUTED: { label: "Tool result committed", tone: "success", icon: Wrench },
  SUBAGENTS_REQUESTED: { label: "Subagent work requested", tone: "accent", icon: BrainCircuit },
  VERIFIER_REJECTED: { label: "Verifier rejected proposal", tone: "warning", icon: ShieldCheck },
  USER_GATE_OPENED: { label: "Human gate opened", tone: "warning", icon: LockKeyhole },
  USER_GATE_RESUMED: { label: "Human gate resumed", tone: "accent", icon: Play },
  USER_MESSAGE_RECEIVED: { label: "User message received", tone: "accent", icon: UserRound },
  STEP_COMPLETED: { label: "Step closed", tone: "default", icon: CheckCircle2 },
  TURN_COMPLETED: { label: "Turn closed", tone: "default", icon: GitCommitHorizontal },
  TASK_COMPLETED: { label: "Task completed", tone: "success", icon: CheckCircle2 },
  TASK_FAILED: { label: "Task failed", tone: "warning", icon: CircleDot },
  TASK_CANCELLED: { label: "Task cancelled", tone: "warning", icon: CircleDot },
};

export function HarnessWorkbenchView({ data, loading, error, sessions, activeTaskId, onCreateRun, onSelectSession, onRun, onNavigate }: HarnessWorkbenchProps) {
  if (loading && !data) return <LoadingState label="正在读取 Harness trajectory" />;

  if (!data) {
    return (
      <div className="space-y-6">
        <PageHeader eyebrow="AGENT HARNESS / CONTROL ROOM" title="Harness Workbench" description="Session 是可恢复的控制面对象；模型只提出 proposal，运行时负责策略、提交和证据。" />
        <InlineError message={error} />
        <SessionComposer loading={loading} onCreateRun={onCreateRun} />
        <SessionList sessions={sessions} activeTaskId={activeTaskId} onSelectSession={onSelectSession} />
      </div>
    );
  }

  const { snapshot, metrics, events } = data;
  const request = snapshot.request;
  const context = snapshot.context;
  const model = snapshot.model;
  const cache = snapshot.cache;
  const durability = snapshot.durability;
  const finalAnswer = parseJson(data.final_answer);
  const mode = String(snapshot.fixture?.mode ?? "scripted_model");
  const modelName = String(model.adapter ?? "scripted");
  const eventHead = events.at(-1);
  const cacheStats = asObject(cache.stats);
  const contextUsage = Math.min(100, (readNumber(context, "estimated_tokens") / Math.max(1, readNumber(context, "budget_tokens"))) * 100);

  return (
    <div className="space-y-7">
      <PageHeader
        eyebrow="AGENT HARNESS / CONTROL ROOM"
        title="Harness Workbench"
        description="Model proposes. Harness decides. Environment executes. Evidence proves."
        action={
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="ghost" onPress={() => onNavigate("trajectory")}>
              <Activity className="h-4 w-4" />
              查看轨迹
            </Button>
            <Button variant="primary" onPress={onRun} isPending={loading}>
              <Play className="h-4 w-4" />
              Replay fixture
            </Button>
          </div>
        }
      />
      <InlineError message={error} />

      <SessionComposer loading={loading} onCreateRun={onCreateRun} />
      <SessionList sessions={sessions} activeTaskId={activeTaskId} onSelectSession={onSelectSession} />

      <section id="session-overview" className="scroll-mt-24 space-y-5">
        <Panel
          eyebrow="Live session"
          title="一次真实 Harness run"
          description="离线 scripted model 只负责提出 proposal；下面的状态、工具副作用和证据来自 Go runtime。"
          action={<StatusChip value={data.status} />}
        >
          <div className="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(340px,0.8fr)]">
            <div className="space-y-5">
              <div className="flex flex-wrap items-center gap-2">
                <Chip color="accent" variant="soft" size="sm"><Chip.Label className="font-mono text-[11px]">{data.task_id}</Chip.Label></Chip>
                <Chip color="default" variant="soft" size="sm"><Chip.Label>{mode === "scripted_model" ? "offline fixture" : mode}</Chip.Label></Chip>
                <Chip color="success" variant="soft" size="sm"><Chip.Label className="inline-flex items-center gap-1.5"><span className="status-dot" />hash chain verified</Chip.Label></Chip>
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between gap-3 text-[11px] text-muted">
                  <span>Goal</span>
                  <span className="font-mono">{String(request.task_id ?? data.task_id)}</span>
                </div>
                <div className="readonly-field text-sm leading-6">{String(request.goal ?? "--")}</div>
              </div>
              <div className="grid gap-3 sm:grid-cols-3">
                <RuntimeField label="Model adapter" value={modelName} icon={<BrainCircuit className="h-4 w-4" />} />
                <RuntimeField label="Reasoning" value={String(request.reasoning_effort ?? "low")} icon={<Sparkles className="h-4 w-4" />} />
                <RuntimeField label="Policy" value={String(request.policy_version ?? "policy-v1")} icon={<ShieldCheck className="h-4 w-4" />} />
              </div>
            </div>
            <div className="session-boundary">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-xs font-semibold text-foreground">Commit boundary</p>
                  <p className="mt-1 text-[11px] text-muted">只有 durable event 才能推进状态</p>
                </div>
                <LockKeyhole className="h-5 w-5 text-accent" />
              </div>
              <div className="mt-5 space-y-3">
                <BoundaryLine label="task state" value={data.status} tone="success" />
                <BoundaryLine label="event head" value={shortHash(String(durability.event_head_hash ?? eventHead?.event_hash), 18)} tone="accent" />
                <BoundaryLine label="checkpoint" value={durability.checkpoint_available ? `sequence ${durability.checkpoint_sequence ?? "--"}` : "missing"} tone={durability.checkpoint_available ? "success" : "warning"} />
                <BoundaryLine label="replay" value={durability.replay_verified ? "verified" : "needs review"} tone={durability.replay_verified ? "success" : "warning"} />
              </div>
            </div>
          </div>
        </Panel>

        <KPIGroup className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
          <KPI>
            <KPI.Header><KPI.Title>Run status</KPI.Title><KPI.Icon status="success"><CheckCircle2 className="h-4 w-4" /></KPI.Icon></KPI.Header>
            <KPI.Content><StatusChip value={data.status} /></KPI.Content>
            <KPI.Footer><span className="font-mono text-[11px]">{data.task_id}</span></KPI.Footer>
          </KPI>
          <MetricKpi label="model rounds" value={metrics.model_rounds} icon={<BrainCircuit className="h-4 w-4" />} suffix="proposals" />
          <MetricKpi label="tool calls" value={metrics.tool_calls} icon={<Wrench className="h-4 w-4" />} suffix="committed" />
          <MetricKpi label="steps" value={metrics.steps} icon={<Route className="h-4 w-4" />} suffix="loop turns" />
          <MetricKpi label="events" value={metrics.event_count} icon={<Hash className="h-4 w-4" />} suffix="journal rows" />
        </KPIGroup>
      </section>

      <section id="agent-loop" className="scroll-mt-24 space-y-5">
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1.2fr)_minmax(340px,0.8fr)]">
          <Panel
            eyebrow="Agent loop"
            title="从 proposal 到 durable evidence"
            description="按事件顺序展开一轮完整的 context → model → tool → verifier → commit。"
            action={<Button variant="ghost" size="sm" onPress={() => onNavigate("trajectory")}><ArrowUpRight className="h-4 w-4" />完整日志</Button>}
          >
            <div className="relative space-y-1">
              <div className="timeline-rail" />
              {events.map((event) => <EventTimelineItem key={`${event.sequence}-${event.event_hash}`} event={event} />)}
            </div>
          </Panel>

          <Panel eyebrow="上下文诊断" title="本轮上下文快照" description="这是发给模型前的只读组装结果，仅用于工程排障；不是模型回答，也不是模型服务内部的 GPU KV cache。">
            <div className="space-y-5">
              <div className="flex items-end justify-between gap-3">
                <div><p className="text-3xl font-semibold numeric">{readNumber(context, "estimated_tokens").toLocaleString()}</p><p className="mt-1 text-[11px] text-muted">估算 token</p></div>
                <div className="text-right"><p className="font-mono text-sm">{readNumber(context, "budget_tokens").toLocaleString()}</p><p className="mt-1 text-[11px] text-muted">上下文上限</p></div>
              </div>
              <div className="meter-track"><span className="meter-fill" style={{ width: `${contextUsage}%` }} /></div>
              <div className="grid grid-cols-2 gap-2">
                <MiniMetric label="稳定前缀" value={readNumber(context, "stable_messages")} />
                <MiniMetric label="动态消息" value={readNumber(context, "dynamic_messages")} />
                <MiniMetric label="记忆命中" value={readNumber(context, "memory_items")} />
                <MiniMetric label="可见工具" value={readNumber(context, "visible_tools")} />
              </div>
              <div className="rounded-lg border border-border/70 bg-surface-tertiary/70 p-3">
                <div className="flex items-center justify-between gap-3"><span className="text-xs text-muted">前缀缓存</span><Chip color={readBoolean(context, "prefix_cache_hit") ? "success" : "warning"} variant="soft" size="sm"><Chip.Label>{readBoolean(context, "prefix_cache_hit") ? "命中" : "未命中"}</Chip.Label></Chip></div>
                <p className="mt-2 break-all font-mono text-[10px] leading-5 text-foreground/70">{String(context.prefix_cache_key ?? "--")}</p>
              </div>
              <Button variant="outline" fullWidth onPress={() => onNavigate("context")}><Layers3 className="h-4 w-4" />打开上下文诊断</Button>
            </div>
          </Panel>
        </div>
      </section>

      <section id="evidence-summary" className="scroll-mt-24 space-y-5">
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
          <Panel eyebrow="Final answer" title="Verifier 通过的结构化输出" description="最终文本只有在 schema 和 verifier 都通过后才会写入 TASK_COMPLETED。">
            <JsonBlock value={finalAnswer} />
          </Panel>
          <Panel eyebrow="Runtime surfaces" title="控制面边界" description="从同一个 demo snapshot 继续检查工具、记忆、子代理和长任务能力。">
            <div className="grid gap-2 sm:grid-cols-2">
              <SurfaceLink icon={<TerminalSquare className="h-4 w-4" />} label="Tools & MCP" detail={`${snapshot.tools.length} registered tools`} onPress={() => onNavigate("tools")} />
              <SurfaceLink icon={<Sparkles className="h-4 w-4" />} label="Skills & memory" detail={`${snapshot.skills.length} active skill · ${snapshot.memory.length} hit`} onPress={() => onNavigate("skills")} />
              <SurfaceLink icon={<GitCommitHorizontal className="h-4 w-4" />} label="Subagents" detail="fan-out / fan-in contract" onPress={() => onNavigate("subagents")} />
              <SurfaceLink icon={<Database className="h-4 w-4" />} label="Long-horizon DAG" detail="lease · attempt · UAT" onPress={() => onNavigate("dag")} />
              <SurfaceLink icon={<FileCheck2 className="h-4 w-4" />} label="Evaluation" detail="success · p95 · ablation" onPress={() => onNavigate("evaluation")} />
              <SurfaceLink icon={<Copy className="h-4 w-4" />} label="Replay evidence" detail={`${readNumber(durability, "replay_events")} hash-linked events`} onPress={() => onNavigate("trajectory")} />
            </div>
            <div className="mt-5 border-t border-border/70 pt-4 text-[11px] text-muted">
              <span className="inline-flex items-center gap-2"><Clock3 className="h-3.5 w-3.5 text-accent" />last commit {formatDate(eventHead?.created_at)}</span>
            </div>
          </Panel>
        </div>
      </section>
    </div>
  );
}

function SessionComposer({ loading, onCreateRun }: { loading: boolean; onCreateRun: (input: HarnessRunRequest) => Promise<void> }) {
  const [goal, setGoal] = useState("");
  const [reasoning, setReasoning] = useState("low");
  const [submitError, setSubmitError] = useState("");

  const submit = async () => {
    const trimmed = goal.trim();
    if (!trimmed) {
      setSubmitError("请输入一个 Harness goal");
      return;
    }
    setSubmitError("");
    try {
      await onCreateRun({ goal: trimmed, reasoning_effort: reasoning, constraints: ["return structured evidence"] });
      setGoal("");
    } catch (cause) {
      setSubmitError(cause instanceof Error ? cause.message : "Harness run 启动失败");
    }
  };

  return (
    <Panel eyebrow="Control plane / create session" title="启动一个 Harness session" description="这会向 Go runtime 提交 TaskRequest，并返回可重放的 session event log；当前 adapter 是离线 scripted fixture。">
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_220px_auto] lg:items-end">
        <label className="min-w-0">
          <span className="mb-2 block text-[11px] font-medium text-muted">Goal</span>
          <textarea value={goal} onChange={(event) => setGoal(event.target.value)} placeholder="例如：验证一次 artifact 写入并返回结构化证据" rows={3} className="composer-textarea w-full" />
        </label>
        <label>
          <span className="mb-2 block text-[11px] font-medium text-muted">Reasoning effort</span>
          <select value={reasoning} onChange={(event) => setReasoning(event.target.value)} className="composer-select w-full">
            <option value="low">low</option>
            <option value="medium">medium</option>
            <option value="high">high</option>
          </select>
        </label>
        <Button variant="primary" onPress={() => void submit()} isPending={loading} className="lg:min-w-[150px]"><Plus className="h-4 w-4" />创建 session</Button>
      </div>
      {submitError ? <p className="mt-3 text-xs text-danger">{submitError}</p> : null}
    </Panel>
  );
}

function SessionList({ sessions, activeTaskId, onSelectSession }: { sessions: HarnessSessionSummary[]; activeTaskId: string | null; onSelectSession: (taskId: string) => Promise<void> }) {
  if (sessions.length === 0) return null;
  return (
    <Panel eyebrow="Session registry" title={`${sessions.length} durable session${sessions.length === 1 ? "" : "s"}`} description="列表来自 Harness Store 的 materialized task state；选择后从 event log 重建当前视图。">
      <div className="session-list" role="list" aria-label="Harness sessions">
        {sessions.map((session) => (
          <Button key={session.task_id} variant="ghost" fullWidth onPress={() => void onSelectSession(session.task_id)} className={`session-list__item h-auto justify-start rounded-lg border px-3 py-3 text-left ${activeTaskId === session.task_id ? "session-list__item--active" : ""}`}>
            <span className="session-list__status"><span className={`status-dot ${session.status === "COMPLETED" ? "text-success" : session.status === "FAILED" ? "text-danger" : "text-warning"}`} /></span>
            <span className="min-w-0 flex-1"><span className="flex flex-wrap items-center gap-2"><span className="truncate text-xs font-semibold">{session.goal}</span><StatusChip value={session.status} /></span><span className="mt-1 block truncate font-mono text-[10px] text-muted">{session.task_id} · step {session.step} · {session.tool_calls} tool calls</span></span>
          </Button>
        ))}
      </div>
    </Panel>
  );
}

function EventTimelineItem({ event }: { event: HarnessEvent }) {
  const meta = eventMeta[event.event_type] ?? { label: event.event_type, tone: "default" as const, icon: CircleDot };
  const Icon = meta.icon;
  const toolResult = event.event_type === "TOOLS_EXECUTED" ? firstObject(event.payload.results) : null;
  return (
    <div className="timeline-item">
      <div className={`timeline-icon timeline-icon--${meta.tone}`}><Icon className="h-4 w-4" /></div>
      <div className="min-w-0 flex-1 pb-4">
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
          <span className="font-mono text-[11px] text-muted">{String(event.sequence).padStart(2, "0")}</span>
          <span className="text-sm font-medium">{meta.label}</span>
          <StatusChip value={event.event_type === "TOOLS_EXECUTED" ? String(toolResult?.status ?? "SUCCEEDED") : event.event_type === "TASK_COMPLETED" ? "COMPLETED" : "COMMITTED"} tone={meta.tone} />
          <span className="ml-auto text-[11px] text-muted">{formatDate(event.created_at)}</span>
        </div>
        <p className="mt-1 truncate font-mono text-[11px] text-muted">{event.event_type === "TOOLS_EXECUTED" ? `${String(toolResult?.tool ?? "tool")} · ${String(toolResult?.call_id ?? "call")}` : summarizeEvent(event)}</p>
      </div>
    </div>
  );
}

function RuntimeField({ label, value, icon }: { label: string; value: string; icon: ReactNode }) {
  return <div className="runtime-field"><span className="text-accent">{icon}</span><div className="min-w-0"><p className="text-[11px] text-muted">{label}</p><p className="mt-1 truncate font-mono text-xs">{value}</p></div></div>;
}

function BoundaryLine({ label, value, tone }: { label: string; value: string; tone: "success" | "accent" | "warning" }) {
  return <div className="flex items-center justify-between gap-3 text-xs"><span className="text-muted">{label}</span><span className={`inline-flex items-center gap-1.5 text-${tone}`}><span className="status-dot" />{value}</span></div>;
}

function MetricKpi({ label, value, icon, suffix }: { label: string; value: number; icon: ReactNode; suffix: string }) {
  return <KPI><KPI.Header><KPI.Title>{label}</KPI.Title><KPI.Icon>{icon}</KPI.Icon></KPI.Header><KPI.Content><KPI.Value value={value} /></KPI.Content><KPI.Footer><span className="text-xs text-muted">{suffix}</span></KPI.Footer></KPI>;
}

function MiniMetric({ label, value }: { label: string; value: number }) {
  return <div className="rounded-lg bg-surface-tertiary px-3 py-2.5"><p className="text-[11px] text-muted">{label}</p><p className="mt-1 font-mono text-sm numeric">{value}</p></div>;
}

function SurfaceLink({ icon, label, detail, onPress }: { icon: ReactNode; label: string; detail: string; onPress: () => void }) {
  return <Button variant="ghost" fullWidth onPress={onPress} className="surface-link h-auto min-h-[68px] justify-start rounded-lg border border-border/70 px-3 py-3 text-left"><span className="surface-link__icon">{icon}</span><span className="min-w-0"><span className="block text-xs font-medium">{label}</span><span className="mt-1 block truncate text-[11px] font-normal text-muted">{detail}</span></span><ArrowUpRight className="ml-auto h-3.5 w-3.5 shrink-0 text-muted" /></Button>;
}

function summarizeEvent(event: HarnessEvent) {
  if (event.event_type === "PLAN_COMPILED") return `${readArray(event.payload, "steps").length} plan steps materialized`;
  if (event.event_type === "TASK_ACCEPTED") return String(event.payload.goal ?? "task accepted");
  if (event.event_type === "MODEL_REQUESTED") return `provider input sealed · ${String(event.payload.request_digest ?? "digest pending")}`;
  if (event.event_type === "MODEL_PROPOSED") {
    const response = asObject(event.payload.response);
    const toolCalls = readArray(response, "tool_calls").length;
    return toolCalls > 0 ? `${toolCalls} tool proposal${toolCalls === 1 ? "" : "s"} awaiting policy` : `proposal · ${String(response.finish_reason ?? "stop")}`;
  }
  if (event.event_type === "CONTEXT_COMPILED") return `${String(event.payload.estimated_tokens ?? 0)} tokens · prefix ${event.payload.prefix_cache_hit === true ? "hit" : "miss"}`;
  if (event.event_type === "TOOL_INVOKED") {
    const call = asObject(event.payload.call);
    return `${String(call.name ?? "tool")} · proposal admitted before execution`;
  }
  if (event.event_type === "TOOL_OBSERVED") return String(asObject(event.payload.observation).status ?? "observation committed");
  if (event.event_type === "VERIFIER_REJECTED") return String(asObject(event.payload.error).message ?? "verifier rejected output");
  if (event.event_type === "USER_GATE_OPENED") return "runtime is waiting for an explicit user decision";
  if (event.event_type === "TASK_COMPLETED") return String(event.payload.final_answer ?? "structured output verified");
  return "durable state transition";
}

function parseJson(value: string): unknown {
  try { return JSON.parse(value); } catch { return { raw: value }; }
}

function asObject(value: unknown): JsonObject {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : {};
}

function readNumber(value: unknown, key: string) {
  const candidate = asObject(value)[key];
  return typeof candidate === "number" ? candidate : Number(candidate ?? 0) || 0;
}

function readBoolean(value: unknown, key: string) {
  return asObject(value)[key] === true;
}

function readArray(value: unknown, key: string): unknown[] {
  const candidate = asObject(value)[key];
  return Array.isArray(candidate) ? candidate : [];
}

function firstObject(value: unknown): JsonObject | null {
  const array = Array.isArray(value) ? value : [];
  const first = array[0];
  return first && typeof first === "object" ? first as JsonObject : null;
}
