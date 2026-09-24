import { useMemo } from "react";
import {
  Activity,
  Boxes,
  CheckCircle2,
  Code2,
  Database,
  GitBranch,
  GitFork,
  Gauge,
  KeyRound,
  Layers3,
  Network,
  RotateCcw,
  Scale,
  Search,
  ShieldCheck,
  Sparkles,
  Wrench,
} from "lucide-react";
import { Button, Chip } from "@heroui/react";

import type { HarnessDemoResponse } from "../types";
import { InlineError, JsonBlock, LoadingState, PageHeader, Panel, StatusChip } from "../components/Primitives";
import type { HarnessViewKey } from "./HarnessWorkbenchView";

type CapabilityFocus = "tools" | "skills" | "subagents" | "evaluation";

type Capability = {
  id: string;
  label: string;
  summary: string;
  source: string;
  icon: React.ReactNode;
  detail: string[];
};

const capabilities: Capability[] = [
  { id: "model", label: "Model API boundary", summary: "provider-neutral complete / stream / structured output", source: "internal/harness/model.go", icon: <Network className="h-4 w-4" />, detail: ["OpenAI-compatible adapter", "SSE stream projection", "model router / fallback / circuit breaker", "usage、latency、cost ledger"] },
  { id: "loop", label: "Agent Loop", summary: "context → proposal → effect → observation → verifier", source: "internal/harness/runtime.go", icon: <Activity className="h-4 w-4" />, detail: ["max steps / tool budget", "loop fingerprint detection", "user gate / cancel / resume", "structured output + verifier before completion"] },
  { id: "context", label: "Context compiler + KV accounting", summary: "stable prefix 与 dynamic state 分开编译", source: "internal/harness/context.go", icon: <Layers3 className="h-4 w-4" />, detail: ["memory / skill / tool injection", "provider message projection", "context compaction", "prefix key identity + hit/miss/LRU stats"] },
  { id: "tools", label: "Tool Runtime + MCP", summary: "副作用进入环境前经过完整策略链", source: "internal/harness/tools.go", icon: <Wrench className="h-4 w-4" />, detail: ["JSON Schema validation", "scope + policy authorization", "timeout / retry / UNKNOWN", "idempotency ledger", "MCP initialize / tools/list / tools/call"] },
  { id: "skills", label: "Skills + Memory", summary: "可版本化的能力注入和长期记忆", source: "internal/harness/skills.go · memory.go", icon: <Sparkles className="h-4 w-4" />, detail: ["ACTIVE / DEPRECATED version selection", "goal/tag discovery + scope filter", "working / episodic / semantic / procedural contract", "version、expiry、supersedes、tombstone"] },
  { id: "multi", label: "Subagent / Multi-agent", summary: "隔离 context 的 fan-out / fan-in 协调", source: "internal/harness/multi_agent.go", icon: <GitFork className="h-4 w-4" />, detail: ["parallel limit", "subagent tool allow-list", "result merge", "conflict / consensus"] },
  { id: "durable", label: "Durable Evidence", summary: "状态、事件、checkpoint 绑定同一事务", source: "internal/harness/store.go", icon: <KeyRound className="h-4 w-4" />, detail: ["optimistic version conflict", "immutable event journal", "SHA-256 hash chain", "checkpoint / replay / lease"] },
  { id: "dag", label: "Long-horizon DAG", summary: "Canonical Manifest 驱动可恢复交付", source: "internal/dag/", icon: <GitBranch className="h-4 w-4" />, detail: ["ALL / ANY / QUORUM activation", "lease + fresh attempt", "artifact contract + evidence", "rework branch + UAT gate + crash recovery"] },
  { id: "eval", label: "Evaluation + Evolution", summary: "用固定 fixture 评估、对照和晋级", source: "internal/harness/evaluation.go · experiment.go", icon: <Gauge className="h-4 w-4" />, detail: ["success rate + Wilson CI", "p95 latency / mean steps / tool calls", "ablation comparison", "promotion guardrail"] },
];

export function CapabilityView({ data, loading, error, focus, onNavigate }: { data: HarnessDemoResponse | null; loading: boolean; error: string; focus: CapabilityFocus; onNavigate: (view: HarnessViewKey) => void }) {
  if (loading && !data) return <LoadingState label="正在读取 runtime capability snapshot" />;
  if (!data) return <div className="space-y-6"><PageHeader eyebrow="RUNTIME / CAPABILITIES" title="Capability map" description="等待 runtime snapshot。" /><InlineError message={error || "能力快照不可用"} /></div>;

  const selected = useMemo(() => capabilities.find((capability) => capability.id === focus || (focus === "subagents" && capability.id === "multi")) ?? capabilities[0], [focus]);
  const tools = data.snapshot.tools;
  const mcp = data.snapshot.mcp;

  return (
    <div className="space-y-7">
      <PageHeader eyebrow="RUNTIME / CAPABILITIES" title="Harness capability map" description="代码契约快照 · 通过 `/api/harness/demo` 运行时实例采集；这些不是线上吞吐或 GPU 指标。" action={<Button variant="ghost" onPress={() => onNavigate("workbench")}><RotateCcw className="h-4 w-4" />回到 session</Button>} />
      <div className="grid gap-5 xl:grid-cols-[minmax(0,1.15fr)_minmax(340px,0.85fr)]">
        <Panel eyebrow="Implemented contract" title="知识点覆盖矩阵" description="每一行都对应 Go 包中的可读实现边界。">
          <div className="capability-grid">{capabilities.map((capability) => <CapabilityRow key={capability.id} capability={capability} selected={capability.id === selected.id || (focus === "subagents" && capability.id === "multi")} onPress={() => { const next = capability.id === "multi" ? "subagents" : capability.id as CapabilityFocus; onNavigate(next); }} />)}</div>
        </Panel>
        <Panel eyebrow="Selected surface" title={selected.label} description={selected.summary}>
          <div className="flex items-center gap-3"><span className="capability-row__icon capability-row__icon--active">{selected.icon}</span><div><StatusChip value="IMPLEMENTED" tone="success" /><p className="mt-2 font-mono text-[11px] text-muted">{selected.source}</p></div></div><div className="mt-5 space-y-2">{selected.detail.map((item) => <div key={item} className="flex items-start gap-2 text-xs"><CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-success" /><span className="leading-5">{item}</span></div>)}</div>
          <div className="mt-6 border-t border-border/70 pt-4"><p className="mb-3 text-[11px] font-semibold tracking-[0.12em] text-muted">ROUTING</p><div className="flex flex-wrap gap-2"><Button size="sm" variant="outline" onPress={() => onNavigate("tools")}><Wrench className="h-4 w-4" />Tools</Button><Button size="sm" variant="outline" onPress={() => onNavigate("skills")}><Sparkles className="h-4 w-4" />Skills</Button><Button size="sm" variant="outline" onPress={() => onNavigate("dag")}><GitBranch className="h-4 w-4" />DAG</Button></div></div>
        </Panel>
      </div>

      {focus === "tools" ? <ToolsPanel data={data} /> : null}
      {focus === "skills" ? <MemorySkillsPanel data={data} /> : null}
      {focus === "subagents" ? <SubagentPanel data={data} /> : null}
      {focus === "evaluation" ? <EvaluationPanel data={data} /> : null}
      {focus !== "tools" && focus !== "skills" && focus !== "subagents" && focus !== "evaluation" ? <ToolsPanel data={data} /> : null}
    </div>
  );
}

function CapabilityRow({ capability, selected, onPress }: { capability: Capability; selected: boolean; onPress: () => void }) {
  return <Button variant="ghost" fullWidth onPress={onPress} className={`capability-row h-auto justify-start rounded-lg border px-3 py-3 text-left ${selected ? "capability-row--selected" : ""}`}><span className={`capability-row__icon ${selected ? "capability-row__icon--active" : ""}`}>{capability.icon}</span><span className="min-w-0 flex-1"><span className="flex items-center gap-2"><span className="truncate text-xs font-semibold">{capability.label}</span><Chip color="success" variant="soft" size="sm"><Chip.Label>GO</Chip.Label></Chip></span><span className="mt-1 block truncate text-[11px] font-normal text-muted">{capability.summary}</span><span className="mt-1 block truncate font-mono text-[10px] font-normal text-muted">{capability.source}</span></span></Button>;
}

function ToolsPanel({ data }: { data: HarnessDemoResponse }) {
  const mcp = data.snapshot.mcp;
  return <div className="grid gap-5 xl:grid-cols-[minmax(0,1.15fr)_minmax(340px,0.85fr)]"><Panel eyebrow="Tools" title={`${data.snapshot.tools.length} registered tools`} description="schema、scope、side effect 和幂等要求来自当前 DemoEnvironment registry。"><div className="space-y-2">{data.snapshot.tools.map((tool) => <div key={tool.name} className="tool-row"><span className="tool-row__icon"><Wrench className="h-4 w-4" /></span><span className="min-w-0 flex-1"><span className="flex flex-wrap items-center gap-2"><span className="text-xs font-medium">{tool.name}</span><Chip size="sm" variant="soft" color={tool.side_effect === "IRREVERSIBLE" ? "warning" : "default"}><Chip.Label>{tool.side_effect || "READ"}</Chip.Label></Chip>{tool.idempotency_required ? <Chip size="sm" variant="soft" color="accent"><Chip.Label>idempotent</Chip.Label></Chip> : null}</span><span className="mt-1 block truncate text-[11px] text-muted">{tool.description}</span><span className="mt-1 block font-mono text-[10px] text-muted">scopes: {(tool.required_scopes ?? []).join(" · ") || "none"} · timeout {tool.timeout_seconds ?? 10}s</span></span></div>)}</div></Panel><Panel eyebrow="MCP handshake" title="Protocol boundary"><div className="space-y-3"><StatusChip value={mcp.initialized === true ? "INITIALIZED" : "FAILED"} /><ContractLine label="protocol" value={String(asObject(mcp.initialize).protocolVersion ?? "2025-06-18")} /><ContractLine label="server" value={String(asObject(asObject(mcp.initialize).serverInfo).name ?? "agent-harness")} /><ContractLine label="tools/list" value={`${Array.isArray(mcp.tools) ? mcp.tools.length : 0} schemas`} /><JsonBlock value={{ initialize: mcp.initialize, tools: mcp.tools }} /></div></Panel></div>;
}

function MemorySkillsPanel({ data }: { data: HarnessDemoResponse }) {
  return <div className="grid gap-5 xl:grid-cols-2"><Panel eyebrow="Memory contract" title="Retrieval + lifecycle"><div className="grid grid-cols-2 gap-2 sm:grid-cols-4"><TinyStat label="hits" value={data.snapshot.memory.length} /><TinyStat label="versioned" value={data.snapshot.memory.filter((item) => Number(item.version ?? 0) > 0).length} /><TinyStat label="scoped" value={data.snapshot.auth.subject_id ? 1 : 0} /><TinyStat label="tombstone" value={0} /></div><div className="mt-5 rounded-lg border border-border/70 bg-surface-tertiary/60 p-3 text-xs leading-5 text-muted">memory 记录使用 tenant / subject / namespace 隔离；更新通过 supersedes 链接版本，过期和 forget 通过 tombstone 让旧记录停止暴露。</div></Panel><Panel eyebrow="Skill registry" title="Active resolution"><div className="space-y-2">{data.snapshot.skills.map((skill) => <ContractLine key={String(skill.skill_id)} label={`${String(skill.skill_id)}@${String(skill.version)}`} value={String(skill.status ?? "ACTIVE")} />)}</div><div className="mt-5 rounded-lg border border-accent/20 bg-accent/5 p-3 text-xs leading-5 text-muted">skill digest 会进入 prefix cache identity，因此升级 skill 版本会自然失效旧 stable prefix。</div></Panel></div>;
}

function SubagentPanel({ data }: { data: HarnessDemoResponse }) {
  return <Panel eyebrow="Subagent / Multi-agent" title="Fan-out / fan-in contract" description="当前 offline fixture 没有发起 subagent，但 Go coordinator 的边界已在 runtime 中可复用。"><div className="grid gap-3 md:grid-cols-3"><FlowStep icon={<GitFork className="h-4 w-4" />} title="Fan-out" detail="isolated task id + context" /><FlowStep icon={<Scale className="h-4 w-4" />} title="Bounded parallel" detail="max_parallel semaphore" /><FlowStep icon={<GitBranch className="h-4 w-4" />} title="Fan-in" detail="merge / conflict / consensus" /></div><div className="mt-6 grid gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(320px,0.8fr)]"><div className="space-y-2 text-xs leading-5 text-muted"><p>Coordinator 为每个 agent 生成隔离 task id、重新编译 context，并按 allow-list 过滤工具。</p><p>结果携带 status、summary、artifacts 和 usage；requireConsensus 打开时，冲突不会静默合并。</p></div><JsonBlock value={{ max_parallel: 4, context_isolation: true, tool_allow_list: true, merge_modes: ["best_effort", "require_consensus"], fixture_task: data.task_id }} /></div></Panel>;
}

function EvaluationPanel({ data }: { data: HarnessDemoResponse }) {
  const events = data.events.length;
  return <div className="grid gap-5 xl:grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)]"><Panel eyebrow="Evaluation" title="Trajectory metrics"><div className="grid grid-cols-2 gap-2 sm:grid-cols-4"><TinyStat label="success" value={data.status === "COMPLETED" ? "100%" : "0%"} /><TinyStat label="steps" value={data.metrics.steps} /><TinyStat label="tool calls" value={data.metrics.tool_calls} /><TinyStat label="events" value={events} /></div><div className="mt-5 rounded-lg border border-border/70 bg-surface-tertiary/60 p-3 text-xs leading-5 text-muted">生产评测使用固定 cases 输出 success rate、Wilson confidence interval、p95 latency、mean steps/tool calls 和 failure taxonomy。</div></Panel><Panel eyebrow="Promotion guardrail" title="Candidate → promotion"><div className="space-y-3"><Guardrail label="sample count" value="required" /><Guardrail label="success delta" value="guarded" /><Guardrail label="latency delta" value="p95 bound" /><Guardrail label="step delta" value="regression check" /></div><div className="mt-5"><JsonBlock value={{ evaluator: "EvaluateCases", compare: "CompareEvaluation", ablation: "RunAblation", promotion: "EvaluatePromotion", production_mutation: false }} /></div></Panel></div>;
}

function FlowStep({ icon, title, detail }: { icon: React.ReactNode; title: string; detail: string }) { return <div className="flow-step"><span className="flow-step__icon">{icon}</span><span><span className="block text-xs font-medium">{title}</span><span className="mt-1 block text-[11px] text-muted">{detail}</span></span></div>; }
function ContractLine({ label, value }: { label: string; value: string }) { return <div className="flex items-center justify-between gap-3 border-b border-border/70 pb-2 text-xs last:border-0 last:pb-0"><span className="text-muted">{label}</span><span className="max-w-[65%] truncate text-right font-mono text-[11px]">{value}</span></div>; }
function TinyStat({ label, value }: { label: string; value: string | number }) { return <div className="rounded-lg bg-surface-tertiary px-3 py-2.5"><p className="text-[11px] text-muted">{label}</p><p className="mt-1 font-mono text-sm">{value}</p></div>; }
function Guardrail({ label, value }: { label: string; value: string }) { return <div className="flex items-center justify-between gap-3 rounded-lg border border-border/70 px-3 py-2.5 text-xs"><span className="inline-flex items-center gap-2"><ShieldCheck className="h-4 w-4 text-success" />{label}</span><span className="font-mono text-[11px] text-muted">{value}</span></div>; }
function asObject(value: unknown): Record<string, unknown> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
