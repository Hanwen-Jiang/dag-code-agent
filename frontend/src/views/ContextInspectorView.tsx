import { useMemo, useState } from "react";
import {
  Braces,
  CheckCircle2,
  ChevronDown,
  Database,
  FileKey2,
  Layers3,
  LockKeyhole,
  MemoryStick,
  PackageOpen,
  Search,
  ShieldCheck,
  Sparkles,
  Wrench,
} from "lucide-react";
import { Button, Chip } from "@heroui/react";
import { Segment } from "@heroui-pro/react/segment";

import type { HarnessDemoResponse, JsonObject } from "../types";
import { InlineError, JsonBlock, LoadingState, PageHeader, Panel, formatDate } from "../components/Primitives";
import type { HarnessViewKey } from "./HarnessWorkbenchView";

type ContextTab = "compiler" | "cache" | "memory";

export function ContextInspectorView({ data, loading, error, onNavigate }: { data: HarnessDemoResponse | null; loading: boolean; error: string; onNavigate: (view: HarnessViewKey) => void }) {
  const [tab, setTab] = useState<ContextTab>("compiler");
  const [expanded, setExpanded] = useState<string[]>(["stable", "dynamic"]);

  if (loading && !data) return <LoadingState label="正在读取上下文诊断快照" />;
  if (!data) return <div className="space-y-6"><PageHeader eyebrow="设置 / 高级诊断" title="上下文诊断" description="等待本次 session 的运行快照。" /><InlineError message={error || "上下文快照不可用"} /></div>;

  const context = data.snapshot.context;
  const request = data.snapshot.request;
  const estimated = numberAt(context, "estimated_tokens");
  const budget = numberAt(context, "budget_tokens");
  const usage = Math.min(100, (estimated / Math.max(1, budget)) * 100);
  const memories = data.snapshot.memory;
  const skills = data.snapshot.skills;
  const tools = data.snapshot.tools;
  const toggled = (key: string) => expanded.includes(key);
  const toggle = (key: string) => setExpanded((current) => current.includes(key) ? current.filter((item) => item !== key) : [...current, key]);

  return (
    <div className="space-y-7">
      <PageHeader eyebrow="设置 / 高级诊断" title="上下文诊断" description="只读查看本轮请求在发给模型前如何组装；这里是工程诊断快照，不是聊天内容。" action={<Button variant="ghost" onPress={() => onNavigate("settings")}><ChevronDown className="h-4 w-4 rotate-90" />回到设置</Button>} />
      <InlineError message={error} />

      <Panel eyebrow="诊断快照" title="本轮上下文统计" description="估算值来自 Go `EstimateTokens`；它不代表模型服务内部的真实 GPU KV cache。" action={<Chip color={context.prefix_cache_hit === true ? "success" : "warning"} variant="soft" size="sm"><Chip.Label>{context.prefix_cache_hit === true ? "前缀命中" : "前缀未命中"}</Chip.Label></Chip>}>
        <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(300px,0.7fr)]">
          <div className="space-y-4"><div className="flex items-end justify-between gap-3"><div><p className="text-3xl font-semibold numeric">{estimated.toLocaleString()}</p><p className="mt-1 text-xs text-muted">估算 token</p></div><div className="text-right"><p className="font-mono text-sm">{budget.toLocaleString()}</p><p className="mt-1 text-xs text-muted">上下文上限</p></div></div><div className="meter-track meter-track--large"><span className="meter-fill" style={{ width: `${usage}%` }} /></div><div className="flex flex-wrap gap-2 text-[11px] text-muted"><span>{numberAt(context, "stable_messages")} 条稳定消息</span><span>·</span><span>{numberAt(context, "dynamic_messages")} 条动态消息</span><span>·</span><span>编译器 {String(context.compiler_version ?? "context-v1")}</span></div></div>
          <div className="grid grid-cols-2 gap-2"><ContextMetric icon={<MemoryStick className="h-4 w-4" />} label="memory" value={numberAt(context, "memory_items")} /><ContextMetric icon={<Sparkles className="h-4 w-4" />} label="skills" value={numberAt(context, "skill_items")} /><ContextMetric icon={<Wrench className="h-4 w-4" />} label="tools" value={numberAt(context, "visible_tools")} /><ContextMetric icon={<ShieldCheck className="h-4 w-4" />} label="invariants" value={arrayAt(context, "exact_invariants").length} /></div>
        </div>
      </Panel>

      <Segment selectedKey={tab} onSelectionChange={(key) => setTab(String(key) as ContextTab)} size="sm" className="w-fit"><Segment.Item id="compiler">上下文结构</Segment.Item><Segment.Item id="cache">前缀缓存</Segment.Item><Segment.Item id="memory">记忆与技能</Segment.Item></Segment>

      {tab === "compiler" ? <div className="grid gap-5 xl:grid-cols-[minmax(0,1.15fr)_minmax(340px,0.85fr)]"><Panel eyebrow="稳定内容 / 动态状态" title="模型输入投影" description="稳定内容用于复用；动态内容随任务状态、工具观察和用户确认变化。"><div className="space-y-2"><Disclosure title="稳定前缀" icon={<LockKeyhole className="h-4 w-4" />} open={toggled("stable")} onToggle={() => toggle("stable")} tone="accent"><div className="space-y-2"><MessageRow role="system" content="You are a controlled Agent Harness worker." /><MessageRow role="developer" content="Model output is a proposal; the runtime commits state and effects." /><div className="flex flex-wrap gap-2 pt-1"><Chip size="sm" variant="soft" color="accent"><Chip.Label>策略已绑定</Chip.Label></Chip><Chip size="sm" variant="soft" color="default"><Chip.Label>工具 schema</Chip.Label></Chip></div></div></Disclosure><Disclosure title="动态任务状态" icon={<Braces className="h-4 w-4" />} open={toggled("dynamic")} onToggle={() => toggle("dynamic")}><div className="space-y-3"><MessageRow role="user" content={String(request.goal ?? "--")} /><MessageRow role="constraint" content={String(arrayAt(request, "constraints").join(" · ") || "return structured evidence")} /><MessageRow role="state" content={`status=${String(data.status)} · step=${String(data.metrics.steps)} · tool_calls=${String(data.metrics.tool_calls)}`} /></div></Disclosure><Disclosure title="检索到的上下文" icon={<PackageOpen className="h-4 w-4" />} open={toggled("retrieved")} onToggle={() => toggle("retrieved")}><div className="grid gap-2 sm:grid-cols-2"><ContextPill label="记忆条目" value={memories.length} /><ContextPill label="技能条目" value={skills.length} /><ContextPill label="可见工具" value={tools.length} /><ContextPill label="响应格式" value="严格 JSON" /></div></Disclosure></div></Panel><Panel eyebrow="编译器状态" title="上下文封装" description="任务状态、记忆和技能在投影阶段显式加入模型请求。"><JsonBlock value={{ task_id: data.task_id, compiler_version: context.compiler_version, estimated_tokens: estimated, budget_tokens: budget, exact_invariants: context.exact_invariants, prefix_cache_key: context.prefix_cache_key }} /></Panel></div> : null}

      {tab === "cache" ? <CachePanel data={data} /> : null}
      {tab === "memory" ? <MemoryPanel data={data} /> : null}
    </div>
  );
}

function CachePanel({ data }: { data: HarnessDemoResponse }) {
  const context = data.snapshot.context;
  const cache = data.snapshot.cache;
  const stats = asObject(cache.stats);
  const hits = numberAt(stats, "hits");
  const misses = numberAt(stats, "misses");
  const total = hits + misses;
  return <div className="grid gap-5 xl:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)]"><Panel eyebrow="前缀缓存" title="可复用的稳定前缀" description="缓存身份绑定 model/revision、reasoning、tenant、subject、scope、policy、compiler 和 skill digest。"><div className="grid grid-cols-2 gap-3 sm:grid-cols-3"><ContextMetric icon={<CheckCircle2 className="h-4 w-4" />} label="命中" value={hits} /><ContextMetric icon={<Search className="h-4 w-4" />} label="未命中" value={misses} /><ContextMetric icon={<Layers3 className="h-4 w-4" />} label="条目" value={numberAt(stats, "entries")} /></div><div className="mt-5 space-y-2"><div className="flex justify-between text-xs"><span className="text-muted">命中率</span><span className="font-mono">{total ? Math.round((hits / total) * 100) : 0}%</span></div><div className="meter-track"><span className="meter-fill meter-fill--success" style={{ width: `${total ? (hits / total) * 100 : 0}%` }} /></div></div><div className="mt-5 flex items-start gap-3 rounded-lg border border-accent/20 bg-accent/5 p-3 text-xs"><FileKey2 className="mt-0.5 h-4 w-4 shrink-0 text-accent" /><span className="leading-5 text-muted">本地只记录前缀账务和失效边界；真实 GPU KV cache 由模型服务持有。</span></div></Panel><Panel eyebrow="缓存身份" title="Key 内容"><p className="mb-3 text-[11px] text-muted">当前前缀缓存 key</p><p className="break-all rounded-lg bg-surface-tertiary p-3 font-mono text-[11px] leading-5">{String(context.prefix_cache_key ?? "--")}</p><div className="mt-5 space-y-3"><KeyLine label="model" value={String(data.snapshot.model.adapter ?? "scripted")} /><KeyLine label="policy" value={String(data.snapshot.request.policy_version ?? "policy-v1")} /><KeyLine label="compiler" value={String(context.compiler_version ?? "context-v1")} /><KeyLine label="provider boundary" value={String(cache.provider_kv_boundary ?? "provider/model service")} /></div></Panel></div>;
}

function MemoryPanel({ data }: { data: HarnessDemoResponse }) {
  const memories = data.snapshot.memory;
  const skills = data.snapshot.skills;
  return <div className="grid gap-5 xl:grid-cols-2"><Panel eyebrow="Memory manager" title={`${memories.length} retrieved memories`} description="按 tenant / subject / namespace 隔离，版本、过期和 tombstone 都属于 durable contract。"><div className="space-y-2">{memories.map((memory) => <div key={String(memory.memory_id)} className="memory-row"><span className="memory-row__icon"><Database className="h-4 w-4" /></span><span className="min-w-0 flex-1"><span className="flex flex-wrap items-center gap-2"><span className="text-xs font-medium">{String(memory.key ?? "memory")}</span><Chip size="sm" variant="soft" color="default"><Chip.Label>{String(memory.type ?? "semantic")}</Chip.Label></Chip></span><span className="mt-1 block truncate text-[11px] text-muted">{String(memory.content ?? "--")}</span><span className="mt-1 block font-mono text-[10px] text-muted">v{String(memory.version ?? "1")} · confidence {String(memory.confidence ?? "--")}</span></span></div>)}</div></Panel><Panel eyebrow="Skill registry" title={`${skills.length} active skills`} description="按 goal/tag discovery、版本排序和 scope filter 选择可注入 skill。"><div className="space-y-2">{skills.map((skill) => <div key={String(skill.skill_id)} className="memory-row"><span className="memory-row__icon memory-row__icon--skill"><Sparkles className="h-4 w-4" /></span><span className="min-w-0 flex-1"><span className="flex flex-wrap items-center gap-2"><span className="text-xs font-medium">{String(skill.title ?? skill.skill_id ?? "skill")}</span><Chip size="sm" variant="soft" color="success"><Chip.Label>{String(skill.version ?? "active")}</Chip.Label></Chip></span><span className="mt-1 block truncate text-[11px] text-muted">{String(skill.instructions ?? "--")}</span><span className="mt-1 block text-[10px] text-muted">tags: {Array.isArray(skill.tags) ? skill.tags.join(" · ") : "--"}</span></span></div>)}</div></Panel></div>;
}

function Disclosure({ title, icon, open, onToggle, children, tone = "default" }: { title: string; icon: React.ReactNode; open: boolean; onToggle: () => void; children: React.ReactNode; tone?: "default" | "accent" }) {
  return <div className={`disclosure ${open ? "disclosure--open" : ""}`}><Button variant="ghost" fullWidth onPress={onToggle} className="h-auto justify-start rounded-lg px-3 py-3 text-left"><span className={`disclosure__icon disclosure__icon--${tone}`}>{icon}</span><span className="text-xs font-semibold">{title}</span><ChevronDown className={`ml-auto h-4 w-4 text-muted transition-transform ${open ? "rotate-180" : ""}`} /></Button>{open ? <div className="px-3 pb-4 pt-1">{children}</div> : null}</div>;
}

function MessageRow({ role, content }: { role: string; content: string }) {
  return <div className="message-row"><span className="message-row__role">{role}</span><span className="min-w-0 text-xs leading-5">{content}</span></div>;
}

function ContextPill({ label, value }: { label: string; value: string | number }) {
  return <div className="rounded-lg bg-surface-tertiary px-3 py-2.5"><p className="text-[11px] text-muted">{label}</p><p className="mt-1 font-mono text-sm">{value}</p></div>;
}

function ContextMetric({ icon, label, value }: { icon: React.ReactNode; label: string; value: number }) {
  return <div className="context-metric"><span className="context-metric__icon">{icon}</span><span><span className="block text-[11px] text-muted">{label}</span><span className="mt-1 block font-mono text-lg numeric">{value}</span></span></div>;
}

function KeyLine({ label, value }: { label: string; value: string }) {
  return <div className="flex items-center justify-between gap-3 border-b border-border/70 pb-2 text-xs last:border-0 last:pb-0"><span className="text-muted">{label}</span><span className="max-w-[65%] truncate text-right font-mono text-[11px]">{value}</span></div>;
}

function numberAt(value: unknown, key: string) {
  const candidate = asObject(value)[key];
  return typeof candidate === "number" ? candidate : Number(candidate ?? 0) || 0;
}

function arrayAt(value: unknown, key: string): unknown[] {
  const candidate = asObject(value)[key];
  return Array.isArray(candidate) ? candidate : [];
}

function asObject(value: unknown): JsonObject {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : {};
}
