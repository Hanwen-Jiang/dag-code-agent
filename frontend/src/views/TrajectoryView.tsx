import { useMemo, useState } from "react";
import {
  Check,
  CheckCircle2,
  ChevronRight,
  CircleDot,
  FileCode2,
  Hash,
  History,
  Link2,
  RefreshCw,
  Wrench,
} from "lucide-react";
import { Button } from "@heroui/react";
import { Segment } from "@heroui-pro/react/segment";

import type { HarnessDemoResponse, HarnessEvent, JsonObject } from "../types";
import { InlineError, JsonBlock, LoadingState, PageHeader, Panel, StatusChip, formatDate, shortHash } from "../components/Primitives";
import type { HarnessViewKey } from "./HarnessWorkbenchView";

type Filter = "all" | "commits" | "tools";

export function TrajectoryView({ data, loading, error, onRun, onNavigate }: { data: HarnessDemoResponse | null; loading: boolean; error: string; onRun: () => void; onNavigate: (view: HarnessViewKey) => void }) {
  const [filter, setFilter] = useState<Filter>("all");
  const [selectedSequence, setSelectedSequence] = useState(1);

  const filtered = useMemo(() => (data?.events ?? []).filter((event) => {
    const isToolEvent = ["TOOL_INVOKED", "TOOL_OBSERVED", "TOOLS_EXECUTED"].includes(event.event_type);
    if (filter === "tools") return isToolEvent;
    if (filter === "commits") return !isToolEvent;
    return true;
  }), [data?.events, filter]);

  if (loading && !data) return <LoadingState label="正在读取 immutable event journal" />;
  if (!data) return <EmptyTrajectory error={error} onRun={onRun} />;

  const selected = data.events.find((event) => event.sequence === selectedSequence) ?? data.events[0];
  const verified = Boolean(data.snapshot.durability.replay_verified);

  return (
    <div className="space-y-7">
      <PageHeader
        eyebrow="SESSION / TRAJECTORY"
        title="Event timeline"
        description="Conversation is a projection. This immutable journal is the source of truth for replay and audit."
        action={<div className="flex items-center gap-2"><Button variant="ghost" onPress={() => onNavigate("workbench")}><ChevronRight className="h-4 w-4 rotate-180" />工作台</Button><Button variant="primary" onPress={onRun} isPending={loading}><RefreshCw className="h-4 w-4" />Replay</Button></div>}
      />
      <InlineError message={error} />

      <div className="grid gap-5 xl:grid-cols-[minmax(0,1.2fr)_minmax(340px,0.8fr)]">
        <Panel eyebrow="Immutable journal" title={`${data.events.length} events · ${data.task_id}`} description="sequence、task version、previous hash 与 event hash 均由 Go Store 写入。" action={<StatusChip value={verified ? "VERIFIED" : "UNKNOWN"} /> }>
          <Segment selectedKey={filter} onSelectionChange={(key) => setFilter(String(key) as Filter)} size="sm" className="mb-4 w-fit"><Segment.Item id="all">All events</Segment.Item><Segment.Item id="commits">Control commits</Segment.Item><Segment.Item id="tools">Tool results</Segment.Item></Segment>
          <div className="event-table" role="list" aria-label="Harness event timeline">
            {filtered.map((event) => <TimelineRow key={`${event.sequence}-${event.event_hash}`} event={event} selected={event.sequence === selected?.sequence} onSelect={() => setSelectedSequence(event.sequence)} />)}
          </div>
        </Panel>

        <Panel eyebrow="Selected event" title={selected?.event_type ?? "No event"} description={selected ? `sequence ${selected.sequence} · task version ${selected.task_version}` : "选择左侧事件查看 payload"}>
          {selected ? <EventInspector event={selected} /> : <LoadingState label="没有事件" />}
        </Panel>
      </div>

      <Panel eyebrow="Time-travel contract" title="Hash chain / checkpoint / replay" description="崩溃恢复和审计都从同一条 durable journal 重建，而不是从模型对话猜测。">
        <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(360px,0.9fr)]">
          <div className="hash-chain" aria-label="hash chain">
            {data.events.map((event, index) => <HashNode key={`${event.sequence}-${event.event_hash}`} event={event} isLast={index === data.events.length - 1} />)}
          </div>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1">
            <EvidenceRow icon={<History className="h-4 w-4" />} label="Replay verification" value={verified ? "PASS · links and hashes recomputed" : "PENDING"} tone={verified ? "success" : "warning"} />
            <EvidenceRow icon={<CheckCircle2 className="h-4 w-4" />} label="Latest checkpoint" value={`sequence ${String(data.snapshot.durability.checkpoint_sequence ?? "--")}`} tone="accent" />
            <EvidenceRow icon={<Link2 className="h-4 w-4" />} label="Event head" value={shortHash(String(data.snapshot.durability.event_head_hash ?? "--"), 24)} tone="default" />
            <EvidenceRow icon={<Hash className="h-4 w-4" />} label="Hash function" value="SHA-256 / canonical payload" tone="default" />
          </div>
        </div>
      </Panel>
    </div>
  );
}

function TimelineRow({ event, selected, onSelect }: { event: HarnessEvent; selected: boolean; onSelect: () => void }) {
  const isTool = ["TOOL_INVOKED", "TOOL_OBSERVED", "TOOLS_EXECUTED"].includes(event.event_type);
  return <Button variant="ghost" fullWidth onPress={onSelect} className={`event-row h-auto justify-start rounded-lg border px-3 py-3 text-left ${selected ? "event-row--selected" : ""}`}><span className={`event-row__index ${isTool ? "event-row__index--tool" : ""}`}>{String(event.sequence).padStart(2, "0")}</span><span className="min-w-0 flex-1"><span className="flex flex-wrap items-center gap-2"><span className="truncate text-xs font-semibold">{event.event_type}</span><StatusChip value={isTool ? "SUCCEEDED" : event.event_type === "TASK_COMPLETED" ? "COMPLETED" : "COMMITTED"} tone={isTool || event.event_type === "TASK_COMPLETED" ? "success" : "accent"} /></span><span className="mt-1 block truncate font-mono text-[10px] text-muted">{formatDate(event.created_at)} · v{event.task_version} · {shortHash(event.event_hash, 16)}</span></span><ChevronRight className="h-4 w-4 shrink-0 text-muted" /></Button>;
}

function EventInspector({ event }: { event: HarnessEvent }) {
  const tool = firstObject(event.payload.results) ?? asObject(event.payload.observation);
  return <div className="space-y-5"><div className="grid grid-cols-2 gap-2"><InspectorValue label="sequence" value={event.sequence} /><InspectorValue label="task version" value={event.task_version} /><InspectorValue label="created" value={formatDate(event.created_at)} /><InspectorValue label="type" value={event.event_type} /></div><div className="space-y-2"><p className="inline-flex items-center gap-2 text-xs font-semibold"><FileCode2 className="h-4 w-4 text-accent" />payload</p><JsonBlock value={event.payload} /></div>{tool ? <div className="rounded-lg border border-success/20 bg-success/5 p-3 text-xs"><div className="flex items-center gap-2 text-success"><Wrench className="h-4 w-4" /><span>{String(tool.tool ?? "tool")} · {String(tool.call_id ?? "call")}</span></div><p className="mt-2 font-mono text-[11px] text-muted">status={String(tool.status ?? "--")} attempts={String(tool.attempts ?? "--")}</p></div> : null}<div className="space-y-3 border-t border-border/70 pt-4"><HashLine label="previous_hash" value={event.previous_hash} /><HashLine label="event_hash" value={event.event_hash} /></div></div>;
}

function HashNode({ event, isLast }: { event: HarnessEvent; isLast: boolean }) {
  return <div className="hash-node"><div className={`hash-node__dot ${event.event_type === "TASK_COMPLETED" ? "hash-node__dot--done" : ""}`}><Check className="h-3 w-3" /></div><div className="min-w-0 flex-1"><div className="flex items-center justify-between gap-3"><span className="font-mono text-[11px]">#{event.sequence} {event.event_type}</span><span className="text-[10px] text-muted">v{event.task_version}</span></div><p className="mt-1 truncate font-mono text-[10px] text-muted">{shortHash(event.event_hash, 30)}</p></div>{!isLast ? <div className="hash-node__line" /> : null}</div>;
}

function EvidenceRow({ icon, label, value, tone }: { icon: React.ReactNode; label: string; value: string; tone: "success" | "accent" | "warning" | "default" }) {
  return <div className="evidence-row"><span className={`evidence-row__icon evidence-row__icon--${tone}`}>{icon}</span><span className="min-w-0"><span className="block text-xs font-medium">{label}</span><span className="mt-1 block truncate font-mono text-[11px] text-muted">{value}</span></span></div>;
}

function InspectorValue({ label, value }: { label: string; value: string | number }) {
  return <div className="rounded-lg bg-surface-tertiary px-3 py-2.5"><p className="text-[11px] text-muted">{label}</p><p className="mt-1 truncate font-mono text-xs">{value}</p></div>;
}

function HashLine({ label, value }: { label: string; value: string }) {
  return <div className="space-y-1"><p className="text-[11px] text-muted">{label}</p><p className="break-all font-mono text-[10px] leading-5">{value}</p></div>;
}

function EmptyTrajectory({ error, onRun }: { error: string; onRun: () => void }) {
  return <div className="space-y-6"><PageHeader eyebrow="SESSION / TRAJECTORY" title="Event timeline" description="等待 immutable event journal。" /><InlineError message={error || "尚未加载 session"} /><Button variant="primary" onPress={onRun}><RefreshCw className="h-4 w-4" />读取 fixture</Button></div>;
}

function firstObject(value: unknown): JsonObject | null {
  const first = Array.isArray(value) ? value[0] : undefined;
  return first && typeof first === "object" ? first as JsonObject : null;
}

function asObject(value: unknown): JsonObject | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : null;
}
