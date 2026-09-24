import { useCallback, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import {
  AlertTriangle,
  Archive,
  Check,
  CheckCircle2,
  ChevronRight,
  CircleDot,
  FileCheck2,
  FileJson2,
  GitBranch,
  KeyRound,
  Play,
  RefreshCw,
  RotateCw,
  ShieldCheck,
  UserRoundCheck,
  X,
} from "lucide-react";
import { Button } from "@heroui/react";
import { DataGrid } from "@heroui-pro/react/data-grid";
import type { DataGridColumn } from "@heroui-pro/react/data-grid";
import { KPI } from "@heroui-pro/react/kpi";
import { KPIGroup } from "@heroui-pro/react/kpi-group";
import { Segment } from "@heroui-pro/react/segment";
import { Stepper } from "@heroui-pro/react/stepper";

import { api } from "../api";
import type {
  DagArtifact,
  DagAttempt,
  DagManifest,
  DagNode,
  DagReport,
  HarnessEvent,
} from "../types";
import {
  InlineError,
  JsonBlock,
  LoadingState,
  PageHeader,
  Panel,
  StatusChip,
  formatDate,
  shortHash,
  statusLabel,
} from "../components/Primitives";

type Scenario = "happy" | "rework" | "crash";
type EvidenceTab = "attempts" | "artifacts" | "events";

const mainPath = [
  "N-DESIGN",
  "N-IMPLEMENT-V1",
  "N-VERIFY-V1",
  "N-INTEGRATE",
  "N-SYSTEM-VERIFY",
  "N-UAT-PREP",
  "N-UAT",
];

const scenarioCopy: Record<Scenario, { label: string; title: string; detail: string; tone: "success" | "warning" | "accent" }> = {
  happy: {
    label: "Happy path",
    title: "一次通过技术验收",
    detail: "按 Canonical Manifest 顺序执行，完成后停在 UAT gate。",
    tone: "success",
  },
  rework: {
    label: "Rework branch",
    title: "保留失败并创建 fresh attempt",
    detail: "N-VERIFY-V1 失败两次，进入 diagnosis 与 V2 验证，不覆盖 V1 证据。",
    tone: "warning",
  },
  crash: {
    label: "Crash recovery",
    title: "从提交点恢复同一 run",
    detail: "模拟进程崩溃，再由 executor 读取 durable event 恢复未完成节点。",
    tone: "accent",
  },
};

export function DagView() {
  const [manifest, setManifest] = useState<DagManifest | null>(null);
  const [report, setReport] = useState<DagReport | null>(null);
  const [scenario, setScenario] = useState<Scenario>("happy");
  const [lastScenario, setLastScenario] = useState<Scenario | null>(null);
  const [selectedNode, setSelectedNode] = useState("");
  const [evidenceTab, setEvidenceTab] = useState<EvidenceTab>("attempts");
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [recoveredFromCrash, setRecoveredFromCrash] = useState(false);

  const loadManifest = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const value = await api.dagManifest();
      setManifest(value);
      setSelectedNode((current) => current || value.nodes[0]?.node_id || "");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "DAG manifest 读取失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadManifest();
  }, [loadManifest]);

  const start = async () => {
    setRunning(true);
    setError("");
    try {
      const response = await api.startDag(scenario);
      setReport(response.report);
      setLastScenario(response.scenario);
      setRecoveredFromCrash(response.recovered_from_crash);
      document.querySelector("#run-overview")?.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "DAG 执行失败");
    } finally {
      setRunning(false);
    }
  };

  const refresh = async () => {
    if (!report?.run_id) return;
    setRunning(true);
    setError("");
    try {
      setReport(await api.getDagRun(report.run_id));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "运行报告读取失败");
    } finally {
      setRunning(false);
    }
  };

  const decide = async (outcome: "PASS" | "REJECT") => {
    if (!report?.run_id) return;
    setRunning(true);
    setError("");
    try {
      setReport(await api.decideDag(report.run_id, outcome, "uat-user"));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "用户 Gate 提交失败");
    } finally {
      setRunning(false);
    }
  };

  const selected = manifest?.nodes.find((node) => node.node_id === selectedNode) ?? manifest?.nodes[0];
  const pathNodes = useMemo(
    () => (manifest ? mainPath.map((id) => manifest.nodes.find((node) => node.node_id === id)).filter(Boolean) as DagNode[] : []),
    [manifest],
  );
  const currentStep = report ? resolveCurrentStep(report) : 0;
  const selectedScenario = scenarioCopy[lastScenario ?? scenario];

  if (loading) return <LoadingState label="正在读取 Canonical Manifest" />;

  if (!manifest) {
    return (
      <div className="space-y-6">
        <PageHeader eyebrow="LONG-HORIZON / DELIVERY" title="Long-horizon DAG" description="读取可执行 manifest 后才能启动运行。" />
        <InlineError message={error || "Manifest 不可用"} />
        <Button variant="outline" onPress={() => void loadManifest()} isPending={loading}>
          <RotateCw className="h-4 w-4" />
          重新读取 manifest
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-7">
      <PageHeader
        eyebrow="Long-horizon / Delivery"
        title="Long-horizon DAG"
        description={manifest.plan.objective ?? "以 manifest、lease、attempt、artifact、evidence 和 UAT gate 驱动一次可恢复交付。"}
        action={
          <div className="flex flex-wrap items-center gap-2">
            {report ? (
              <Button variant="ghost" onPress={() => void refresh()} isPending={running}>
                <RefreshCw className="h-4 w-4" />
                刷新报告
              </Button>
            ) : null}
            <Button variant="primary" onPress={() => void start()} isPending={running}>
              <Play className="h-4 w-4" />
              {report ? "重新运行" : "启动 DAG"}
            </Button>
          </div>
        }
      />
      <InlineError message={error} />

      <section id="run-overview" className="scroll-mt-24 space-y-5">
        <Panel
          eyebrow="Run overview"
          title="执行场景"
          description="每个场景都由 Go executor 真实提交状态、attempt、artifact 和 evidence，浏览器只负责控制面操作。"
          action={report ? <StatusChip value={report.status} /> : <StatusChip value="READY" />}
        >
          <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(320px,0.8fr)] xl:items-center">
            <div className="space-y-4">
              <Segment selectedKey={scenario} onSelectionChange={(key) => setScenario(String(key) as Scenario)} className="w-fit" size="sm">
                {(Object.keys(scenarioCopy) as Scenario[]).map((key) => (
                  <Segment.Item key={key} id={key}>{scenarioCopy[key].label}</Segment.Item>
                ))}
              </Segment>
              <div className="flex items-start gap-3">
                <span className={`scenario-icon scenario-icon--${selectedScenario.tone}`}><CircleDot className="h-4 w-4" /></span>
                <div className="min-w-0">
                  <p className="text-sm font-semibold">{selectedScenario.title}</p>
                  <p className="mt-1 max-w-xl text-sm leading-6 text-muted">{selectedScenario.detail}</p>
                </div>
              </div>
            </div>
            <div className="grid grid-cols-3 gap-2 border-l-0 border-border/70 xl:border-l xl:pl-6">
              <RunStat label="nodes" value={manifest.nodes.length} />
              <RunStat label="joins" value={manifest.edges.filter((edge) => edge.gating).length} />
              <RunStat label="revision" value={manifest.plan.revision ?? "--"} />
            </div>
          </div>
        </Panel>

        <KPIGroup className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
          <KPI>
            <KPI.Header><KPI.Title>Run status</KPI.Title><KPI.Icon status={report?.status === "WAITING_USER" ? "warning" : "success"}><GitBranch className="h-4 w-4" /></KPI.Icon></KPI.Header>
            <KPI.Content>{report ? <StatusChip value={report.status} /> : <span className="text-sm font-medium text-muted">Ready</span>}</KPI.Content>
            <KPI.Footer><span className="font-mono text-[11px]">{report?.run_id ?? manifest.manifest_id}</span></KPI.Footer>
          </KPI>
          <CountKpi label="attempts" value={report?.attempts.length ?? 0} icon={<RefreshCw className="h-4 w-4" />} />
          <CountKpi label="artifacts" value={report?.artifacts.length ?? 0} icon={<Archive className="h-4 w-4" />} />
          <CountKpi label="evidence" value={report?.evidence.length ?? 0} icon={<FileCheck2 className="h-4 w-4" />} />
          <CountKpi label="events" value={report?.events.length ?? 0} icon={<KeyRound className="h-4 w-4" />} />
        </KPIGroup>

        {recoveredFromCrash ? (
          <div className="inline-flex items-center gap-2 rounded-lg border border-accent/25 bg-accent/10 px-3 py-2 text-xs text-accent">
            <CheckCircle2 className="h-4 w-4" />
            Crash recovery 已从 durable commits 恢复同一 run
          </div>
        ) : null}
      </section>

      <section id="execution-graph" className="scroll-mt-24 space-y-5">
        <Panel eyebrow="Control path" title="主执行链" description="Stepper 展示 canonical path；失败分支会在节点图和 attempt ledger 中保留 provenance。">
          <div className="dag-scroll overflow-x-auto pb-2">
            <Stepper currentStep={currentStep} size="sm" className="min-w-[900px]">
              {pathNodes.map((node, index) => {
                const state = report?.node_states[node.node_id] ?? "PENDING";
                return (
                  <Stepper.Step key={node.node_id}>
                    <Stepper.Indicator><Stepper.Icon>{index + 1}</Stepper.Icon></Stepper.Indicator>
                    <Stepper.Content>
                      <Stepper.Title>{shortTitle(node.title)}</Stepper.Title>
                      <Stepper.Description>{statusLabel(state)}</Stepper.Description>
                    </Stepper.Content>
                    <Stepper.Separator />
                  </Stepper.Step>
                );
              })}
            </Stepper>
          </div>
          <div className="mt-5 flex flex-wrap items-center gap-2 text-xs text-muted">
            <span>Lifecycle</span><ChevronRight className="h-3.5 w-3.5" />
            <span className="inline-flex items-center gap-1.5"><span className="status-dot text-success" />technical verification</span>
            <ChevronRight className="h-3.5 w-3.5" />
            <span className="inline-flex items-center gap-1.5"><span className="status-dot text-warning" />UAT gate</span>
            <ChevronRight className="h-3.5 w-3.5" />
            <span className="inline-flex items-center gap-1.5"><span className="status-dot text-accent" />completed</span>
          </div>
        </Panel>

        <div className="grid gap-5 xl:grid-cols-[minmax(0,1.25fr)_minmax(340px,0.75fr)]">
          <Panel title="Execution graph" eyebrow="Node graph" description="选择一个节点，查看 executor、gate、retry 和写入边界。">
            <div className="grid gap-2.5 sm:grid-cols-2">
              {manifest.nodes.map((node) => {
                const state = report?.node_states[node.node_id] ?? "PENDING";
                const active = node.node_id === selected?.node_id;
                return (
                  <Button
                    key={node.node_id}
                    fullWidth
                    variant="ghost"
                    onPress={() => setSelectedNode(node.node_id)}
                    className={`node-tile h-auto min-h-[128px] justify-start rounded-xl border p-4 text-left ${active ? "node-tile--active" : ""}`}
                  >
                    <div className="flex w-full min-w-0 flex-col gap-3">
                      <div className="flex items-start justify-between gap-3">
                        <span className="font-mono text-[11px] text-muted">{node.node_id}</span>
                        <StatusChip value={state} />
                      </div>
                      <p className="line-clamp-2 text-sm font-medium leading-5">{node.title}</p>
                      <div className="flex min-w-0 items-center justify-between gap-3 text-[11px] text-muted">
                        <span className="truncate">{node.work_type}</span>
                        <span className="truncate font-mono">{node.executor_role}</span>
                      </div>
                    </div>
                  </Button>
                );
              })}
            </div>
            <div className="mt-5 flex flex-wrap items-center gap-2 border-t border-border/70 pt-4 text-[11px] text-muted">
              <span>Failure edges</span>
              {manifest.edges.filter((edge) => edge.condition?.predicate?.value === "FAILED").map((edge) => (
                <span key={edge.edge_id} className="inline-flex items-center gap-1 rounded-md bg-danger-soft px-2 py-1 text-danger-soft-foreground">
                  <AlertTriangle className="h-3 w-3" />{edge.from_node} → {edge.to_node}
                </span>
              ))}
            </div>
          </Panel>

          <Panel title={selected?.node_id ?? "Node inspector"} eyebrow="Selected node" description={selected?.title ?? "选择一个节点查看详情"}>
            {selected ? <NodeInspector node={selected} state={report?.node_states[selected.node_id] ?? "PENDING"} report={report} /> : <LoadingState label="无节点" />}
          </Panel>
        </div>
      </section>

      {report?.status === "WAITING_USER" ? (
        <section className="scroll-mt-24">
          <Panel eyebrow="User acceptance gate" title="等待用户验收" description={`当前节点 ${report.waiting_node_id ?? "N-UAT"} 需要用户决定后才会提交最终状态。`}>
            <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-start gap-3 text-sm">
                <UserRoundCheck className="mt-0.5 h-5 w-5 shrink-0 text-warning" />
                <span className="max-w-2xl leading-6 text-muted">用户结果会写入 evidence，并继续执行完成分支。拒绝会保留当前 run 和已提交证据。</span>
              </div>
              <div className="flex shrink-0 gap-2">
                <Button variant="danger-soft" onPress={() => void decide("REJECT")} isPending={running}><X className="h-4 w-4" />拒绝</Button>
                <Button variant="primary" onPress={() => void decide("PASS")} isPending={running}><Check className="h-4 w-4" />通过验收</Button>
              </div>
            </div>
          </Panel>
        </section>
      ) : null}

      <section id="evidence-ledger" className="scroll-mt-24 space-y-5">
        <Panel eyebrow="Evidence ledger" title="运行证据" description="将 attempt、artifact、event 放在同一个可排序 ledger 中，保留 hash 与 producer provenance。" action={report ? <StatusChip value={`${report.events.length} events`} tone="default" /> : null}>
          {!report ? (
            <EmptyLedger />
          ) : (
            <>
              <Segment selectedKey={evidenceTab} onSelectionChange={(key) => setEvidenceTab(String(key) as EvidenceTab)} size="sm" className="mb-4 w-fit">
                <Segment.Item id="attempts">Attempts</Segment.Item>
                <Segment.Item id="artifacts">Artifacts</Segment.Item>
                <Segment.Item id="events">Events</Segment.Item>
              </Segment>
              <Ledger tab={evidenceTab} report={report} />
            </>
          )}
        </Panel>
      </section>

      <section id="manifest-provenance" className="scroll-mt-24">
        <Panel eyebrow="Manifest provenance" title="Contract identity" description="当前运行以冻结的 Canonical Manifest 为唯一计划来源。">
          <div className="grid gap-6 lg:grid-cols-[minmax(0,0.9fr)_minmax(360px,1.1fr)]">
            <div className="space-y-3 text-sm">
              <Provenance label="Manifest" value={manifest.manifest_id} />
              <Provenance label="Schema" value={manifest.schema_version} />
              <Provenance label="Spec hash" value={shortHash(manifest.spec_hash, 44)} />
              <Provenance label="Plan" value={`${manifest.plan.plan_id ?? "--"} · revision ${manifest.plan.revision ?? "--"}`} />
              <Provenance label="Status" value={manifest.plan.status ?? "--"} />
            </div>
            <div className="min-w-0">
              <div className="mb-3 flex items-center gap-2 text-xs font-medium text-muted"><FileJson2 className="h-4 w-4 text-accent" />Requirements & policy snapshot</div>
              <JsonBlock value={{ requirements: manifest.plan.requirements?.map((item) => item.title ?? item.requirement_id), constraints: manifest.plan.constraints, concurrency_policy: manifest.plan.concurrency_policy }} />
            </div>
          </div>
        </Panel>
      </section>
    </div>
  );
}

function Ledger({ tab, report }: { tab: EvidenceTab; report: DagReport }) {
  if (tab === "attempts") {
    const columns: DataGridColumn<DagAttempt>[] = [
      { id: "node", header: "Node", accessorKey: "node_id", isRowHeader: true, allowsSorting: true, width: "1.4fr", minWidth: 180 },
      { id: "attempt", header: "Attempt", accessorKey: "attempt_id", allowsSorting: true, cell: (row) => <span className="font-mono text-[11px]">{shortHash(row.attempt_id, 18)}</span>, width: "1.1fr", minWidth: 140 },
      { id: "state", header: "State", accessorKey: "state", allowsSorting: true, cell: (row) => <StatusChip value={row.state} />, width: 120 },
      { id: "ordinal", header: "#", accessorKey: "ordinal", allowsSorting: true, align: "end", width: 56 },
      { id: "started", header: "Started", accessorKey: "started_at", cell: (row) => <span className="text-[11px] text-muted">{formatDate(row.started_at)}</span>, width: 120 },
    ];
    return <DataGrid aria-label="DAG attempts" data={report.attempts} columns={columns} getRowId={(row) => row.attempt_id} variant="secondary" contentClassName="min-w-[680px]" scrollContainerClassName="max-h-[340px] overflow-auto" />;
  }

  if (tab === "artifacts") {
    const columns: DataGridColumn<DagArtifact>[] = [
      { id: "contract", header: "Contract", accessorKey: "contract_id", isRowHeader: true, allowsSorting: true, width: "1.3fr", minWidth: 180 },
      { id: "producer", header: "Producer", accessorKey: "producer_node_id", allowsSorting: true, width: "1.1fr", minWidth: 160 },
      { id: "media", header: "Media type", accessorKey: "media_type", width: 140 },
      { id: "sha", header: "SHA-256", accessorKey: "sha256", cell: (row) => <span className="font-mono text-[11px] text-muted">{shortHash(row.sha256, 24)}</span>, width: "1.2fr", minWidth: 190 },
    ];
    return <DataGrid aria-label="DAG artifacts" data={report.artifacts} columns={columns} getRowId={(row) => row.artifact_id} variant="secondary" contentClassName="min-w-[680px]" scrollContainerClassName="max-h-[340px] overflow-auto" />;
  }

  const columns: DataGridColumn<HarnessEvent>[] = [
    { id: "sequence", header: "#", accessorKey: "sequence", allowsSorting: true, align: "end", width: 56 },
    { id: "event", header: "Event", accessorKey: "event_type", isRowHeader: true, allowsSorting: true, width: "1.25fr", minWidth: 180 },
    { id: "payload", header: "Payload", accessorKey: "payload", cell: (row) => <span className="block max-w-[420px] truncate text-[11px] text-muted">{summarizePayload(row.payload)}</span>, width: "1.8fr", minWidth: 240 },
    { id: "hash", header: "Hash", accessorKey: "event_hash", cell: (row) => <span className="font-mono text-[11px] text-muted">{shortHash(row.event_hash, 18)}</span>, width: "1fr", minWidth: 140 },
    { id: "created", header: "Created", accessorKey: "created_at", cell: (row) => <span className="text-[11px] text-muted">{formatDate(row.created_at)}</span>, width: 120 },
  ];
  return <DataGrid aria-label="DAG events" data={report.events} columns={columns} getRowId={(row) => `${row.sequence}-${row.event_hash}`} variant="secondary" contentClassName="min-w-[820px]" scrollContainerClassName="max-h-[340px] overflow-auto" />;
}

function EmptyLedger() {
  return (
    <div className="flex min-h-[180px] flex-col items-center justify-center rounded-xl border border-dashed border-border/80 bg-surface-secondary/40 px-6 text-center">
      <KeyRound className="h-6 w-6 text-muted" />
      <p className="mt-4 text-sm font-medium">Run a scenario to populate the ledger</p>
      <p className="mt-1 max-w-md text-xs leading-5 text-muted">执行后可以在这里查看 attempt ordinal、artifact contract、event sequence 和 hash chain。</p>
    </div>
  );
}

function NodeInspector({ node, state, report }: { node: DagNode; state: string; report: DagReport | null }) {
  const policy = node.execution_policy as Record<string, unknown>;
  const retry = (policy.retry_policy as Record<string, unknown> | undefined) ?? {};
  const gate = node.gate_policy as Record<string, unknown>;
  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between gap-3"><span className="text-sm text-muted">Current state</span><StatusChip value={state} /></div>
      <div className="grid grid-cols-2 gap-2">
        <InspectorMetric label="Input contracts" value={node.input_contracts.length} />
        <InspectorMetric label="Output contracts" value={node.output_contracts.length} />
        <InspectorMetric label="Max attempts" value={Number(retry.max_attempts ?? 1)} />
        <InspectorMetric label="Gate mode" value={String(gate.mode ?? "NONE")} />
      </div>
      <div className="space-y-3 text-xs">
        <DetailLine label="executor" value={node.executor_role} />
        <DetailLine label="work type" value={node.work_type} />
        <DetailLine label="tool profile" value={typeof node.tool_profile === "string" ? node.tool_profile : JSON.stringify(node.tool_profile ?? "--")} />
        <DetailLine label="write scope" value={JSON.stringify(node.write_scope ?? {})} />
      </div>
      <div className="border-t border-border/70 pt-4 text-xs text-muted">
        {report ? <>{report.attempts.filter((item) => item.node_id === node.node_id).length} attempts · {report.artifacts.filter((item) => item.producer_node_id === node.node_id).length} artifacts · {report.evidence.filter((item) => item.producer_node_id === node.node_id).length} evidence</> : "No run evidence yet"}
      </div>
    </div>
  );
}

function CountKpi({ label, value, icon }: { label: string; value: number; icon: ReactNode }) {
  return (
    <KPI>
      <KPI.Header><KPI.Title>{label}</KPI.Title><KPI.Icon>{icon}</KPI.Icon></KPI.Header>
      <KPI.Content><KPI.Value value={value} /></KPI.Content>
      <KPI.Footer><span className="text-xs text-muted">current run</span></KPI.Footer>
    </KPI>
  );
}

function RunStat({ label, value }: { label: string; value: number | string }) {
  return <div className="min-w-0"><p className="font-mono text-lg font-semibold numeric">{value}</p><p className="mt-1 text-[11px] text-muted">{label}</p></div>;
}

function InspectorMetric({ label, value }: { label: string; value: number | string }) {
  return <div className="rounded-lg bg-surface-tertiary px-3 py-2.5"><p className="text-[11px] text-muted">{label}</p><p className="numeric mt-1 text-sm font-semibold">{value}</p></div>;
}

function DetailLine({ label, value }: { label: string; value: string }) {
  return <div className="flex items-start justify-between gap-3"><span className="shrink-0 text-muted">{label}</span><span className="max-w-[68%] break-words text-right font-mono text-[11px]">{value}</span></div>;
}

function Provenance({ label, value }: { label: string; value: string }) {
  return <div className="flex items-center justify-between gap-4 border-b border-border/70 pb-3 last:border-0 last:pb-0"><span className="text-muted">{label}</span><span className="max-w-[70%] break-all text-right font-mono text-xs">{value}</span></div>;
}

function shortTitle(value: string) {
  return value.replace(/^由用户验收/, "用户验收").replace(/^将通过验收的实现合入隔离集成分支$/, "集成").slice(0, 18);
}

function resolveCurrentStep(report: DagReport) {
  const index = mainPath.findIndex((id) => report.node_states[id] !== "SUCCEEDED");
  return index < 0 ? mainPath.length - 1 : index;
}

function summarizePayload(payload: Record<string, unknown>) {
  const entries = Object.entries(payload ?? {}).slice(0, 3);
  if (entries.length === 0) return "--";
  return entries.map(([key, value]) => `${key}: ${typeof value === "string" ? value : JSON.stringify(value)}`).join(" · ");
}
