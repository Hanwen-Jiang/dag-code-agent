import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ComponentType, ReactNode } from "react";
import {
  Activity,
  Boxes,
  BrainCircuit,
  CheckCircle2,
  Gauge,
  GitBranch,
  GitCommitHorizontal,
  Layers3,
  PanelLeft,
  Settings2,
  ShieldCheck,
  Sparkles,
  Moon,
  Sun,
  Wrench,
} from "lucide-react";
import { Button, Chip, Tooltip } from "@heroui/react";
import { AppLayout } from "@heroui-pro/react/app-layout";
import { Sidebar } from "@heroui-pro/react/sidebar";

import { api } from "./api";
import type { HarnessDemoResponse, HarnessRunRequest, HarnessSessionSummary, ModelProviderStatus } from "./types";
import { CapabilityView } from "./views/CapabilityView";
import { ContextInspectorView } from "./views/ContextInspectorView";
import { DagView } from "./views/DagView";
import { HarnessWorkbenchView, type HarnessViewKey } from "./views/HarnessWorkbenchView";
import { TrajectoryView } from "./views/TrajectoryView";
import { ChatView } from "./views/ChatView";
import { SettingsView } from "./views/SettingsView";

export type ViewKey = HarnessViewKey;

type NavItem = {
  key: ViewKey;
  label: string;
  description: string;
  href: `#${ViewKey}`;
  icon: ComponentType<{ className?: string }>;
  group: "session" | "runtime" | "delivery" | "settings";
};

const navItems: NavItem[] = [
  { key: "chat", label: "聊天", description: "和 Code Agent 对话", href: "#chat", icon: BrainCircuit, group: "session" },
  { key: "settings", label: "设置", description: "模型、工作区和高级诊断", href: "#settings", icon: Settings2, group: "settings" },
  { key: "workbench", label: "当前运行", description: "查看本次 session 状态", href: "#workbench", icon: Activity, group: "session" },
  { key: "trajectory", label: "事件轨迹", description: "按顺序回放运行记录", href: "#trajectory", icon: GitCommitHorizontal, group: "session" },
  { key: "context", label: "上下文诊断", description: "模型本轮看到的内容", href: "#context", icon: Layers3, group: "runtime" },
  { key: "tools", label: "工具与 MCP", description: "Agent 可以调用什么", href: "#tools", icon: Wrench, group: "runtime" },
  { key: "skills", label: "技能与记忆", description: "可复用知识和历史", href: "#skills", icon: Sparkles, group: "runtime" },
  { key: "subagents", label: "子 Agent", description: "并行分工与汇总", href: "#subagents", icon: BrainCircuit, group: "runtime" },
  { key: "dag", label: "长任务 DAG", description: "多阶段交付和验收", href: "#dag", icon: GitBranch, group: "delivery" },
  { key: "evaluation", label: "评测与护栏", description: "质量、延迟和晋级门", href: "#evaluation", icon: Gauge, group: "delivery" },
];

const primaryNavItems = navItems.filter((item) => item.key === "chat" || item.key === "settings");

export function App() {
  const [apiOnline, setApiOnline] = useState<boolean | null>(null);
  const [modelProvider, setModelProvider] = useState<ModelProviderStatus | null>(null);
  const [view, setView] = useState<ViewKey>(() => viewFromHash(window.location.hash));
  const [theme, setTheme] = useState<"light" | "dark">(() => (localStorage.getItem("dag-code-agent-theme") as "light" | "dark" | null) ?? "light");
  const [demo, setDemo] = useState<HarnessDemoResponse | null>(null);
  const [sessions, setSessions] = useState<HarnessSessionSummary[]>([]);
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);
  const [demoLoading, setDemoLoading] = useState(true);
  const [demoError, setDemoError] = useState("");
  const activeTaskRef = useRef<string | null>(null);
  const toggleTheme = useCallback(() => setTheme((current) => current === "dark" ? "light" : "dark"), []);

  useEffect(() => {
    activeTaskRef.current = activeTaskId;
  }, [activeTaskId]);

  const loadDemo = useCallback(async () => {
    setDemoLoading(true);
    setDemoError("");
    try {
      setDemo(await api.harnessDemo());
      setApiOnline(true);
    } catch (cause) {
      setDemoError(cause instanceof Error ? cause.message : "Harness demo 读取失败");
      setApiOnline(false);
    } finally {
      setDemoLoading(false);
    }
  }, []);

  const loadSessions = useCallback(async () => {
    setDemoLoading(true);
    setDemoError("");
    try {
      const listing = await api.harnessSessions();
      setSessions(listing.sessions);
      const preferredTaskId = activeTaskRef.current;
      const nextTaskId = preferredTaskId && listing.sessions.some((session) => session.task_id === preferredTaskId)
        ? preferredTaskId
        : listing.sessions[0]?.task_id;
      if (nextTaskId) {
        setDemo(await api.harnessRun(nextTaskId));
        setActiveTaskId(nextTaskId);
      } else {
        setDemo(null);
        setActiveTaskId(null);
      }
      setApiOnline(true);
    } catch (cause) {
      setDemoError(cause instanceof Error ? cause.message : "Harness session 读取失败");
      setApiOnline(false);
    } finally {
      setDemoLoading(false);
    }
  }, []);

  const createRun = useCallback(async (input: HarnessRunRequest) => {
    setDemoLoading(true);
    setDemoError("");
    try {
      const result = await api.createHarnessRun(input);
      setDemo(result);
      setActiveTaskId(result.task_id);
      const listing = await api.harnessSessions();
      setSessions(listing.sessions);
      setApiOnline(true);
    } catch (cause) {
      setDemoError(cause instanceof Error ? cause.message : "Harness run 启动失败");
      setApiOnline(false);
      throw cause;
    } finally {
      setDemoLoading(false);
    }
  }, []);

  const sendMessage = useCallback(async (message: string) => {
    const taskID = activeTaskRef.current;
    if (!taskID) return;
    setDemoLoading(true);
    setDemoError("");
    try {
      const result = await api.resumeHarnessRun(taskID, message);
      setDemo(result);
      setSessions((await api.harnessSessions()).sessions);
      setApiOnline(true);
    } catch (cause) {
      setDemoError(cause instanceof Error ? cause.message : "消息发送失败");
      setApiOnline(false);
      throw cause;
    } finally {
      setDemoLoading(false);
    }
  }, []);

  const selectSession = useCallback(async (taskId: string) => {
    setDemoLoading(true);
    setDemoError("");
    try {
      setDemo(await api.harnessRun(taskId));
      setActiveTaskId(taskId);
    } catch (cause) {
      setDemoError(cause instanceof Error ? cause.message : "Harness session 读取失败");
    } finally {
      setDemoLoading(false);
    }
  }, []);

  const newChat = useCallback(() => {
    setDemo(null);
    setActiveTaskId(null);
    setDemoError("");
  }, []);

  useEffect(() => {
    let active = true;
    api.health().then(() => active && setApiOnline(true)).catch(() => active && setApiOnline(false));
    api.modelStatus().then((status) => active && setModelProvider(status)).catch(() => active && setModelProvider(null));
    void loadSessions();
    return () => { active = false; };
  }, [loadSessions]);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
    localStorage.setItem("dag-code-agent-theme", theme);
  }, [theme]);

  useEffect(() => {
    const syncHash = () => setView(viewFromHash(window.location.hash));
    window.addEventListener("hashchange", syncHash);
    return () => window.removeEventListener("hashchange", syncHash);
  }, []);

  // The event stream is the UI's live projection boundary. Every durable
  // lifecycle event triggers a fresh materialized snapshot; reconnecting uses
  // the last sequence already rendered in the timeline.
  useEffect(() => {
    if (!activeTaskId || apiOnline !== true) return;
    const cursor = demo?.events.at(-1)?.sequence ?? 0;
    const stream = new EventSource(api.harnessEventsURL(activeTaskId, cursor));
    const refresh = () => {
      void api.harnessRun(activeTaskId).then((next) => {
        setDemo(next);
        if (["COMPLETED", "FAILED", "CANCELLED"].includes(next.status)) stream.close();
      }).catch(() => undefined);
    };
    const eventTypes = [
      "TASK_ACCEPTED", "PLAN_COMPILED", "TURN_STARTED", "STEP_STARTED", "CONTEXT_COMPILED",
      "MODEL_REQUESTED", "MODEL_PROPOSED", "TOOL_INVOKED", "TOOL_OBSERVED", "TOOLS_EXECUTED",
      "SUBAGENTS_REQUESTED", "VERIFIER_REJECTED", "USER_GATE_OPENED", "USER_GATE_RESUMED", "USER_MESSAGE_RECEIVED",
      "STEP_COMPLETED", "TURN_COMPLETED", "TASK_COMPLETED", "TASK_FAILED", "TASK_CANCELLED",
    ];
    eventTypes.forEach((eventType) => stream.addEventListener(eventType, refresh));
    stream.onerror = () => stream.close();
    return () => stream.close();
  }, [activeTaskId, apiOnline, demo?.events.at(-1)?.sequence]);

  const navigate = useCallback((href: string) => {
    if (href.startsWith("#")) {
      const nextView = viewFromHash(href);
      setView(nextView);
      window.history.replaceState(null, "", href);
      window.scrollTo({ top: 0, behavior: "smooth" });
    }
  }, []);

  const goTo = useCallback((next: ViewKey) => navigate(`#${next}`), [navigate]);
  const activeItem = navItems.find((item) => item.key === view) ?? navItems[0];
  const header = useMemo(() => <TopBar apiOnline={apiOnline} modelProvider={modelProvider} active={activeItem} theme={theme} onToggleTheme={toggleTheme} />, [activeItem, apiOnline, modelProvider, theme, toggleTheme]);
  const chatView = <ChatView data={demo} modelProvider={modelProvider} loading={demoLoading} error={demoError} sessions={sessions} activeTaskId={activeTaskId} onCreateRun={createRun} onSendMessage={sendMessage} onSelectSession={selectSession} onNewChat={newChat} onOpenAdvanced={() => goTo("settings")} theme={theme} onToggleTheme={toggleTheme} />;

  if (view === "chat") {
    return <div className="app-shell chat-home-shell min-h-screen text-foreground">{chatView}</div>;
  }

  return (
    <div className="app-shell min-h-screen text-foreground">
      <AppLayout
        className="harness-app-layout min-h-screen"
        sidebar={<HarnessSidebar apiOnline={apiOnline} active={view} onNavigate={goTo} />}
        navbar={header}
        navigate={navigate}
        sidebarCollapsible="icon"
        sidebarDefaultSize={23}
        sidebarMinSize={18}
        sidebarMaxSize={30}
        toggleShortcut="mod+b"
        scrollMode="page"
      >
        <div className="content-layer min-h-screen px-4 pb-14 pt-8 sm:px-6 lg:px-10 lg:pt-10">
          <div className="mx-auto w-full max-w-[1480px]">
            {view === "settings" ? <SettingsView modelProvider={modelProvider} onNavigate={goTo} /> : null}
            {view === "workbench" ? <HarnessWorkbenchView data={demo} loading={demoLoading} error={demoError} sessions={sessions} activeTaskId={activeTaskId} onCreateRun={createRun} onSelectSession={selectSession} onRun={loadDemo} onNavigate={goTo} /> : null}
            {view === "trajectory" ? <TrajectoryView data={demo} loading={demoLoading} error={demoError} onRun={loadDemo} onNavigate={goTo} /> : null}
            {view === "context" ? <ContextInspectorView data={demo} loading={demoLoading} error={demoError} onNavigate={goTo} /> : null}
            {view === "dag" ? <DagView /> : null}
            {view === "tools" || view === "skills" || view === "subagents" || view === "evaluation" ? <CapabilityView data={demo} loading={demoLoading} error={demoError} focus={view} onNavigate={goTo} /> : null}
          </div>
        </div>
      </AppLayout>
    </div>
  );
}

function HarnessSidebar({ apiOnline, active, onNavigate }: { apiOnline: boolean | null; active: ViewKey; onNavigate: (view: ViewKey) => void }) {
  return (
    <Sidebar className="harness-sidebar">
      <Sidebar.Header className="px-4 py-5">
        <div className="flex items-center gap-3"><div className="brand-mark" aria-hidden="true"><Boxes className="h-5 w-5" /><span className="brand-mark__dot" /></div><div className="min-w-0 flex-1"><p className="truncate text-sm font-semibold tracking-wide">Agent Harness</p><p className="truncate text-[11px] text-muted">dag-code-agent / Go runtime</p></div><Sidebar.Trigger aria-label="收起导航" /></div>
      </Sidebar.Header>
      <Sidebar.Content className="px-3">
        <NavGroup label="主工作区" items={primaryNavItems} active={active} onNavigate={onNavigate} />
        <Sidebar.Group className="mt-7"><Sidebar.GroupLabel>运行边界</Sidebar.GroupLabel><div className="space-y-2 rounded-xl border border-border/70 bg-surface-secondary/55 p-3"><BoundaryRow icon={<BrainCircuit className="h-3.5 w-3.5 text-accent" />} label="模型" value="只提议" /><BoundaryRow icon={<Wrench className="h-3.5 w-3.5 text-warning" />} label="工具副作用" value="策略门" /><BoundaryRow icon={<ShieldCheck className="h-3.5 w-3.5 text-success" />} label="证据" value="哈希链" /></div></Sidebar.Group>
      </Sidebar.Content>
      <Sidebar.Footer className="space-y-3 px-4 py-4"><div className="flex items-center gap-2 text-[11px] text-muted"><span className={`status-dot ${apiOnline ? "text-success" : apiOnline === false ? "text-danger" : "text-warning"}`} /><span>{apiOnline ? "Go API 在线" : apiOnline === false ? "Go API 离线" : "正在检查 Go API"}</span></div><div className="flex items-center gap-2 text-[11px] text-muted"><CheckCircle2 className="h-3.5 w-3.5 text-success" /><span>SQLite 持久化 / 可回放</span></div></Sidebar.Footer>
    </Sidebar>
  );
}

function NavGroup({ label, items, active, onNavigate }: { label: string; items: NavItem[]; active: ViewKey; onNavigate: (view: ViewKey) => void }) {
  return <Sidebar.Group className="mt-3 first:mt-0"><Sidebar.GroupLabel>{label}</Sidebar.GroupLabel><Sidebar.Menu aria-label={label} showGuideLines={false}>{items.map((item) => { const Icon = item.icon; return <Sidebar.MenuItem key={item.key} href={item.href} isCurrent={active === item.key} onPress={() => onNavigate(item.key)} textValue={item.label} tooltipProps={{ content: item.label, placement: "right" }}><Sidebar.MenuIcon><Icon className="h-4 w-4" /></Sidebar.MenuIcon><Sidebar.MenuLabel><span className="block truncate text-sm">{item.label}</span><span className="mt-0.5 block truncate text-[11px] font-normal text-muted">{item.description}</span></Sidebar.MenuLabel>{item.key === "workbench" && active === "workbench" ? <Sidebar.MenuChip>LIVE</Sidebar.MenuChip> : null}</Sidebar.MenuItem>; })}</Sidebar.Menu></Sidebar.Group>;
}

function BoundaryRow({ icon, label, value }: { icon: ReactNode; label: string; value: string }) { return <div className="flex items-center justify-between gap-3 text-[11px]"><span className="inline-flex items-center gap-2 text-muted">{icon}{label}</span><span className="font-mono text-foreground/80">{value}</span></div>; }

function TopBar({ apiOnline, modelProvider, active, theme, onToggleTheme }: { apiOnline: boolean | null; modelProvider: ModelProviderStatus | null; active: NavItem; theme: "light" | "dark"; onToggleTheme: () => void }) {
  const providerLabel = !modelProvider ? "模型状态读取中" : modelProvider.mode === "remote" && modelProvider.configured ? `${modelProvider.provider} · ${modelProvider.model}` : "离线模型";
  const providerColor = !modelProvider ? "warning" : modelProvider.mode === "remote" && modelProvider.configured ? "success" : modelProvider.mode === "remote" ? "danger" : "default";
  const isDiagnostic = active.key !== "chat" && active.key !== "settings";
  const groupLabel = isDiagnostic ? "设置 / 高级诊断" : active.group === "settings" ? "设置" : "会话";
  return <header className="harness-topbar flex h-16 items-center justify-between gap-3 border-b border-border/80 px-4 sm:px-6 lg:px-8"><div className="flex min-w-0 items-center gap-3"><AppLayout.MenuToggle tooltip="打开导航"><PanelLeft className="h-4 w-4" /></AppLayout.MenuToggle><div className="hidden min-w-0 sm:block"><p className="truncate text-[11px] font-semibold tracking-[0.14em] text-accent">{isDiagnostic ? groupLabel : `${groupLabel} / HARNESS`}</p><p className="truncate text-sm font-medium">{active.label}</p></div><Chip size="sm" variant="soft" color="default" className="hidden md:flex"><Chip.Label className="font-mono text-[11px]">runtime v0.2</Chip.Label></Chip></div><div className="flex items-center gap-2"><Chip size="sm" variant="soft" color={apiOnline ? "success" : apiOnline === false ? "danger" : "warning"} className="hidden sm:flex"><Chip.Label className="inline-flex items-center gap-1.5"><span className="status-dot" />{apiOnline ? "Go API 在线" : apiOnline === false ? "Go API 离线" : "连接中"}</Chip.Label></Chip><Chip size="sm" variant="soft" color={providerColor} className="hidden lg:flex"><Chip.Label>{providerLabel}</Chip.Label></Chip><span className={`status-dot sm:hidden ${apiOnline ? "text-success" : apiOnline === false ? "text-danger" : "text-warning"}`} aria-label={apiOnline ? "Go API 在线" : apiOnline === false ? "Go API 离线" : "连接中"} /><ThemeToggle theme={theme} onToggle={onToggleTheme} /></div></header>;
}

function viewFromHash(hash: string): ViewKey {
  const candidate = hash.replace(/^#/, "") as ViewKey;
  return navItems.some((item) => item.key === candidate) ? candidate : "chat";
}

export default App;

function ThemeToggle({ theme, onToggle }: { theme: "light" | "dark"; onToggle: () => void }) {
  return <div className="theme-toggle"><Tooltip><Tooltip.Trigger><Button isIconOnly size="sm" variant="ghost" aria-label={theme === "dark" ? "切换浅色主题" : "切换深色主题"} onPress={onToggle}>{theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}</Button></Tooltip.Trigger><Tooltip.Content>{theme === "dark" ? "浅色主题" : "深色主题"}</Tooltip.Content></Tooltip></div>;
}
