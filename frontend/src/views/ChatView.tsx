import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import {
  ArrowUp,
  Bot,
  Check,
  CheckCircle2,
  ChevronRight,
  ChevronDown,
  Clipboard,
  FileCode2,
  FileText,
  FolderGit2,
  Moon,
  Plus,
  RotateCcw,
  Settings2,
  Sun,
  Terminal,
  ThumbsDown,
  ThumbsUp,
  User,
  X,
} from "lucide-react";
import { Button, Chip, Tooltip } from "@heroui/react";
import { ChainOfThought } from "@heroui-pro/react/chain-of-thought";
import { ChatAttachment, ChatAttachmentGroup } from "@heroui-pro/react/chat-attachment";
import { ChatConversation } from "@heroui-pro/react/chat-conversation";
import { ChatListView } from "@heroui-pro/react/chat-list-view";
import { ChatLoader } from "@heroui-pro/react/chat-loader";
import { ChatMessage } from "@heroui-pro/react/chat-message";
import { ChatMessageActions } from "@heroui-pro/react/chat-message-actions";
import { ChatSource } from "@heroui-pro/react/chat-source";
import { ChatTool } from "@heroui-pro/react/chat-tool";
import { CodeBlock } from "@heroui-pro/react/code-block";
import { Markdown } from "@heroui-pro/react/markdown";
import { PromptInput } from "@heroui-pro/react/prompt-input";
import { PromptSuggestion } from "@heroui-pro/react/prompt-suggestion";
import { TextShimmer } from "@heroui-pro/react/text-shimmer";

import type { HarnessDemoResponse, HarnessEvent, HarnessRunRequest, HarnessSessionSummary, ModelProviderStatus } from "../types";
import { InlineError, LoadingState, StatusChip, formatDate } from "../components/Primitives";

type ChatViewProps = {
  data: HarnessDemoResponse | null;
  modelProvider: ModelProviderStatus | null;
  sessions: HarnessSessionSummary[];
  activeTaskId: string | null;
  loading: boolean;
  error: string;
  onCreateRun: (input: HarnessRunRequest) => Promise<void>;
  onSendMessage: (message: string) => Promise<void>;
  onSelectSession: (taskId: string) => Promise<void>;
  onNewChat: () => void;
  onOpenAdvanced: () => void;
  theme: "light" | "dark";
  onToggleTheme: () => void;
};

type ProjectGroup = {
  id: string;
  name: string;
  sessions: HarnessSessionSummary[];
};

const DEFAULT_PROJECT_ID = "dag-code-agent";

type ChatTranscriptMessage = {
  id: string;
  role: "user" | "assistant" | "tool" | "system";
  content: string;
  label?: string;
  status?: string;
  timestamp?: string;
  toolName?: string;
  toolCallId?: string;
  toolInput?: unknown;
  toolValue?: unknown;
  sourcePath?: string;
};

export function ChatView({ data, modelProvider, sessions, activeTaskId, loading, error, onCreateRun, onSendMessage, onSelectSession, onNewChat, onOpenAdvanced, theme, onToggleTheme }: ChatViewProps) {
  const [draft, setDraft] = useState("");
  const [creating, setCreating] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [expandedProjects, setExpandedProjects] = useState<string[]>([DEFAULT_PROJECT_ID]);
  const [selectedProjectId, setSelectedProjectId] = useState(DEFAULT_PROJECT_ID);
  const messages = useMemo(() => buildMessages(data), [data]);
  const projects = useMemo(() => groupSessions(sessions), [sessions]);

  useEffect(() => {
    setExpandedProjects((current) => current.length > 0 ? current : projects.map((project) => project.id));
    if (!projects.some((project) => project.id === selectedProjectId)) {
      setSelectedProjectId(projects[0]?.id ?? DEFAULT_PROJECT_ID);
    }
  }, [projects, selectedProjectId]);

  async function submit() {
    const message = draft.trim();
    if (!message || loading || creating) return;
    setDraft("");
    if (!data) {
      setCreating(true);
      try {
        const project = projects.find((item) => item.id === selectedProjectId) ?? projects[0];
        await onCreateRun({ goal: message, project_id: project?.id ?? DEFAULT_PROJECT_ID, project_name: project?.name ?? "dag-code-agent", constraints: ["keep the response grounded in repository evidence"] });
      } finally {
        setCreating(false);
      }
      return;
    }
    await onSendMessage(message);
  }

  if (loading && !data && sessions.length === 0) return <LoadingState label="正在连接 Code Agent" />;

  return (
    <div className="chat-shell">
      <aside className="chat-sessions" aria-label="项目与会话">
        <div className="chat-sessions__header">
          <div>
            <p className="chat-kicker">WORKSPACE</p>
            <h1 className="chat-title">项目</h1>
          </div>
          <Button isIconOnly size="sm" variant="ghost" aria-label="新建会话" onPress={() => { setDraft(""); onNewChat(); }}>
            <Plus className="h-4 w-4" />
          </Button>
        </div>
        <div className="chat-project-list">
          {projects.map((project) => {
            const expanded = expandedProjects.includes(project.id);
            return <section key={project.id} className={`chat-project ${selectedProjectId === project.id ? "chat-project--selected" : ""}`}>
              <button type="button" className="chat-project__header" onClick={() => { setSelectedProjectId(project.id); setExpandedProjects((current) => current.includes(project.id) ? current.filter((item) => item !== project.id) : [...current, project.id]); }}>
                <FolderGit2 className="h-4 w-4 shrink-0" />
                <span className="min-w-0 flex-1 text-left"><span className="chat-project__name">{project.name}</span><span className="chat-project__meta">{project.sessions.length} 个 session</span></span>
                <ChevronDown className={`h-4 w-4 shrink-0 transition-transform ${expanded ? "rotate-0" : "-rotate-90"}`} />
              </button>
              {expanded ? <div className="chat-project__sessions">
                {project.sessions.length === 0 ? <p className="chat-empty">还没有 session</p> : <ChatListView<Record<string, unknown>> density="compact" aria-label={`${project.name} sessions`} className="chat-session-list">
                  {project.sessions.map((session) => (
                    <ChatListView.Item
                      key={session.task_id}
                      id={session.task_id}
                      textValue={session.goal}
                      className={activeTaskId === session.task_id ? "chat-session chat-session--active" : "chat-session"}
                      onAction={() => { setSelectedProjectId(project.id); void onSelectSession(session.task_id); }}
                    >
                      <ChatListView.Icon><span className="chat-session__icon"><FileCode2 className="h-4 w-4" /></span></ChatListView.Icon>
                      <ChatListView.ItemContent>
                        <ChatListView.Text>
                          <ChatListView.Title>{session.goal}</ChatListView.Title>
                          <ChatListView.Preview>{session.step} steps · {session.tool_calls} tool calls</ChatListView.Preview>
                        </ChatListView.Text>
                      </ChatListView.ItemContent>
                      <ChatListView.Meta><StatusChip value={session.status} /><ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted" /></ChatListView.Meta>
                    </ChatListView.Item>
                  ))}
                </ChatListView>}
              </div> : null}
            </section>;
          })}
        </div>
      </aside>

      <main className="chat-main">
        <header className="chat-header">
          <div className="flex min-w-0 items-center gap-3">
            <span className="chat-agent-mark"><Bot className="h-5 w-5" /></span>
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2"><h2 className="truncate text-sm font-semibold">DAG Code Agent</h2>{data ? <StatusChip value={data.status} /> : <Chip size="sm" variant="soft" color="default"><Chip.Label>新会话</Chip.Label></Chip>}<ProviderChip status={modelProvider} /></div>
              <p className="truncate text-xs text-muted">{projects.find((project) => project.id === selectedProjectId)?.name ?? "dag-code-agent"} · Go runtime</p>
            </div>
          </div>
          <div className="chat-header__actions">
            <Tooltip><Tooltip.Trigger><Button isIconOnly size="sm" variant="ghost" aria-label={settingsOpen ? "关闭设置" : "打开设置"} onPress={() => setSettingsOpen((current) => !current)}><Settings2 className="h-4 w-4" /></Button></Tooltip.Trigger><Tooltip.Content>{settingsOpen ? "关闭设置" : "打开设置"}</Tooltip.Content></Tooltip>
            <Tooltip><Tooltip.Trigger><Button isIconOnly size="sm" variant="ghost" aria-label={theme === "dark" ? "切换浅色主题" : "切换深色主题"} onPress={onToggleTheme}>{theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}</Button></Tooltip.Trigger><Tooltip.Content>{theme === "dark" ? "浅色主题" : "深色主题"}</Tooltip.Content></Tooltip>
          </div>
        </header>

        <ChatConversation className="chat-transcript chat-conversation" initial="instant" resize="smooth" aria-live="polite">
          <ChatConversation.Content className="chat-conversation__content">
            {!data ? <EmptyChat onSuggestion={setDraft} /> : <>
              {messages.map((message) => <MessageBubble key={message.id} message={message} />)}
              <ExecutionTrace data={data} loading={loading} />
            </>}
            {loading && data ? <div className="chat-thinking"><ChatLoader.Dots label="Agent 正在处理" size="sm" /><TextShimmer>Agent 正在处理工具调用</TextShimmer></div> : null}
            <ChatConversation.ScrollAnchor />
          </ChatConversation.Content>
        </ChatConversation>

        <div className="chat-composer-wrap">
          <InlineError message={error} />
          <PromptInput
            value={draft}
            onValueChange={setDraft}
            onSubmit={() => void submit()}
            status={loading || creating ? "submitted" : "ready"}
            className="chat-composer"
            maxHeight={180}
            aria-label="代码任务输入"
          >
            <PromptInput.Shell>
              <PromptInput.Content>
                <PromptInput.TextArea
                  placeholder={data ? "继续描述要检查、实现或修复的内容..." : "描述你要完成的代码任务..."}
                  aria-label="发送消息"
                  className="chat-composer__input"
                />
              </PromptInput.Content>
              <PromptInput.Toolbar>
                <PromptInput.ToolbarStart>
                  <span className="chat-composer__mode"><Terminal className="h-3.5 w-3.5" /> workspace evidence</span>
                </PromptInput.ToolbarStart>
                <PromptInput.ToolbarEnd>
                  <PromptInput.Send isIconOnly aria-label="发送" isDisabled={!draft.trim()} status={loading || creating ? "submitted" : "ready"}>
                    <ArrowUp className="h-4 w-4" />
                  </PromptInput.Send>
                </PromptInput.ToolbarEnd>
              </PromptInput.Toolbar>
            </PromptInput.Shell>
          </PromptInput>
        </div>
      </main>

      <aside className={`chat-context ${settingsOpen ? "chat-context--visible" : ""}`} aria-label={settingsOpen ? "设置" : "运行上下文"}>
        {settingsOpen ? <ChatSettingsPanel status={modelProvider} onClose={() => setSettingsOpen(false)} onOpenAdvanced={onOpenAdvanced} /> : <ChatRuntimePanel data={data} modelProvider={modelProvider} />}
      </aside>
    </div>
  );
}

function ProviderChip({ status }: { status: ModelProviderStatus | null }) {
  if (!status) return <Chip size="sm" variant="soft" color="warning"><Chip.Label>模型状态读取中</Chip.Label></Chip>;
  const remote = status.mode === "remote";
  const configured = remote && status.configured;
  return <Chip size="sm" variant="soft" color={configured ? "success" : remote ? "danger" : "default"}><Chip.Label>{configured ? `${status.provider} · ${status.model}` : remote ? "远程模型未配置" : "离线适配器"}</Chip.Label></Chip>;
}

function ProviderStatusCard({ status }: { status: ModelProviderStatus | null }) {
  const remote = status?.mode === "remote";
  const configured = remote && status?.configured;
  return <div className={`chat-provider-card ${configured ? "chat-provider-card--remote" : remote ? "chat-provider-card--warning" : ""}`}>
    <p className="chat-kicker">MODEL API</p>
    <div className="mt-2 flex items-center gap-2"><span className="chat-provider-card__dot" /><strong>{configured ? "已接入远程模型" : remote ? "远程模型未配置" : "当前使用离线模型"}</strong></div>
    <p className="mt-1 text-[11px] leading-5 text-muted">{status?.detail ?? "正在读取 Go 服务端配置"}</p>
    {configured ? <p className="mt-2 truncate font-mono text-[10px] text-muted">{status?.model} · {status?.base_url}</p> : remote ? <p className="mt-2 text-[10px] text-muted">API key 只在 Go 服务端读取，不会进入浏览器。</p> : <p className="mt-2 text-[10px] text-muted">设置 HARNESS_MODEL_PROVIDER、HARNESS_MODEL_BASE_URL、HARNESS_MODEL_API_KEY 后启用。</p>}
  </div>;
}

function ChatRuntimePanel({ data, modelProvider }: { data: HarnessDemoResponse | null; modelProvider: ModelProviderStatus | null }) {
  return <>
    <div className="chat-context__header"><div><p className="chat-kicker">SESSION</p><p className="chat-pane-title">运行状态</p></div><span className="status-dot text-success" /></div>
    <ProviderStatusCard status={modelProvider} />
    {data ? <>
      <div className="chat-context__stat"><span>轮次</span><strong>{numberValue(data.metrics.turns)}</strong></div>
      <div className="chat-context__stat"><span>模型步骤</span><strong>{numberValue(data.metrics.steps)}</strong></div>
      <div className="chat-context__stat"><span>工具调用</span><strong>{numberValue(data.metrics.tool_calls)}</strong></div>
      <div className="chat-context__divider" />
      <p className="chat-kicker">LATEST EVIDENCE</p>
      <div className="chat-evidence-list">
        {latestEvidence(data.events).map((item) => <div key={item.id} className="chat-evidence"><span className="chat-evidence__icon"><Check className="h-3.5 w-3.5" /></span><span className="min-w-0"><strong>{item.label}</strong><small>{item.detail}</small></span></div>)}
      </div>
    </> : <p className="mt-4 text-sm leading-6 text-muted">发送第一条消息后，这里会显示本次 session 的执行状态。</p>}
  </>;
}

function ChatSettingsPanel({ status, onClose, onOpenAdvanced }: { status: ModelProviderStatus | null; onClose: () => void; onOpenAdvanced: () => void }) {
  const configured = status?.mode === "remote" && status.configured;
  return <>
    <div className="chat-context__header"><div><p className="chat-kicker">WORKSPACE</p><p className="chat-pane-title">设置</p></div><Button isIconOnly size="sm" variant="ghost" aria-label="关闭设置" onPress={onClose}><X className="h-4 w-4" /></Button></div>
    <ProviderStatusCard status={status} />
    <div className="chat-settings-list">
      <div className="chat-setting-row"><span>模型</span><strong>{configured ? status?.model : "离线适配器"}</strong></div>
      <div className="chat-setting-row"><span>工作区</span><strong>dag-code-agent</strong></div>
      <div className="chat-setting-row"><span>执行范围</span><strong>workspace root</strong></div>
      <div className="chat-setting-row"><span>主题</span><strong>跟随顶部按钮</strong></div>
    </div>
    <Button variant="outline" fullWidth onPress={onOpenAdvanced}><Settings2 className="h-4 w-4" />高级诊断</Button>
  </>;
}

function groupSessions(sessions: HarnessSessionSummary[]): ProjectGroup[] {
  const grouped = new Map<string, ProjectGroup>();
  for (const session of sessions) {
    const id = session.project_id?.trim() || DEFAULT_PROJECT_ID;
    const name = session.project_name?.trim() || DEFAULT_PROJECT_ID;
    const group = grouped.get(id) ?? { id, name, sessions: [] };
    group.sessions.push(session);
    grouped.set(id, group);
  }
  if (grouped.size === 0) return [{ id: DEFAULT_PROJECT_ID, name: DEFAULT_PROJECT_ID, sessions: [] }];
  return Array.from(grouped.values());
}

function EmptyChat({ onSuggestion }: { onSuggestion: (value: string) => void }) {
  return <div className="chat-empty-state">
    <div className="chat-empty-state__intro"><span className="chat-empty-state__mark"><Bot className="h-7 w-7" /></span><span className="chat-empty-state__eyebrow">DAG CODE AGENT · GO HARNESS</span><h2>从一个代码任务开始</h2><p>描述目标、报错或需要检查的文件，Agent 会在同一个 session 中持续工作，并把工具证据留在对话里。</p></div>
    <PromptSuggestion variant="pill" className="chat-suggestions">
      <PromptSuggestion.Header><PromptSuggestion.Title>可以从这里开始</PromptSuggestion.Title></PromptSuggestion.Header>
      <PromptSuggestion.Items>
        <PromptSuggestion.Item onPress={() => onSuggestion("检查当前 Go 项目的测试状态，并给出失败原因。")}>检查测试状态<PromptSuggestion.ItemMeta><CheckCircle2 className="h-3.5 w-3.5" /></PromptSuggestion.ItemMeta></PromptSuggestion.Item>
        <PromptSuggestion.Item onPress={() => onSuggestion("阅读 README.md，总结这个 Harness 的核心运行链路。")}>总结运行链路<PromptSuggestion.ItemMeta><FileText className="h-3.5 w-3.5" /></PromptSuggestion.ItemMeta></PromptSuggestion.Item>
        <PromptSuggestion.Item onPress={() => onSuggestion("找出当前项目最值得先完善的一处，并说明验证方法。")}>寻找下一步改进<PromptSuggestion.ItemMeta><RotateCcw className="h-3.5 w-3.5" /></PromptSuggestion.ItemMeta></PromptSuggestion.Item>
      </PromptSuggestion.Items>
    </PromptSuggestion>
  </div>;
}

function MessageBubble({ message }: { message: ChatTranscriptMessage }) {
  if (message.role === "tool") return <ToolMessage message={message} />;
  const isUser = message.role === "user";
  const MessageRoot = isUser ? ChatMessage.User : ChatMessage.Assistant;
  return <MessageRoot className={`ai-chat-message ai-chat-message--${message.role}`}>
    <ChatMessage.Avatar alt={isUser ? "用户" : "Code Agent"} fallback={isUser ? "你" : "AI"}>
      {isUser ? <User className="h-4 w-4" /> : <Bot className="h-4 w-4" />}
    </ChatMessage.Avatar>
    <ChatMessage.Content className="ai-chat-message__content">
      <div className="ai-chat-message__meta"><span>{isUser ? "你" : message.role === "system" ? "Harness" : "Code Agent"}</span>{message.timestamp ? <time>{formatDate(message.timestamp)}</time> : null}</div>
      <ChatMessage.Bubble className="ai-chat-message__bubble">
        <ChatMessage.Body className="ai-chat-message__body">{isUser ? <p className="m-0 whitespace-pre-wrap">{message.content}</p> : <AssistantContent content={message.content} />}</ChatMessage.Body>
      </ChatMessage.Bubble>
      {!isUser ? <ChatMessageActions className="ai-chat-message__actions">
        <ChatMessageActions.Copy aria-label="复制回答" onPress={() => void navigator.clipboard?.writeText(message.content)}><Clipboard className="h-3.5 w-3.5" /></ChatMessageActions.Copy>
        <ChatMessageActions.Regenerate aria-label="重新生成"><RotateCcw className="h-3.5 w-3.5" /></ChatMessageActions.Regenerate>
        <ChatMessageActions.ThumbsUp aria-label="回答有帮助"><ThumbsUp className="h-3.5 w-3.5" /></ChatMessageActions.ThumbsUp>
        <ChatMessageActions.ThumbsDown aria-label="回答需要改进"><ThumbsDown className="h-3.5 w-3.5" /></ChatMessageActions.ThumbsDown>
      </ChatMessageActions> : null}
    </ChatMessage.Content>
  </MessageRoot>;
}

function ToolMessage({ message }: { message: ChatTranscriptMessage }) {
  const succeeded = message.status === "SUCCEEDED";
  const state = succeeded ? "output-available" : "output-error";
  const output = message.toolValue ?? message.content;
  return <div className="ai-tool-row">
    <div className="ai-tool-row__avatar"><Terminal className="h-4 w-4" /></div>
    <ChatTool
      className="ai-tool-card"
      state={state}
      toolName={message.toolName ?? message.label ?? "workspace tool"}
      toolCallId={message.toolCallId ?? message.id}
      input={message.toolInput}
      output={output}
      errorText={succeeded ? undefined : message.content}
    >
      <ChatTool.Trigger>
        <span className="ai-tool-trigger"><span className="ai-tool-trigger__name">{message.toolName ?? message.label ?? "workspace tool"}</span><span className="ai-tool-trigger__status">{succeeded ? "evidence committed" : message.status ?? "needs review"}</span></span>
      </ChatTool.Trigger>
      <ChatTool.Content>
        <ChatTool.Args input={message.toolInput} label="调用参数" />
        {succeeded ? <ChatTool.Result value={output} label="工具输出" /> : <ChatTool.Error errorText={message.content} label="工具错误" />}
        {message.sourcePath ? <div className="ai-tool-evidence">
          <ChatAttachmentGroup>
            <ChatAttachment name={message.sourcePath} mediaType="document" size={numberValue(objectValue(message.toolValue).bytes)}>
              <ChatAttachment.Icon><FileText className="h-4 w-4" /></ChatAttachment.Icon>
              <ChatAttachment.Info><ChatAttachment.Name /><ChatAttachment.Size /></ChatAttachment.Info>
            </ChatAttachment>
          </ChatAttachmentGroup>
          <ChatSource sourceType="document" title={message.sourcePath} description="来自 workspace tool 的持久化读取证据">
            <ChatSource.Trigger label={message.sourcePath}><ChatSource.Icon /><ChatSource.Title /></ChatSource.Trigger>
          </ChatSource>
        </div> : null}
      </ChatTool.Content>
    </ChatTool>
  </div>;
}

function AssistantContent({ content }: { content: string }) {
  const parts: ReactNode[] = [];
  const fence = /```([^\n]*)\n([\s\S]*?)```/g;
  let cursor = 0;
  let match: RegExpExecArray | null;
  let index = 0;
  while ((match = fence.exec(content)) !== null) {
    if (match.index > cursor) parts.push(<Markdown key={`md-${index++}`}>{content.slice(cursor, match.index)}</Markdown>);
    const language = match[1].trim() || "text";
    const code = match[2].replace(/\n$/, "");
    parts.push(<CodeBlock key={`code-${index++}`} className="ai-code-block"><CodeBlock.Header><span>{language}</span><CodeBlock.CopyButton code={code} aria-label="复制代码" /></CodeBlock.Header><CodeBlock.Code code={code} language={language} /></CodeBlock>);
    cursor = match.index + match[0].length;
  }
  if (cursor < content.length || parts.length === 0) parts.push(<Markdown key={`md-${index++}`}>{content.slice(cursor)}</Markdown>);
  return <div className="ai-markdown">{parts}</div>;
}

function ExecutionTrace({ data, loading }: { data: HarnessDemoResponse; loading: boolean }) {
  const toolCount = numberValue(data.metrics.tool_calls);
  const stepCount = numberValue(data.metrics.steps);
  const events = data.events.filter((event) => ["PLAN_COMPILED", "CONTEXT_COMPILED", "TOOL_OBSERVED", "TASK_COMPLETED", "TASK_FAILED"].includes(event.event_type));
  return <ChainOfThought className="ai-chain-of-thought" isStreaming={loading}>
    <ChainOfThought.Trigger><span>Harness execution trace</span><span className="ai-chain-of-thought__meta">{stepCount} steps · {toolCount} tools</span></ChainOfThought.Trigger>
    <ChainOfThought.Content>
      <ChainOfThought.Steps>
        <ChainOfThought.Step label="Plan">任务已进入 durable session，状态由 Go runtime 提交。</ChainOfThought.Step>
        {events.slice(-4).map((event) => <ChainOfThought.Step key={event.sequence} label={event.event_type.replaceAll("_", " ").toLowerCase()}>{event.event_type === "TOOL_OBSERVED" ? "工具返回了可回放的 observation。" : "控制面已提交这个生命周期节点。"}</ChainOfThought.Step>)}
      </ChainOfThought.Steps>
    </ChainOfThought.Content>
  </ChainOfThought>;
}

function buildMessages(data: HarnessDemoResponse | null): ChatTranscriptMessage[] {
  if (!data) return [];
  const messages: ChatTranscriptMessage[] = [{ id: "goal", role: "user", content: String(data.snapshot.request.goal ?? data.task_id) }];
  const toolCalls = new Map<string, Record<string, unknown>>();
  for (const event of data.events) {
    if (event.event_type === "TOOL_INVOKED") {
      const call = objectValue(event.payload.call);
      const callID = stringValue(call.call_id);
      if (callID) toolCalls.set(callID, call);
    }
    if (event.event_type === "USER_MESSAGE_RECEIVED") {
      messages.push({ id: `${event.sequence}-user`, role: "user", content: stringValue(event.payload.message), timestamp: event.created_at });
    }
    if (event.event_type === "MODEL_PROPOSED") {
      const response = objectValue(event.payload.response);
      const text = stringValue(response.text);
      if (text) messages.push({ id: `${event.sequence}-assistant`, role: "assistant", content: text, timestamp: event.created_at });
    }
    if (event.event_type === "TOOL_OBSERVED") {
      const observation = objectValue(event.payload.observation);
      const callID = stringValue(observation.call_id);
      const value = observation.value;
      const call = toolCalls.get(callID);
      const sourcePath = stringValue(objectValue(value).path);
      messages.push({ id: `${event.sequence}-tool`, role: "tool", label: stringValue(observation.tool) || "Tool", toolName: stringValue(observation.tool) || "workspace tool", toolCallId: callID, toolInput: call?.arguments, toolValue: value, sourcePath, content: toolSummary(observation), status: stringValue(observation.status), timestamp: event.created_at });
    }
    if (event.event_type === "TASK_FAILED") messages.push({ id: `${event.sequence}-failed`, role: "system", content: stringValue(event.payload.message) || "本轮执行失败", timestamp: event.created_at });
  }
  return messages;
}

function latestEvidence(events: HarnessEvent[]) {
  return events.filter((event) => event.event_type === "TOOL_OBSERVED" || event.event_type === "MODEL_PROPOSED").slice(-4).reverse().map((event) => {
    if (event.event_type === "TOOL_OBSERVED") {
      const observation = objectValue(event.payload.observation);
      return { id: String(event.sequence), label: stringValue(observation.tool) || "tool", detail: stringValue(observation.status) || "observation committed" };
    }
    return { id: String(event.sequence), label: "model proposal", detail: "captured in event log" };
  });
}

function toolSummary(observation: Record<string, unknown>) {
  const value = observation.value;
  if (typeof value === "string") return value;
  if (value !== undefined && value !== null) return JSON.stringify(value, null, 2);
  return stringValue(observation.error_code) || "no output";
}

function objectValue(value: unknown): Record<string, unknown> { return value && typeof value === "object" ? value as Record<string, unknown> : {}; }
function stringValue(value: unknown) { return typeof value === "string" ? value : value == null ? "" : String(value); }
function numberValue(value: unknown) { return typeof value === "number" ? value : Number(value ?? 0); }
