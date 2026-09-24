import {
  Activity,
  Boxes,
  BrainCircuit,
  CheckCircle2,
  ChevronRight,
  FileClock,
  GitBranch,
  Layers3,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Wrench,
} from "lucide-react";
import type { ComponentType, ReactNode } from "react";
import { Button, Chip } from "@heroui/react";

import type { HarnessViewKey } from "./HarnessWorkbenchView";
import type { ModelProviderStatus } from "../types";
import { PageHeader, Panel } from "../components/Primitives";

type SettingsViewProps = {
  modelProvider: ModelProviderStatus | null;
  onNavigate: (view: HarnessViewKey) => void;
};

const diagnosticLinks: Array<{ view: HarnessViewKey; title: string; detail: string; icon: ComponentType<{ className?: string }> }> = [
  { view: "workbench", title: "当前运行", detail: "本次 session 的状态、模型轮次和最终结果", icon: Activity },
  { view: "trajectory", title: "事件轨迹", detail: "按顺序查看事件日志和状态变更", icon: FileClock },
  { view: "context", title: "上下文诊断", detail: "查看本轮上下文组装和 token 估算（只读）", icon: Layers3 },
  { view: "tools", title: "工具与 MCP", detail: "查看工具 schema、权限和协议握手", icon: Wrench },
  { view: "skills", title: "技能与记忆", detail: "查看注入的技能版本和记忆记录", icon: Sparkles },
  { view: "subagents", title: "子 Agent", detail: "查看并行分工和结果汇总契约", icon: BrainCircuit },
  { view: "dag", title: "长任务 DAG", detail: "查看多阶段交付、返工和 UAT", icon: GitBranch },
  { view: "evaluation", title: "评测与护栏", detail: "查看质量指标和晋级约束", icon: ShieldCheck },
];

export function SettingsView({ modelProvider, onNavigate }: SettingsViewProps) {
  const remote = modelProvider?.mode === "remote";
  const configured = remote && modelProvider?.configured;

  return <div className="settings-page space-y-7">
    <PageHeader eyebrow="应用设置" title="设置" description="这里放模型接入、工作区权限和 Harness 高级诊断。日常使用只需要回到聊天。" />

    <div className="grid gap-5 xl:grid-cols-2">
      <Panel eyebrow="MODEL API" title="模型接入状态" description="API key 由 Go 服务端环境变量读取，浏览器只显示状态。">
        <div className={`settings-provider ${configured ? "settings-provider--remote" : remote ? "settings-provider--warning" : ""}`}>
          <div className="flex items-start gap-3">
            <span className="settings-provider__icon"><Boxes className="h-4 w-4" /></span>
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-center gap-2"><strong>{configured ? "OpenAI-compatible API" : remote ? "远程 API 配置不完整" : "本地离线适配器"}</strong><Chip size="sm" variant="soft" color={configured ? "success" : remote ? "danger" : "default"}><Chip.Label>{configured ? "已接入" : remote ? "需配置" : "离线"}</Chip.Label></Chip></div>
              <p className="mt-1 text-xs leading-5 text-muted">{modelProvider?.detail ?? "正在读取 Go 服务端状态"}</p>
              {configured ? <p className="mt-2 truncate font-mono text-[10px] text-muted">{modelProvider?.model} · {modelProvider?.base_url}</p> : null}
            </div>
          </div>
          <div className="mt-4 rounded-lg border border-border/70 bg-surface-tertiary/60 p-3 text-xs leading-5 text-muted"><SlidersHorizontal className="mr-2 inline h-3.5 w-3.5 text-accent" />通过 `HARNESS_MODEL_PROVIDER`、`HARNESS_MODEL_BASE_URL`、`HARNESS_MODEL_API_KEY` 和 `HARNESS_MODEL_NAME` 配置，重启 Go 服务后生效。</div>
        </div>
      </Panel>

      <Panel eyebrow="WORKSPACE" title="工作区执行边界" description="Agent 只在服务端绑定的 workspace root 内读取和运行白名单测试。">
        <div className="space-y-3">
          <SettingLine icon={<CheckCircle2 className="h-4 w-4 text-success" />} title="读取范围" value="workspace root" />
          <SettingLine icon={<Wrench className="h-4 w-4 text-warning" />} title="允许测试" value="go test / go vet / npm typecheck" />
          <SettingLine icon={<ShieldCheck className="h-4 w-4 text-accent" />} title="权限" value="workspace:read · workspace:write" />
        </div>
      </Panel>
    </div>

    <Panel eyebrow="高级诊断" title="Harness 内部页面" description="这些页面用于开发、排障和项目复盘，不参与普通聊天流程。">
      <div className="settings-diagnostic-grid">
        {diagnosticLinks.map(({ view, title, detail, icon: Icon }) => <Button key={view} variant="ghost" onPress={() => onNavigate(view)} className="settings-diagnostic-link h-auto justify-start rounded-lg border border-border/70 px-3 py-3 text-left">
          <span className="settings-diagnostic-link__icon"><Icon className="h-4 w-4" /></span>
          <span className="min-w-0 flex-1"><strong className="block text-xs font-semibold">{title}</strong><span className="mt-1 block text-[11px] font-normal leading-5 text-muted">{detail}</span></span>
          <ChevronRight className="h-4 w-4 shrink-0 text-muted" />
        </Button>)}
      </div>
    </Panel>
  </div>;
}

function SettingLine({ icon, title, value }: { icon: ReactNode; title: string; value: string }) {
  return <div className="flex items-center gap-3 rounded-lg border border-border/70 px-3 py-2.5"><span className="shrink-0">{icon}</span><span className="min-w-0 flex-1 text-xs text-muted">{title}</span><span className="max-w-[65%] truncate text-right font-mono text-[10px]">{value}</span></div>;
}
