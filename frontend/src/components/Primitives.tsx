import type { ReactNode } from "react";
import { AlertCircle } from "lucide-react";
import { Card, Chip } from "@heroui/react";

export type Tone = "default" | "accent" | "success" | "warning" | "danger";

export function formatDate(value?: string) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

export function shortHash(value?: string, length = 12) {
  if (!value) return "--";
  if (value.length <= length) return value;
  return value.slice(0, length) + "...";
}

const statusLabels: Record<string, string> = {
  COMPLETED: "已完成",
  SUCCEEDED: "已通过",
  RUNNING: "运行中",
  PENDING: "待处理",
  READY: "就绪",
  LEASED: "已租约",
  WAITING_USER: "等待用户",
  WAITING_APPROVAL: "等待审批",
  APPROVED: "已批准",
  APPROVAL_GRANTED: "审批通过",
  FAILED: "失败",
  BLOCKED: "已阻塞",
  REWORK: "返工",
  REJECTED: "已拒绝",
  CANCELLED: "已取消",
  UNKNOWN: "未知结果",
};

const successValues = new Set([
  "COMPLETED",
  "SUCCEEDED",
  "APPROVED",
  "APPROVAL_GRANTED",
  "PASS",
]);
const warningValues = new Set([
  "RUNNING",
  "PENDING",
  "READY",
  "LEASED",
  "WAITING_USER",
  "WAITING_APPROVAL",
  "UNKNOWN",
]);
const dangerValues = new Set([
  "FAILED",
  "BLOCKED",
  "REJECTED",
  "CANCELLED",
  "REWORK",
]);

export function statusTone(value?: string): Tone {
  const normalized = String(value ?? "").toUpperCase();
  if (successValues.has(normalized)) return "success";
  if (dangerValues.has(normalized)) return "danger";
  if (warningValues.has(normalized)) return "warning";
  return "default";
}

export function statusLabel(value?: string) {
  const normalized = String(value ?? "").toUpperCase();
  return statusLabels[normalized] ?? value ?? "未知";
}

export function StatusChip({ value, tone }: { value?: string; tone?: Tone }) {
  const resolvedTone = tone ?? statusTone(value);
  return (
    <Chip color={resolvedTone} variant="soft" size="sm">
      <Chip.Label>{statusLabel(value)}</Chip.Label>
    </Chip>
  );
}

export function Panel({
  title,
  eyebrow,
  description,
  action,
  children,
  className = "",
  variant = "secondary",
}: {
  title: string;
  eyebrow?: string;
  description?: string;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
  variant?: "default" | "secondary" | "tertiary" | "transparent";
}) {
  return (
    <Card variant={variant} className={"panel " + className}>
      <Card.Header className="flex-row items-start justify-between gap-4">
        <div className="min-w-0">
          {eyebrow ? (
            <p className="mb-1 text-[11px] font-medium tracking-[0.12em] text-muted">
              {eyebrow}
            </p>
          ) : null}
          <Card.Title>{title}</Card.Title>
          {description ? <Card.Description>{description}</Card.Description> : null}
        </div>
        {action ? <div className="shrink-0">{action}</div> : null}
      </Card.Header>
      <Card.Content>{children}</Card.Content>
    </Card>
  );
}

export function PageHeader({
  eyebrow,
  title,
  description,
  action,
}: {
  eyebrow: string;
  title: string;
  description: string;
  action?: ReactNode;
}) {
  return (
    <div className="mb-7 flex flex-col gap-5 md:flex-row md:items-end md:justify-between">
      <div className="min-w-0">
        <p className="mb-2 text-[11px] font-semibold tracking-[0.16em] text-accent">
          {eyebrow}
        </p>
        <h1 className="text-2xl font-semibold tracking-normal text-foreground md:text-3xl">
          {title}
        </h1>
        <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">{description}</p>
      </div>
      {action ? <div className="flex shrink-0 items-center gap-2">{action}</div> : null}
    </div>
  );
}

export function InlineError({ message }: { message?: string }) {
  if (!message) return null;
  return (
    <div className="flex items-start gap-3 rounded-xl border border-danger/30 bg-danger-soft px-4 py-3 text-sm text-danger-soft-foreground">
      <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" aria-hidden="true" />
      <span className="break-words">{message}</span>
    </div>
  );
}

export function LoadingState({ label = "正在读取运行状态" }: { label?: string }) {
  return (
    <div className="flex min-h-32 items-center justify-center gap-3 rounded-xl border border-dashed border-border bg-surface/60 px-4 text-sm text-muted">
      <span className="h-2 w-2 animate-pulse rounded-full bg-accent" aria-hidden="true" />
      {label}
    </div>
  );
}

export function JsonBlock({ value }: { value: unknown }) {
  return <pre className="code-panel p-4">{JSON.stringify(value, null, 2)}</pre>;
}
