import type { Account, AccountAvailabilityKind } from './api';

const timeFormatter = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

const shortTimeFormatter = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
});

export function formatDateTime(value?: string | number | null) {
  if (value == null || value === '') return '-';
  const date = typeof value === 'number' ? new Date(value) : new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return timeFormatter.format(date).replace(/\//g, '-');
}

export function formatShortDateTime(value?: string | number | null) {
  if (value == null || value === '') return '-';
  const date = typeof value === 'number' ? new Date(value) : new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return shortTimeFormatter.format(date).replace(/\//g, '-');
}

export function formatEpochSeconds(value?: number | string | null) {
  if (value == null || value === '') return '无重置时间';
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return '无重置时间';
  return formatDateTime(parsed * 1000);
}

export function formatRelativeFromMs(value?: number | null) {
  if (!value) return '-';
  const minutes = Math.max(0, Math.round((Date.now() - value) / 60000));
  if (minutes < 60) return `${minutes} 分钟前`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours} 小时前`;
  return `${Math.round(hours / 24)} 天前`;
}

export function accountStatusText(value: string) {
  const labels: Record<string, string> = {
    ready: '可用',
    active: '可用',
    ok: '可用',
    disabled: '已停用',
    error: '异常',
    banned: '异常',
    running: '处理中',
    pending: '等待中',
    unknown: '未知',
  };
  return labels[value] || '未知';
}

export function requestStatusText(value: string) {
  const labels: Record<string, string> = {
    ok: '成功',
    error: '失败',
    running: '处理中',
    failed: '失败',
    cancelled: '已停止',
  };
  return labels[value] || '未知';
}

export function jobStatusText(value: string) {
  const labels: Record<string, string> = {
    running: '处理中',
    completed: '已完成',
    cancelled: '已停止',
    failed: '失败',
    pending: '等待中',
  };
  return labels[value] || '未知';
}

export function eventTypeText(value: string) {
  const labels: Record<string, string> = {
    progress: '处理中',
    success: '成功',
    skipped: '已跳过',
    failed: '失败',
    error: '失败',
    waiting: '等待中',
    cancelled: '已停止',
    done: '已完成',
    close: '已结束',
  };
  return labels[value] || '更新';
}

export function providerText(value?: string | null) {
  const labels: Record<string, string> = {
    anthropic: 'Anthropic',
    google: 'Google',
    openai: 'OpenAI',
    windsurf: 'Windsurf',
  };
  return labels[(value || '').toLowerCase()] || '其他';
}

export function availabilityText(kind?: AccountAvailabilityKind) {
  const labels: Record<AccountAvailabilityKind, string> = {
    available: '可用',
    probing: '恢复中',
    account_rate_limited: '账号暂时不可用',
    model_rate_limited: '模型暂时不可用',
    rpm_full: '调用过于频繁',
    tier_expired: '套餐不可用',
    model_blocked: '模型已停用',
    credential_missing: '需要补充信息',
    concurrency_full: '执行中',
    status_error: '账号异常',
    status_disabled: '已停用',
    status_banned: '可能封禁',
    status_unavailable: '不可用',
  };
  return kind ? labels[kind] : '可用';
}

export function primaryAccountState(account: Account) {
  if (account.availability?.kind === 'credential_missing' || (!account.credentialMask && !account.apiKey)) {
    return { label: '需要补充信息', className: 'status-error' };
  }
  if (account.status === 'disabled') {
    return { label: '已停用', className: 'status-disabled' };
  }
  if (account.status === 'banned' || account.status === 'error') {
    return { label: '账号异常', className: 'status-error' };
  }
  if (account.rateLimited || account.availability?.kind === 'account_rate_limited') {
    return { label: '暂时不可用', className: 'status-running' };
  }
  if (account.availability?.kind === 'model_rate_limited' || Object.keys(account.modelRateLimits || {}).length > 0) {
    return { label: '部分模型不可用', className: 'status-running' };
  }
  if (account.errorCount > 0 || account.lastError) {
    return { label: '最近失败', className: 'status-error' };
  }
  if (['ready', 'active', 'ok'].includes(account.status)) {
    return { label: '可用', className: '' };
  }
  return { label: accountStatusText(account.status), className: 'status-disabled' };
}

export function accountIssueText(account: Account) {
  if (account.availability?.upstreamError) return account.availability.upstreamError;
  if (account.lastError) return account.lastError;
  if (account.availability?.kind && account.availability.kind !== 'available') return availabilityText(account.availability.kind);
  if (account.errorCount > 0) return `最近失败 ${account.errorCount} 次`;
  return '无';
}
