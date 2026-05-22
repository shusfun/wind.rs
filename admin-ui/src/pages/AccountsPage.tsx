import { FormEvent, Fragment, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import {
  Activity,
  Ban,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ClipboardList,
  KeyRound,
  MoreHorizontal,
  Play,
  Plus,
  RefreshCw,
  ShieldCheck,
  Square,
  X,
  Search,
} from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { SecretInput } from '../components/SecretInput';
import { StateBlock } from '../components/StateBlock';
import { StatusBadge } from '../components/StatusBadge';
import { Account, AdminEvent, LoginJob, LoginJobEvent, api, authHeaders, getToken } from '../lib/api';

type AddMode = 'password' | 'batch' | 'token' | 'apiKey';
type ProbeModelOption = { id: string; label?: string };

const addModes: Array<{ key: AddMode; label: string }> = [
  { key: 'password', label: '邮箱密码' },
  { key: 'batch', label: '批量导入' },
  { key: 'token', label: 'Auth Token' },
  { key: 'apiKey', label: 'API Key' },
];

function formatTime(value?: string | null) {
  return value ? new Date(value).toLocaleString() : '-';
}

function formatPercent(value?: number | null) {
  return typeof value === 'number' ? `${Math.max(0, Math.min(100, value)).toFixed(0)}%` : 'N/A';
}

function clampPercent(value?: number | null) {
  return typeof value === 'number' ? Math.max(0, Math.min(100, value)) : null;
}

function formatAgoFromMs(value?: number | null) {
  if (!value) return '-';
  const minutes = Math.max(0, Math.round((Date.now() - value) / 60000));
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function formatShortTime(value?: string | null) {
  if (!value) return '-';
  return new Date(value).toLocaleString([], { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function modelId(model: string | { id: string }) {
  return typeof model === 'string' ? model : model.id;
}

function modelLabel(model: string | { id: string; label?: string }) {
  return typeof model === 'string' ? model : model.label || model.id;
}

function normalizeProbeModels(models: ProbeModelOption[] | undefined, preferred: string): ProbeModelOption[] {
  const seen = new Set<string>();
  const next: ProbeModelOption[] = [];
  const add = (item?: ProbeModelOption | string) => {
    const id = typeof item === 'string' ? item : item?.id;
    if (!id || seen.has(id)) return;
    seen.add(id);
    next.push(typeof item === 'string' ? { id } : { id, label: item?.label });
  };
  add(preferred);
  (models || []).forEach((item) => add(item));
  return next;
}

function modeLabel(value?: string | null) {
  if (!value) return '-';
  const labels: Record<string, string> = {
    auth1: '邮箱密码',
    firebase: '邮箱密码',
    password: '邮箱密码',
    token: 'Auth Token',
    api_key: 'API Key',
  };
  return labels[value] || value;
}

function statusText(value: string) {
  const labels: Record<string, string> = {
    ready: '可用',
    active: '可用',
    ok: '可用',
    disabled: '已停用',
    error: '异常',
    banned: '异常',
    running: '处理中',
  };
  return labels[value] || value;
}

function availabilityText(account: Account) {
  const labels: Record<string, string> = {
    available: '可用',
    probing: '正在探测',
    account_rate_limited: '账号限流',
    model_rate_limited: '模型限流',
    rpm_full: '本分钟已满',
    tier_expired: '额度不可用',
    model_blocked: '模型不可用',
    credential_missing: '凭据不可用',
    concurrency_full: '执行中',
    status_error: '账号异常',
    status_disabled: '已停用',
    status_banned: '可能封禁',
    status_unavailable: '不可用',
  };
  const kind = account.availability?.kind || (account.rateLimited ? 'account_rate_limited' : modelLimitCount(account) ? 'model_rate_limited' : 'available');
  return labels[kind] || kind;
}

function availabilityClass(account: Account) {
  const kind = account.availability?.kind || 'available';
  if (kind === 'available') return 'availability-ok';
  if (kind === 'probing') return 'availability-probing';
  if (kind.includes('rate_limited') || kind === 'rpm_full' || kind === 'concurrency_full') return 'availability-warn';
  return 'availability-danger';
}

function availabilityDetail(account: Account) {
  const availability = account.availability;
  if (!availability) return account.rateLimited ? formatShortTime(account.rateLimitedUntil) : '调度可用';
  if (availability.available) return availability.kind === 'probing' ? '正在尝试恢复' : '调度可用';
  if (availability.retryAfterSecs > 0) return `${availability.retryAfterSecs}s 后再试`;
  return '暂不可用';
}

function tierText(value?: string | null) {
  const labels: Record<string, string> = {
    pro: 'PRO',
    free: 'FREE',
    expired: '已过期',
    unknown: '未知',
  };
  return labels[value || 'unknown'] || value || '未知';
}

function eventText(event: LoginJobEvent) {
  if (event.email) return event.email;
  if (event.message) return event.message;
  if (event.errorCode) return event.errorCode;
  if (event.reason) return event.reason;
  if (event.accountId) return `账号 ${event.accountId}`;
  return '-';
}

function eventStatusText(value?: string) {
  const labels: Record<string, string> = {
    running: '处理中',
    waiting: '等待中',
    progress: '处理中',
    success: '成功',
    failed: '失败',
    error: '失败',
    cancelled: '已停止',
    completed: '已完成',
    done: '已完成',
    normal: '正常等待',
    message: '消息',
  };
  return labels[value || ''] || value || '-';
}

function jobStatusText(value: string) {
  return eventStatusText(value);
}

function eventTitle(event: LoginJobEvent) {
  if (event.type === 'progress') return '开始处理';
  if (event.type === 'success') return '添加成功';
  if (event.type === 'failed') return '添加失败';
  if (event.type === 'waiting') return '等待下一次';
  return eventStatusText(event.type);
}

function eventMessage(event: LoginJobEvent) {
  if (event.type === 'waiting') {
    const reason = event.reason === 'failed' ? '失败后等待' : '成功后等待';
    return `${reason} ${event.seconds || 0} 秒`;
  }
  if (event.type === 'progress') return `正在处理 ${eventText(event)}`;
  if (event.type === 'success') return `${eventText(event)} 已添加`;
  if (event.type === 'failed') return `${eventText(event)} ${event.message || event.errorCode || '导入失败'}`;
  if (event.type === 'cancelled') return event.message || '任务已停止';
  if (event.type === 'done') return `成功 ${event.successCount || 0} 个，失败 ${event.failedCount || 0} 个`;
  return eventText(event);
}

function jobDoneCount(job?: LoginJob) {
  return (job?.successCount || 0) + (job?.failedCount || 0);
}

function jobProgress(job?: LoginJob) {
  if (!job || job.total <= 0) return 0;
  return Math.max(0, Math.min(100, Math.round((jobDoneCount(job) / job.total) * 100)));
}

function latestAccountEvent(events: LoginJobEvent[]) {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (event.email || event.emailMasked || event.accountId) return event;
  }
  return null;
}

function accountTitle(account: Account) {
  const label = account.label?.trim();
  if (label && label !== account.email) return label;
  return account.email;
}

function accountPlanName(account: Account) {
  const fromCredits = account.credits?.planName;
  if (fromCredits && fromCredits !== 'Unknown') return fromCredits;
  const fromStatus = account.userStatus?.planName || account.userStatus?.userStatus?.planStatus?.planInfo?.planName;
  return fromStatus && fromStatus !== 'Unknown' ? fromStatus : '';
}

function accountTrialEndMs(account: Account) {
  const direct = account.userStatus?.trialEndMs || account.credits?.trialEndMs;
  if (typeof direct === 'number' && direct > 0) return direct;
  const nested = account.userStatus?.userStatus?.windsurfProTrialEndTime;
  if (typeof nested === 'number') return nested > 1_000_000_000_000 ? nested : nested * 1000;
  if (typeof nested === 'string') {
    const parsed = Number(nested);
    return Number.isFinite(parsed) ? (parsed > 1_000_000_000_000 ? parsed : parsed * 1000) : null;
  }
  if (typeof nested === 'object' && nested?.seconds != null) {
    const seconds = Number(nested.seconds);
    return Number.isFinite(seconds) ? seconds * 1000 : null;
  }
  return null;
}

function tierSubline(account: Account) {
  const trialEndMs = accountTrialEndMs(account);
  if (trialEndMs && trialEndMs > Date.now()) {
    return `${Math.max(0, Math.ceil((trialEndMs - Date.now()) / 86400000))}d trial`;
  }
  const planName = accountPlanName(account);
  return planName.length > 12 ? `${planName.slice(0, 12)}...` : planName;
}

function modelLimitCount(account: Account) {
  return Object.keys(account.modelRateLimits || {}).length;
}

function modelStats(account: Account) {
  const tierModels = account.tierModels || [];
  const blockedCount = account.blockedModels?.length || 0;
  const total = tierModels.length;
  return {
    models: tierModels,
    total,
    blockedCount,
    availableCount: total > 0 ? Math.max(0, total - blockedCount) : 0,
  };
}

function creditResetText(value?: number | string | null) {
  if (value == null || value === '') return '无重置时间';
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return '无重置时间';
  return new Date(parsed * 1000).toLocaleString();
}

function creditTone(value?: number | null) {
  const pct = clampPercent(value);
  if (pct == null) return 'none';
  if (pct <= 10) return 'danger';
  if (pct <= 30) return 'warn';
  return 'success';
}

function CreditMeter({ label, value, resetAt }: { label: string; value?: number | null; resetAt?: number | string | null }) {
  const pct = clampPercent(value);
  const noData = pct == null;
  return (
    <div className={`credit-meter ${noData ? 'empty' : creditTone(pct)}`} title={noData ? `${label}额度未返回` : `${label}剩余 ${pct.toFixed(0)}%，重置时间：${creditResetText(resetAt)}`}>
      <span>{label}</span>
      <div className="credit-track">
        <i style={{ width: `${noData ? 0 : pct}%` }} />
      </div>
      <strong>{noData ? 'N/A' : formatPercent(pct)}</strong>
    </div>
  );
}

function CreditSummary({ account }: { account: Account }) {
  const credits = account.credits;
  if (!credits) return <span className="muted-text">未获取</span>;
  if (credits.lastError && credits.percent == null && !credits.prompt?.limit) {
    return <span className="credit-error" title={credits.lastError}>获取失败</span>;
  }
  const planName = credits.planName && credits.planName !== 'Unknown' ? credits.planName : accountPlanName(account) || '-';
  return (
    <div className="credit-summary">
      <div className="credit-plan-line">
        <strong title={planName}>{planName.length > 12 ? `${planName.slice(0, 12)}...` : planName}</strong>
        <span>{formatAgoFromMs(credits.fetchedAt)}</span>
      </div>
      <CreditMeter label="日" value={credits.dailyPercent} resetAt={credits.dailyResetAt} />
      <CreditMeter label="周" value={credits.weeklyPercent} resetAt={credits.weeklyResetAt} />
    </div>
  );
}

function useAdminEvents(enabled: boolean, onEvent: (event: AdminEvent) => void, onError?: (message: string) => void) {
  const onEventRef = useRef(onEvent);
  const onErrorRef = useRef(onError);

  useEffect(() => {
    onEventRef.current = onEvent;
    onErrorRef.current = onError;
  }, [onEvent, onError]);

  useEffect(() => {
    if (!enabled) return undefined;
    const controller = new AbortController();
    let cancelled = false;
    async function connect() {
      try {
        const resp = await fetch('/admin/events', {
          headers: authHeaders(),
          signal: controller.signal,
        });
        if (!resp.ok || !resp.body) {
          throw new Error(`实时连接失败：${resp.status}`);
        }
        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (!cancelled) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const parts = buffer.split('\n\n');
          buffer = parts.pop() || '';
          for (const part of parts) {
            const eventType = part.split('\n').find((line) => line.startsWith('event:'))?.slice(6).trim() || 'message';
            const dataLine = part.split('\n').find((line) => line.startsWith('data:'));
            if (!dataLine) continue;
            const raw = dataLine.slice(5).trim();
            if (!raw) continue;
            const payload = JSON.parse(raw) as AdminEvent;
            onEventRef.current({ ...payload, kind: payload.kind || eventType });
          }
        }
      } catch (err) {
        if (!cancelled && (err as Error).name !== 'AbortError') {
          onErrorRef.current?.(err instanceof Error ? err.message : '实时连接已断开');
        }
      }
    }
    connect();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [enabled]);
}

function useLoginJobs(enabled: boolean, onAccountsChanged: () => Promise<void>) {
  const [jobs, setJobs] = useState<LoginJob[]>([]);
  const [activeJobId, setActiveJobId] = useState('');
  const [events, setEvents] = useState<LoginJobEvent[]>([]);
  const [waiting, setWaiting] = useState<number | null>(null);
  const [jobError, setJobError] = useState('');

  async function loadJobs() {
    const data = await api.loginJobs();
    setJobs(data.jobs);
  }

  async function followJob(id: string) {
    setActiveJobId(id);
    setEvents([]);
    setWaiting(null);
    const resp = await fetch(`/admin/login-jobs/${id}/events`, {
      headers: { authorization: `Bearer ${getToken()}` },
    });
    if (!resp.body) return;

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split('\n\n');
      buffer = parts.pop() || '';
      for (const part of parts) {
        const eventType = part.split('\n').find((line) => line.startsWith('event:'))?.slice(6).trim() || 'message';
        const dataLine = part.split('\n').find((line) => line.startsWith('data:'));
        if (!dataLine) continue;
        const payload = JSON.parse(dataLine.slice(5)) as LoginJobEvent;
        if (eventType === 'close') {
          setWaiting(null);
          await Promise.all([loadJobs(), onAccountsChanged()]);
          return;
        }
        setWaiting(eventType === 'waiting' && payload.seconds ? payload.seconds : null);
        setEvents((current) => [...current, { ...payload, type: eventType }].slice(-200));
      }
    }
  }

  async function createJob(payload: {
    text: string;
    delayMinSecs: number;
    delayMaxSecs: number;
    failDelayMinSecs: number;
    failDelayMaxSecs: number;
  }) {
    setJobError('');
    const result = await api.createLoginJob(payload);
    await loadJobs();
    followJob(result.id).catch((err) => setJobError(err instanceof Error ? err.message : '读取进度失败'));
  }

  async function cancelJob(id: string) {
    await api.cancelLoginJob(id);
    await loadJobs();
  }

  useEffect(() => {
    if (!enabled) return undefined;
    loadJobs().catch((err) => setJobError(err instanceof Error ? err.message : '读取任务失败'));
    const timer = window.setInterval(() => loadJobs().catch(() => undefined), 3000);
    return () => window.clearInterval(timer);
  }, [enabled]);

  return { jobs, activeJobId, events, waiting, jobError, createJob, cancelJob, followJob };
}

function AddAccountModal({ open, onClose, onCreated }: { open: boolean; onClose: () => void; onCreated: () => Promise<void> }) {
  const [mode, setMode] = useState<AddMode>('password');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [label, setLabel] = useState('');
  const [text, setText] = useState('');
  const [token, setToken] = useState('');
  const [apiKey, setApiKey] = useState('');
  const [proxyUrl, setProxyUrl] = useState('');
  const [delayMinSecs, setDelayMinSecs] = useState(15);
  const [delayMaxSecs, setDelayMaxSecs] = useState(45);
  const [failDelayMinSecs, setFailDelayMinSecs] = useState(60);
  const [failDelayMaxSecs, setFailDelayMaxSecs] = useState(180);
  const [message, setMessage] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [batchFormOpen, setBatchFormOpen] = useState(true);
  const lastBatchStartedRef = useRef(false);
  const jobsState = useLoginJobs(open, onCreated);

  const lineCount = useMemo(() => text.split('\n').map((line) => line.trim()).filter(Boolean).length, [text]);
  const batchStarted = mode === 'batch' && (!!jobsState.activeJobId || jobsState.events.length > 0);

  useEffect(() => {
    if (!open) {
      setBatchFormOpen(true);
      lastBatchStartedRef.current = false;
      return;
    }
    if (batchStarted && !lastBatchStartedRef.current) {
      setBatchFormOpen(false);
    }
    lastBatchStartedRef.current = batchStarted;
  }, [open, batchStarted]);

  if (!open) return null;

  async function submitPassword(event: FormEvent) {
    event.preventDefault();
    setSubmitting(true);
    setMessage('');
    try {
      await api.createAccount({ email, password, label, proxyUrl: proxyUrl || undefined, maxConcurrent: 1 });
      setEmail('');
      setPassword('');
      setLabel('');
      setMessage('账号已添加。');
      await onCreated();
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '添加失败');
    } finally {
      setSubmitting(false);
    }
  }

  async function submitCredential(event: FormEvent) {
    event.preventDefault();
    setSubmitting(true);
    setMessage('');
    try {
      await api.createAccount({
        email: email || undefined,
        label,
        token: mode === 'token' ? token : undefined,
        apiKey: mode === 'apiKey' ? apiKey : undefined,
        proxyUrl: proxyUrl || undefined,
        maxConcurrent: 1,
      });
      setToken('');
      setApiKey('');
      setEmail('');
      setLabel('');
      setProxyUrl('');
      setMessage('账号已添加。');
      await onCreated();
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '添加失败');
    } finally {
      setSubmitting(false);
    }
  }

  async function submitBatch(event: FormEvent) {
    event.preventDefault();
    setMessage('');
    try {
      await jobsState.createJob({ text, delayMinSecs, delayMaxSecs, failDelayMinSecs, failDelayMaxSecs });
      setText('');
    } catch (err) {
      setMessage(err instanceof Error ? err.message : '创建任务失败');
    }
  }

  return (
    <div className="modal-backdrop" role="presentation">
      <section className="modal-panel" role="dialog" aria-modal="true" aria-labelledby="add-account-title">
        <header className="modal-head">
          <div>
            <h2 id="add-account-title">添加账号</h2>
            <p>选择一种方式添加，批量导入会逐个处理并显示进度。</p>
          </div>
          <button className="icon-button" type="button" onClick={onClose} aria-label="关闭">
            <X size={18} />
          </button>
        </header>

        <div className="segmented-control" role="tablist">
          {addModes.map((item) => (
            <button key={item.key} className={mode === item.key ? 'active' : ''} type="button" onClick={() => setMode(item.key)}>
              {item.label}
            </button>
          ))}
        </div>

        {mode === 'password' ? (
          <form className="modal-section stack" onSubmit={submitPassword}>
            <div className="section-note">
              <ShieldCheck size={18} />
              <span>适合先补充单个账号，后续真实登录流程接通后会在这里直接完成登录。</span>
            </div>
            <div className="field-grid">
              <label>
                邮箱
                <input placeholder="name@example.com" value={email} onChange={(event) => setEmail(event.target.value)} />
              </label>
              <label>
                密码
                <SecretInput autoComplete="current-password" placeholder="登录密码" value={password} onChange={setPassword} />
              </label>
              <label>
                备注
                <input placeholder="例如 主力账号" value={label} onChange={(event) => setLabel(event.target.value)} />
              </label>
              <label>
                代理
                <input placeholder="http://proxy:8080 或 socks5://host:1080" value={proxyUrl} onChange={(event) => setProxyUrl(event.target.value)} />
              </label>
            </div>
            {message ? <StateBlock message={message} /> : null}
            <div className="modal-actions">
              <button className="secondary-button" type="button" onClick={onClose}>
                取消
              </button>
              <button className="primary-button" disabled={!email.trim() || !password || submitting} type="submit">
                登录并添加
              </button>
            </div>
          </form>
        ) : null}

        {mode === 'batch' ? (
          <form className="modal-section stack" onSubmit={submitBatch}>
            <LoginJobPanel {...jobsState} />
            <details className="batch-input-panel" open={batchFormOpen} onToggle={(event) => setBatchFormOpen(event.currentTarget.open)}>
              <summary>{batchStarted ? '继续添加或修改内容' : '填写要导入的账号'}</summary>
              <div className="batch-input-body">
                <label>
                  账号内容
                  <textarea
                    rows={9}
                    value={text}
                    onChange={(event) => setText(event.target.value)}
                    placeholder="user1@mail.com password&#10;http://user:pass@host:port user2@mail.com password&#10;socks5://user:pass@host:1080 user3@mail.com password"
                  />
                </label>
                <div className="field-grid compact">
                  <label>
                    成功后最少等待
                    <input type="number" min={1} value={delayMinSecs} onChange={(event) => setDelayMinSecs(Number(event.target.value))} />
                  </label>
                  <label>
                    成功后最多等待
                    <input type="number" min={1} value={delayMaxSecs} onChange={(event) => setDelayMaxSecs(Number(event.target.value))} />
                  </label>
                  <label>
                    失败后最少等待
                    <input type="number" min={1} value={failDelayMinSecs} onChange={(event) => setFailDelayMinSecs(Number(event.target.value))} />
                  </label>
                  <label>
                    失败后最多等待
                    <input type="number" min={1} value={failDelayMaxSecs} onChange={(event) => setFailDelayMaxSecs(Number(event.target.value))} />
                  </label>
                </div>
                {message || jobsState.jobError ? <StateBlock message={message || jobsState.jobError} /> : null}
                <div className="modal-actions">
                  <span className="muted-text">已填写 {lineCount} 行</span>
                  <button className="primary-button" disabled={lineCount === 0} type="submit">
                    开始导入
                  </button>
                </div>
              </div>
            </details>
          </form>
        ) : null}

        {mode === 'token' || mode === 'apiKey' ? (
          <form className="modal-section stack" onSubmit={submitCredential}>
            <div className="section-note warning">
              <KeyRound size={18} />
              <span>{mode === 'token' ? '粘贴登录后得到的 Token，系统会换取可用凭据。' : '粘贴已有 API Key，系统会保存到账号池。'}</span>
            </div>
            <div className="field-grid">
              <label>
                备注
                <input placeholder="例如 主力账号" value={label} onChange={(event) => setLabel(event.target.value)} />
              </label>
              <label>
                代理
                <input placeholder="http://proxy:8080 或 socks5://host:1080" value={proxyUrl} onChange={(event) => setProxyUrl(event.target.value)} />
              </label>
            </div>
            <label>
              {mode === 'token' ? 'Auth Token' : 'API Key'}
              <textarea
                rows={5}
                placeholder={mode === 'token' ? '粘贴登录后得到的 Token' : '粘贴已有 API Key'}
                value={mode === 'token' ? token : apiKey}
                onChange={(event) => (mode === 'token' ? setToken(event.target.value) : setApiKey(event.target.value))}
              />
            </label>
            {message ? <StateBlock message={message} /> : null}
            <div className="modal-actions">
              <button className="secondary-button" type="button" onClick={onClose}>
                关闭
              </button>
              <button className="primary-button" disabled={submitting || (mode === 'token' ? !token.trim() : !apiKey.trim())} type="submit">
                添加账号
              </button>
            </div>
          </form>
        ) : null}
      </section>
    </div>
  );
}

function LoginJobPanel({
  jobs,
  activeJobId,
  events,
  waiting,
  cancelJob,
  followJob,
}: ReturnType<typeof useLoginJobs>) {
  const eventListRef = useRef<HTMLDivElement | null>(null);
  const activeJob = jobs.find((job) => job.id === activeJobId) || jobs[0];
  const doneCount = jobDoneCount(activeJob);
  const progress = jobProgress(activeJob);
  const latestEvent = latestAccountEvent(events);
  const running = activeJob?.status === 'running';

  useEffect(() => {
    eventListRef.current?.scrollTo({ top: eventListRef.current.scrollHeight });
  }, [events.length]);

  return (
    <div className="job-console">
      <div className="job-console-head">
        <div>
          <h3>{activeJob ? (running ? '正在导入账号' : jobStatusText(activeJob.status)) : '等待开始导入'}</h3>
          <p>{activeJob ? `任务 ${activeJob.id.slice(0, 8)} · ${doneCount}/${activeJob.total}` : '填写账号后开始导入，这里会显示实时进度。'}</p>
        </div>
        {activeJob?.status === 'running' ? (
          <button className="secondary-button danger" type="button" onClick={() => cancelJob(activeJob.id)}>
            <Square size={16} />
            停止
          </button>
        ) : null}
      </div>

      <div className="job-progress-panel">
        <div className="job-progress-number">
          <strong>{progress}%</strong>
          <span>{activeJob ? `${doneCount} / ${activeJob.total}` : '0 / 0'}</span>
        </div>
        <div className="job-progress-main">
          <div className="job-progress-track">
            <i style={{ width: `${progress}%` }} />
          </div>
          <div className="job-progress-meta">
            <span>成功 {activeJob?.successCount || 0}</span>
            <span>失败 {activeJob?.failedCount || 0}</span>
            <span>等待 {activeJob ? Math.max(0, activeJob.total - doneCount) : 0}</span>
            {waiting != null ? <span className="waiting">继续前等待 {waiting} 秒</span> : null}
          </div>
        </div>
      </div>

      <div className="current-account-line">
        <span>当前账号</span>
        <strong>{latestEvent ? eventText(latestEvent) : '还没有开始处理账号'}</strong>
        {latestEvent?.index ? <small>第 {latestEvent.index} 行</small> : null}
      </div>

      <div className="job-columns">
        <section className="job-log-section">
          <h3>实时返回</h3>
          {events.length === 0 ? <StateBlock message="创建或选择任务后查看返回。" /> : null}
          <div ref={eventListRef} className="event-list compact">
            {events.map((event, index) => (
              <article className={`event-item event-${event.type}`} key={`${event.type}-${event.id || index}-${index}`}>
                <strong>{eventTitle(event)}</strong>
                <span>{eventMessage(event)}</span>
                {event.index ? <small>第 {event.index} 行</small> : null}
              </article>
            ))}
          </div>
        </section>
        <section className="job-list-section">
          <h3>最近任务</h3>
          <div className="mini-table">
            {jobs.length === 0 ? <StateBlock message="还没有批量任务。" /> : null}
            {jobs.map((job) => (
              <article className={activeJobId === job.id ? 'mini-row active' : 'mini-row'} key={job.id}>
                <button className="text-button" type="button" onClick={() => followJob(job.id)}>
                  {job.id.slice(0, 8)}
                </button>
                <span>{jobStatusText(job.status)}</span>
                <span>
                  {jobDoneCount(job)}/{job.total}
                </span>
                {job.status === 'running' ? (
                  <button className="icon-button danger" type="button" onClick={() => cancelJob(job.id)} aria-label="停止任务">
                    <Square size={14} />
                  </button>
                ) : null}
              </article>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}

function ProbeModelPicker({
  value,
  models,
  disabled,
  onChange,
}: {
  value: string;
  models: ProbeModelOption[];
  disabled?: boolean;
  onChange: (value: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const ref = useRef<HTMLDivElement | null>(null);
  const selected = models.find((item) => item.id === value) || { id: value };
  const filteredModels = useMemo(() => {
    const keyword = query.trim().toLowerCase();
    if (!keyword) return models;
    return models.filter((item) => {
      const label = modelLabel(item).toLowerCase();
      return label.includes(keyword) || item.id.toLowerCase().includes(keyword);
    });
  }, [models, query]);

  useEffect(() => {
    if (!open) return undefined;
    const close = (event: MouseEvent) => {
      if (!ref.current?.contains(event.target as Node)) setOpen(false);
    };
    window.addEventListener('mousedown', close);
    return () => window.removeEventListener('mousedown', close);
  }, [open]);

  useEffect(() => {
    if (!open) setQuery('');
  }, [open]);

  return (
    <div ref={ref} className="model-picker">
      <button className="model-picker-button" type="button" disabled={disabled || models.length === 0} onClick={() => setOpen((value) => !value)}>
        <span>{modelLabel(selected)}</span>
        <ChevronDown size={16} />
      </button>
      {open ? (
        <div className="model-picker-popover">
          <div className="model-picker-search">
            <Search size={15} />
            <input
              autoFocus
              placeholder="搜索模型"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') event.preventDefault();
              }}
            />
          </div>
          <div className="model-picker-list">
            {filteredModels.length === 0 ? <div className="model-picker-empty">没有找到模型</div> : null}
            {filteredModels.map((item) => (
              <button
                key={item.id}
                className={item.id === value ? 'active' : ''}
                type="button"
                onClick={() => {
                  onChange(item.id);
                  setOpen(false);
                }}
              >
                <strong>{modelLabel(item)}</strong>
                {item.label && item.label !== item.id ? <span>{item.id}</span> : null}
              </button>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function ProbeAccountModal({
  account,
  onClose,
  onFinished,
}: {
  account: Account | null;
  onClose: () => void;
  onFinished: () => Promise<void>;
}) {
  const [model, setModel] = useState('claude-opus-4.7');
  const [message, setMessage] = useState('用一句话确认这个账号可以正常回复。');
  const [models, setModels] = useState<ProbeModelOption[]>(normalizeProbeModels([], 'claude-opus-4.7'));
  const [status, setStatus] = useState('');
  const [output, setOutput] = useState('');
  const [running, setRunning] = useState(false);
  const [aborter, setAborter] = useState<AbortController | null>(null);

  useEffect(() => {
    if (!account) return;
    setStatus('正在读取默认探测内容');
    api.probeAccountDefaults()
      .then((data) => {
        const nextModel = data.model || 'claude-opus-4.7';
        setModels(normalizeProbeModels(data.models, nextModel));
        setModel(nextModel);
        setMessage(data.message || '用一句话确认这个账号可以正常回复。');
        setStatus('');
      })
      .catch((err) => setStatus(err instanceof Error ? err.message : '读取失败'));
  }, [account?.id]);

  useEffect(() => () => aborter?.abort(), [aborter]);

  if (!account) return null;
  const activeAccount = account;

  function stop() {
    aborter?.abort();
    setRunning(false);
    setStatus('已停止读取返回');
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    const controller = new AbortController();
    setAborter(controller);
    setRunning(true);
    setOutput('');
    setStatus('正在发送');
    const waitingTimer = window.setTimeout(() => {
      setStatus('后端还在处理，请检查账号或代理后重试。');
    }, 8000);
    const timeoutTimer = window.setTimeout(() => {
      controller.abort();
      setStatus('请求等待太久，已停止。请检查账号或代理后重试。');
    }, 60000);
    try {
      const resp = await api.probeAccount(activeAccount.id, {
        model,
        message,
        saveDefaults: true,
      }, controller.signal);
      window.clearTimeout(waitingTimer);
      if (!resp.ok || !resp.body) {
        let text = `请求失败：${resp.status}`;
        try {
          const body = await resp.json();
          text = body?.error?.message || text;
        } catch {
          // 保留状态码。
        }
        throw new Error(text);
      }
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const events = buffer.split('\n\n');
        buffer = events.pop() || '';
        for (const eventText of events) {
          const dataLine = eventText.split('\n').find((line) => line.startsWith('data:'));
          if (!dataLine) continue;
          const raw = dataLine.slice(5).trim();
          if (!raw) continue;
          const payload = JSON.parse(raw);
          if (payload.type === 'message_start') {
            setStatus(`正在探测 ${payload.email || accountTitle(activeAccount)}`);
          } else if (payload.type === 'content_block_delta') {
            const text = payload.delta?.text || payload.delta?.delta?.text || '';
            if (text) setOutput((current) => current + text);
          } else if (payload.type === 'error') {
            throw new Error(payload.error?.message || '探测失败');
          } else if (payload.type === 'message_stop') {
            setStatus('探测完成');
          }
        }
      }
      await onFinished();
    } catch (err) {
      if ((err as Error).name === 'AbortError') return;
      setStatus(err instanceof Error ? err.message : '探测失败');
    } finally {
      window.clearTimeout(waitingTimer);
      window.clearTimeout(timeoutTimer);
      setRunning(false);
      setAborter(null);
    }
  }

  return (
    <div className="modal-backdrop" role="presentation">
      <section className="modal-panel probe-modal" role="dialog" aria-modal="true" aria-labelledby="probe-account-title">
        <header className="modal-head">
          <div>
            <h2 id="probe-account-title">探测账号</h2>
            <p>{accountTitle(activeAccount)} · #{activeAccount.id}</p>
          </div>
          <button className="icon-button" type="button" onClick={onClose} aria-label="关闭">
            <X size={18} />
          </button>
        </header>
        <form className="modal-section stack" onSubmit={submit}>
          <div className="field-grid">
            <label>
              模型
              <ProbeModelPicker value={model} models={models} disabled={running} onChange={setModel} />
            </label>
            <label>
              账号
              <input value={`${accountTitle(activeAccount)} (#${activeAccount.id})`} disabled readOnly />
            </label>
          </div>
          <label>
            发送内容
            <textarea rows={7} value={message} onChange={(event) => setMessage(event.target.value)} />
          </label>
          <div className="modal-actions">
            {running ? (
              <button className="secondary-button" type="button" onClick={stop}>
                <Square size={16} />
                停止
              </button>
            ) : null}
            <button className="primary-button" type="submit" disabled={running || !model || !message.trim()}>
              <Play size={16} />
              开始探测
            </button>
          </div>
          {status ? <StateBlock message={status} /> : null}
          <div className="probe-output-wrap">
            <div className="section-heading flat">
              <div>
                <h2>返回内容</h2>
                <p>开始后会显示这个账号的真实返回。</p>
              </div>
            </div>
            <pre className="stream-output">{output || '等待返回'}</pre>
          </div>
        </form>
      </section>
    </div>
  );
}

function AccountActionMenu({
  account,
  disabled,
  onProbe,
  onCredits,
  onToggle,
  onReset,
  onReveal,
  onClearLimit,
  onClearSticky,
  onDelete,
}: {
  account: Account;
  disabled: boolean;
  onProbe: () => void;
  onCredits: () => void;
  onToggle: () => void;
  onReset: () => void;
  onReveal: () => void;
  onClearLimit: () => void;
  onClearSticky: () => void;
  onDelete: () => void;
}) {
  const [open, setOpen] = useState(false);
  const buttonRef = useRef<HTMLButtonElement | null>(null);
  const [position, setPosition] = useState({ top: 0, left: 0 });
  const isDisabled = account.status === 'disabled';

  useEffect(() => {
    if (!open) return undefined;
    const close = () => setOpen(false);
    window.addEventListener('scroll', close, true);
    window.addEventListener('resize', close);
    return () => {
      window.removeEventListener('scroll', close, true);
      window.removeEventListener('resize', close);
    };
  }, [open]);

  function toggleMenu() {
    const rect = buttonRef.current?.getBoundingClientRect();
    if (rect) {
      setPosition({ top: rect.bottom + 6, left: Math.max(12, rect.right - 152) });
    }
    setOpen((value) => !value);
  }

  function click(action: () => void) {
    setOpen(false);
    action();
  }

  return (
    <div className="action-menu">
      <button ref={buttonRef} className="icon-button" type="button" disabled={disabled} onClick={toggleMenu} aria-label="账号操作">
        <MoreHorizontal size={16} />
      </button>
      {open ? createPortal(
        <div className="action-menu-list" style={{ top: position.top, left: position.left }}>
          <button type="button" onClick={() => click(onProbe)}>探测账号</button>
          <button type="button" onClick={() => click(onCredits)}>刷新余额</button>
          <button type="button" onClick={() => click(onToggle)}>{isDisabled ? '启用账号' : '停用账号'}</button>
          <button type="button" onClick={() => click(onReset)}>重置错误</button>
          <button type="button" onClick={() => click(onReveal)}>复制 Key</button>
          <button type="button" onClick={() => click(onClearLimit)}>清除限流</button>
          <button type="button" onClick={() => click(onClearSticky)}>清除会话</button>
          <button className="danger" type="button" onClick={() => click(onDelete)}>删除账号</button>
        </div>,
        document.body,
      ) : null}
    </div>
  );
}

export function AccountsPage() {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [error, setError] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [probeAccount, setProbeAccount] = useState<Account | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set());
  const [busyAction, setBusyAction] = useState('');
  const [revealed, setRevealed] = useState<Record<number, string>>({});
  const [liveText, setLiveText] = useState('正在连接实时状态');
  const refreshTimerRef = useRef<number | null>(null);

  async function load() {
    const data = await api.accounts();
    setAccounts(data.accounts);
  }

  function scheduleLoad() {
    if (refreshTimerRef.current != null) return;
    refreshTimerRef.current = window.setTimeout(() => {
      refreshTimerRef.current = null;
      load().catch((err) => setError(err instanceof Error ? err.message : '刷新失败'));
    }, 250);
  }

  useEffect(() => {
    load().catch((err) => setError(err instanceof Error ? err.message : '读取失败'));
    return () => {
      if (refreshTimerRef.current != null) window.clearTimeout(refreshTimerRef.current);
    };
  }, []);

  useAdminEvents(true, (event) => {
    if (event.kind === 'ready') {
      setLiveText('实时状态已连接');
      return;
    }
    if (event.kind === 'resync') {
      setLiveText('正在同步最新状态');
      scheduleLoad();
      return;
    }
    const refreshKinds = new Set([
      'account_changed',
      'account_request_started',
      'account_request_finished',
      'account_request_succeeded',
      'account_rate_limited',
      'account_error',
      'account_transient_error',
      'account_probe_started',
      'login_job_changed',
    ]);
    if (refreshKinds.has(event.kind)) {
      setLiveText('刚刚更新');
      scheduleLoad();
    }
  }, (message) => setLiveText(message));

  async function remove(id: number) {
    await api.deleteAccount(id);
    await load();
  }

  async function runAction(key: string, action: () => Promise<void>) {
    setBusyAction(key);
    setError('');
    try {
      await action();
    } catch (err) {
      setError(err instanceof Error ? err.message : '操作失败');
    } finally {
      setBusyAction('');
    }
  }

  async function setAccountEnabled(account: Account, enabled: boolean) {
    await api.updateAccount(account.id, { status: enabled ? 'ready' : 'disabled' });
    await load();
  }

  function toggleDetail(id: number) {
    setExpandedIds((current) => {
      const next = new Set(current);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }

  const healthyCount = accounts.filter((account) => ['ready', 'active', 'ok'].includes(account.status)).length;
  const errorCount = accounts.filter((account) => account.status === 'error' || account.status === 'banned' || account.lastError || account.errorCount > 0).length;

  return (
    <>
      <PageHeader title="账号管理" subtitle="维护账号池，查看账号状态和最近登录结果。" />
      <section className="account-toolbar">
        <div className="toolbar-stat">
          <CheckCircle2 size={18} />
          <span>可用账号</span>
          <strong>{healthyCount}</strong>
        </div>
        <div className="toolbar-stat">
          <ClipboardList size={18} />
          <span>全部账号</span>
          <strong>{accounts.length}</strong>
        </div>
        <div className="toolbar-stat">
          <ShieldCheck size={18} />
          <span>需要处理</span>
          <strong>{errorCount}</strong>
        </div>
        <div className="toolbar-stat live-stat">
          <Activity size={18} />
          <span>实时状态</span>
          <strong>{liveText}</strong>
        </div>
        <div className="toolbar-actions">
          <button className="secondary-button" type="button" onClick={() => load().catch((err) => setError(err instanceof Error ? err.message : '刷新失败'))}>
            <RefreshCw size={16} />
            刷新
          </button>
          <button className="secondary-button" type="button" disabled={!!busyAction} onClick={() => runAction('status-all', async () => { await api.refreshAccountsStatus(); await load(); })}>
            <RefreshCw size={16} />
            刷新状态
          </button>
          <button className="secondary-button" type="button" disabled={!!busyAction} onClick={() => runAction('credits-all', async () => { await api.refreshAccountsCredits(); await load(); })}>
            <RefreshCw size={16} />
            刷新余额
          </button>
          <button className="primary-button" type="button" onClick={() => setModalOpen(true)}>
            <Plus size={16} />
            添加账号
          </button>
        </div>
      </section>

      <section className="panel accounts-panel">
        <div className="section-heading">
          <div>
            <h2>账号列表</h2>
            <p>点击刷新可更新列表，异常账号会显示最近失败原因。</p>
          </div>
        </div>
        {error ? <StateBlock message={error} /> : null}
        <table className="accounts-table">
          <colgroup>
            <col className="col-account" />
            <col className="col-state" />
            <col className="col-usage" />
            <col className="col-limit-session" />
            <col className="col-quota-model" />
            <col className="col-actions" />
          </colgroup>
          <thead>
            <tr>
              <th>账号</th>
              <th>状态</th>
              <th>用量</th>
              <th>限制</th>
              <th>额度</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {accounts.map((account) => {
              const isExpanded = expandedIds.has(account.id);
              const isDisabled = account.status === 'disabled';
              const errorNumber = account.errorCount || (account.lastError ? 1 : 0);
              const rpmPct = account.rpmLimit > 0 ? Math.min(100, Math.round((account.rpmUsed / account.rpmLimit) * 100)) : 0;
              const stats = modelStats(account);
              const planSubline = tierSubline(account);
              const limitCount = modelLimitCount(account);
              const keyValue = revealed[account.id] || account.apiKey || account.credentialMask || '-';
              return (
                <Fragment key={account.id}>
                  <tr className={isExpanded ? 'expanded-row' : ''}>
                    <td className="account-main-cell">
                      <div className="account-title-line">
                        <button className={isExpanded ? 'expand-button open' : 'expand-button'} type="button" onClick={() => toggleDetail(account.id)} aria-label="查看详情">
                          <ChevronRight size={14} />
                        </button>
                        <strong>{accountTitle(account)}</strong>
                      </div>
                      {accountTitle(account) !== account.email ? <small>{account.email}</small> : null}
                      <div className="account-meta-line">
                        <span>#{account.id}</span>
                        <span>{modeLabel(account.authMethod)}</span>
                        <span>{account.proxyId ? `代理 #${account.proxyId}` : '未设置代理'}</span>
                      </div>
                      <div className="account-key-tag">
                        <span>Key</span>
                        <strong>{keyValue === '-' ? '未保存' : '已保存'}</strong>
                      </div>
                    </td>
                    <td>
                      <div className="stacked-cell">
                        <div className="status-line">
                          <StatusBadge value={account.status} label={statusText(account.status)} />
                          <span className={`tier ${account.tier || 'unknown'}`}>{tierText(account.tier)}{account.tierManual ? ' *' : ''}</span>
                        </div>
                        {planSubline ? <span className="tier-subline">{planSubline}</span> : null}
                        <span className={errorNumber > 0 ? 'error-count' : 'muted-text'}>{errorNumber} 次错误</span>
                      </div>
                    </td>
                    <td>
                      <div className="stacked-cell">
                        <div className="metric-pair">
                          <span>并发</span>
                          <strong>{account.currentConcurrent}/{account.maxConcurrent}</strong>
                        </div>
                        <div className="quota-cell compact">
                          <div className="quota-line">
                            <span>RPM {account.rpmUsed}/{account.rpmLimit}</span>
                            <span>{rpmPct}%</span>
                          </div>
                          <div className="mini-progress">
                            <span style={{ width: `${rpmPct}%` }} />
                          </div>
                        </div>
                        <span className="muted-text">最近 {formatShortTime(account.lastUsed)}</span>
                      </div>
                    </td>
                    <td>
                      <div className="stacked-cell">
                        <div className={`limit-cell ${availabilityClass(account)}`}>
                            <Ban size={13} />
                          <span>{availabilityText(account)}</span>
                          <small>{availabilityDetail(account)}</small>
                        </div>
                        {limitCount ? <span className="muted-text">限流模型 {limitCount}</span> : null}
                        <div className="metric-pair">
                          <span>会话</span>
                          <strong>{account.stickyCount || 0}</strong>
                        </div>
                      </div>
                    </td>
                    <td>
                      <div className="stacked-cell">
                        <CreditSummary account={account} />
                        <button className="text-button compact" type="button" onClick={() => toggleDetail(account.id)}>
                          {stats.total ? (
                            <>可用模型 {stats.availableCount}/{stats.total}{stats.blockedCount ? ` -${stats.blockedCount}` : ''}</>
                          ) : (
                            '模型待探测'
                          )}
                        </button>
                      </div>
                    </td>
                    <td className="nowrap">
                      <AccountActionMenu
                        account={account}
                        disabled={!!busyAction}
                        onProbe={() => setProbeAccount(account)}
                        onCredits={() => runAction(`credits-${account.id}`, async () => { await api.refreshAccountCredits(account.id); await load(); })}
                        onToggle={() => runAction(`status-${account.id}`, () => setAccountEnabled(account, isDisabled))}
                        onReset={() => runAction(`reset-${account.id}`, async () => { await api.resetAccountErrors(account.id); await load(); })}
                        onReveal={() => runAction(`reveal-${account.id}`, async () => {
                          const data = await api.revealAccountKey(account.id);
                          setRevealed((current) => ({ ...current, [account.id]: data.apiKey }));
                          await navigator.clipboard?.writeText(data.apiKey).catch(() => undefined);
                        })}
                        onClearLimit={() => runAction(`clear-limit-${account.id}`, async () => { await api.clearAccountRateLimit(account.id); await load(); })}
                        onClearSticky={() => runAction(`clear-sticky-${account.id}`, async () => { await api.clearAccountSticky(account.id); await load(); })}
                        onDelete={() => runAction(`delete-${account.id}`, () => remove(account.id))}
                      />
                    </td>
                  </tr>
                  {isExpanded ? (
                    <tr className="account-detail-row" key={`${account.id}-detail`}>
                      <td colSpan={6}>
                        <div className="account-detail-wrap">
                          <div className="detail-grid">
                            <div>
                              <span>邮箱</span>
                              <strong>{account.email}</strong>
                            </div>
                            <div>
                              <span>登录方式</span>
                              <strong>{modeLabel(account.authMethod)}</strong>
                            </div>
                            <div>
                              <span>代理</span>
                              <strong>{account.proxyId ? `#${account.proxyId}` : '未设置'}</strong>
                            </div>
                            <div>
                              <span>并发</span>
                              <strong>
                                {account.currentConcurrent}/{account.maxConcurrent}
                              </strong>
                            </div>
                            <div>
                              <span>最近登录</span>
                              <strong>{formatTime(account.lastLoginAt)}</strong>
                            </div>
                            <div>
                              <span>最近探测</span>
                              <strong>{formatTime(account.lastProbed)}</strong>
                            </div>
                            <div>
                              <span>最近错误</span>
                              <strong>{account.availability?.upstreamError || account.lastError || '无'}</strong>
                            </div>
                            <div>
                              <span>添加时间</span>
                              <strong>{formatTime(account.createdAt)}</strong>
                            </div>
                            <div>
                              <span>更新时间</span>
                              <strong>{formatTime(account.updatedAt)}</strong>
                            </div>
                          </div>
                          <div className="model-list">
                            {stats.models.slice(0, 40).map((model) => (
                              <span key={modelId(model)}>{modelLabel(model)}</span>
                            ))}
                            {stats.total === 0 ? <span>暂无可用模型数据</span> : null}
                          </div>
                        </div>
                      </td>
                    </tr>
                  ) : null}
                </Fragment>
              );
            })}
            {accounts.length === 0 ? (
              <tr>
                <td colSpan={6}>
                  <StateBlock message="还没有账号，点击右上角添加。" />
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </section>

      <AddAccountModal open={modalOpen} onClose={() => setModalOpen(false)} onCreated={load} />
      <ProbeAccountModal account={probeAccount} onClose={() => setProbeAccount(null)} onFinished={load} />
    </>
  );
}
