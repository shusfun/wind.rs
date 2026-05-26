export type ApiEnvelope<T> = {
  success: boolean;
  data: T;
};

export type ApiErrorBody = {
  error?: {
    type?: string;
    message?: string;
  };
};

export type AdminEvent = {
  kind: string;
  payload?: Record<string, unknown>;
  createdAt?: string;
  created_at?: string;
};

export type AccountAvailabilityKind =
  | 'available'
  | 'probing'
  | 'account_rate_limited'
  | 'model_rate_limited'
  | 'rpm_full'
  | 'tier_expired'
  | 'model_blocked'
  | 'credential_missing'
  | 'concurrency_full'
  | 'status_error'
  | 'status_disabled'
  | 'status_banned'
  | 'status_unavailable';

export type AccountAvailability = {
  available: boolean;
  kind: AccountAvailabilityKind;
  retryAfterSecs: number;
  upstreamError?: string | null;
};

export type Account = {
  id: number;
  email: string;
  label?: string | null;
  status: string;
  tier: string;
  tierManual: boolean;
  errorCount: number;
  priority: number;
  maxConcurrent: number;
  currentConcurrent: number;
  proxyId?: number | null;
  cooldownUntil?: string | null;
  lastUsed?: string | null;
  lastProbed?: string | null;
  rateLimitedUntil?: string | null;
  rateLimited: boolean;
  rpmUsed: number;
  rpmLimit: number;
  credits?: AccountCredits | null;
  userStatus?: AccountUserStatus | null;
  availableModels: AccountModel[];
  tierModels: AccountModel[] | string[];
  blockedModels: string[];
  modelRateLimits?: Record<string, { limitedUntil: string; reason?: string | null; probeAfter?: string | null }>;
  availability?: AccountAvailability;
  stickyCount: number;
  lastError?: string | null;
  credentialMask?: string | null;
  apiKey?: string | null;
  authMethod?: string | null;
  apiServerUrl?: string | null;
  lastLoginAt?: string | null;
  createdAt: string;
  updatedAt: string;
};

export type AccountCredits = {
  planName?: string | null;
  trialEndMs?: number | null;
  dailyPercent?: number | null;
  weeklyPercent?: number | null;
  dailyResetAt?: number | string | null;
  weeklyResetAt?: number | string | null;
  percent?: number | null;
  prompt?: { limit?: number | null; used?: number | null; remaining?: number | null };
  flex?: { limit?: number | null; used?: number | null; remaining?: number | null };
  fetchedAt?: number | null;
  lastError?: string | null;
};

export type AccountUserStatus = {
  planName?: string | null;
  trialEndMs?: number | null;
  userStatus?: {
    planStatus?: {
      planInfo?: {
        planName?: string | null;
      };
    };
    windsurfProTrialEndTime?: number | string | { seconds?: number | string | null } | null;
  } | null;
};

export type AccountModel = {
  id: string;
  label?: string;
  provider?: string;
  creditMultiplier?: number | null;
  supportsImages?: boolean;
};

export type CapacitySettings = {
  queueCapacity: number;
  queueTimeoutSecs: number;
  globalConcurrency: number;
  modelConcurrency: number;
  accountConcurrency: number;
  maxRetries: number;
  fallbackDelayMs: number;
  modelCooldownSecs: number;
  suspiciousCooldownSecs: number;
  stickySessionMinutes: number;
};

export type SystemPromptMode = 'passthrough' | 'strip-identity' | 'windsurf-wrap';

export type AdminSettings = {
  systemPromptMode: SystemPromptMode;
};

export type CapacityRuntime = {
  globalInflight: number;
  models: Array<{ model: string; inflight: number }>;
};

export type ProxyItem = {
  id: number;
  name: string;
  url: string;
  status: string;
  lastError?: string | null;
  createdAt: string;
  updatedAt: string;
};

export type LoginJob = {
  id: string;
  status: string;
  total: number;
  successCount: number;
  failedCount: number;
  cancelled: boolean;
  createdAt: string;
  updatedAt: string;
  completedAt?: string | null;
};

export type LoginJobEvent = {
  id?: number;
  type: string;
  index?: number;
  total?: number;
  email?: string;
  emailMasked?: string;
  status?: string;
  accountId?: number | null;
  errorCode?: string;
  message?: string;
  authFail?: boolean;
  retryAfterSecs?: number | null;
  seconds?: number;
  reason?: string;
  waitingUntil?: string;
  successCount?: number;
  failedCount?: number;
};

export type RequestTrace = {
  id: string;
  model?: string | null;
  stream: boolean;
  accountId?: number | null;
  status: string;
  endReason?: string | null;
  errorSummary?: string | null;
  startedAt: string;
  endedAt?: string | null;
};

export type TraceChunk = {
  id: number;
  layer: string;
  payload: string;
  createdAt: string;
};

export type ModelListItem = {
  id: string;
  object: string;
  created: number;
  owned_by: string;
  _windsurf?: AccountModel;
};

export type StatsRangeKey = '24h' | '7d' | '30d';

export type AdminStats = {
  range: { key: StatsRangeKey; label: string; since: string };
  overview: {
    requests: number;
    succeeded: number;
    failed: number;
    running: number;
    successRate: number;
    avgLatencyMs: number;
    accounts: number;
    availableAccounts: number;
    issueAccounts: number;
  };
  models: Array<{ model: string; requests: number; succeeded: number; failed: number; successRate: number }>;
  accounts: Array<{ accountId: number; name: string; requests: number; failed: number; successRate: number; lastError?: string | null }>;
  errors: Array<{ message: string; count: number }>;
  accountStates: Array<{ id: number; name: string; status: string; rateLimited: boolean; errorCount: number; lastError?: string | null }>;
  loginJobs: { total: number; running: number; succeeded: number; failed: number };
  timeline: Array<{ label: string; requests: number; succeeded: number; failed: number }>;
};

export type AdminModelControlItem = AccountModel & {
  enabled: boolean;
  accountCount: number;
  limitedAccountCount: number;
  recentFailures: number;
};

export type AdminModelConfig = {
  defaultModel: string;
  disabledModels: string[];
  models: AdminModelControlItem[];
};

export type ClientApiKey = {
  id: number;
  name: string;
  key?: string | null;
  keyMask: string;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
  lastUsedAt?: string | null;
};

export const authExpiredEventName = 'windsurf-rs-auth-expired';

export function getToken() {
  return localStorage.getItem('windsurf_rs_token') || '';
}

export function setToken(token: string) {
  localStorage.setItem('windsurf_rs_token', token);
}

export function clearToken() {
  localStorage.removeItem('windsurf_rs_token');
}

export function authHeaders() {
  return { authorization: `Bearer ${getToken()}` };
}

export function notifyAuthExpired() {
  clearToken();
  window.dispatchEvent(new Event(authExpiredEventName));
}

export function handleAuthResponse(resp: Response) {
  if (resp.status === 401 || resp.status === 403) {
    notifyAuthExpired();
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  if (!headers.has('content-type') && init.body) {
    headers.set('content-type', 'application/json');
  }
  const token = getToken();
  if (token) {
    headers.set('authorization', `Bearer ${token}`);
  }
  const resp = await fetch(path, { ...init, headers });
  if (!resp.ok) {
    handleAuthResponse(resp);
    let message = `请求失败：${resp.status}`;
    try {
      const body = (await resp.json()) as ApiErrorBody;
      message = body.error?.message || message;
    } catch {
      // 保留默认错误信息。
    }
    throw new Error(message);
  }
  return (await resp.json()) as T;
}

async function envelope<T>(path: string, init?: RequestInit): Promise<T> {
  const body = await request<ApiEnvelope<T>>(path, init);
  return body.data;
}

export const api = {
  health: () => request<{ ok: boolean; setup: boolean; service: string; version: string }>('/health'),
  setupStatus: () => envelope<{ needsSetup: boolean; step: string }>('/setup/status'),
  install: (adminKey: string) =>
    envelope<{ message: string }>('/setup/install', {
      method: 'POST',
      body: JSON.stringify({ adminKey }),
    }),
  login: (adminKey: string) =>
    envelope<{ token: string }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ adminKey }),
    }),
  logout: () => envelope<Record<string, never>>('/auth/logout', { method: 'POST' }),
  models: () => request<{ object: 'list'; data: ModelListItem[] }>('/v1/models'),
  accounts: () => envelope<{ accounts: Account[] }>('/admin/accounts'),
  createAccount: (payload: {
    email?: string;
    password?: string;
    token?: string;
    apiKey?: string;
    label?: string;
    priority?: number;
    maxConcurrent?: number;
    proxyId?: number;
    proxyUrl?: string;
  }) =>
    envelope<{ id: number }>('/admin/accounts', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  updateAccount: (id: number, payload: Partial<Pick<Account, 'label' | 'status' | 'tier' | 'tierManual' | 'priority' | 'maxConcurrent' | 'proxyId' | 'blockedModels'>>) =>
    envelope<Record<string, never>>(`/admin/accounts/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),
  deleteAccount: (id: number) => envelope<Record<string, never>>(`/admin/accounts/${id}`, { method: 'DELETE' }),
  probeAccountDefaults: () => envelope<{ model: string; message: string; models: AccountModel[] }>('/admin/accounts/probe-defaults'),
  probeAccount: (id: number, payload: { model: string; message: string; saveDefaults?: boolean }, signal?: AbortSignal) =>
    fetch(`/admin/accounts/${id}/probe`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${getToken()}`,
      },
      body: JSON.stringify(payload),
      signal,
    }).then((resp) => {
      if (!resp.ok) handleAuthResponse(resp);
      return resp;
    }),
  refreshAccountsStatus: () => envelope<{ results: unknown[] }>('/admin/accounts/refresh-status', { method: 'POST' }),
  refreshAccountCredits: (id: number) => envelope<Record<string, unknown>>(`/admin/accounts/${id}/refresh-credits`, { method: 'POST' }),
  refreshAccountsCredits: () => envelope<{ results: unknown[] }>('/admin/accounts/refresh-credits', { method: 'POST' }),
  resetAccountErrors: (id: number) => envelope<Record<string, never>>(`/admin/accounts/${id}/reset-errors`, { method: 'POST' }),
  revealAccountKey: (id: number) => envelope<{ apiKey: string; credentialMask: string }>(`/admin/accounts/${id}/reveal-key`, { method: 'POST' }),
  clearAccountRateLimit: (id: number) => envelope<Record<string, never>>(`/admin/accounts/${id}/clear-rate-limit`, { method: 'POST' }),
  clearAccountSticky: (id: number) => envelope<Record<string, never>>(`/admin/accounts/${id}/clear-sticky`, { method: 'POST' }),
  proxies: () => envelope<{ proxies: ProxyItem[] }>('/admin/proxies'),
  createProxy: (payload: { name: string; url: string }) =>
    envelope<{ id: number }>('/admin/proxies', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  loginJobs: () => envelope<{ jobs: LoginJob[] }>('/admin/login-jobs'),
  createLoginJob: (payload: {
    text: string;
    delayMinSecs: number;
    delayMaxSecs: number;
    failDelayMinSecs: number;
    failDelayMaxSecs: number;
  }) =>
    envelope<{ id: string }>('/admin/login-jobs', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  cancelLoginJob: (id: string) => envelope<Record<string, never>>(`/admin/login-jobs/${id}/cancel`, { method: 'POST' }),
  clientApiKeys: () => envelope<{ keys: ClientApiKey[] }>('/admin/client-api-keys'),
  createClientApiKey: (payload: { name?: string; key?: string; enabled?: boolean }) =>
    envelope<{ id: number; key: string }>('/admin/client-api-keys', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  updateClientApiKey: (id: number, payload: { name?: string; key?: string; enabled?: boolean }) =>
    envelope<Record<string, never>>(`/admin/client-api-keys/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),
  deleteClientApiKey: (id: number) => envelope<Record<string, never>>(`/admin/client-api-keys/${id}`, { method: 'DELETE' }),
  requests: () => envelope<{ requests: RequestTrace[] }>('/admin/requests'),
  requestDetail: (id: string) => envelope<{ request: RequestTrace; chunks: TraceChunk[] }>(`/admin/requests/${id}`),
  stats: (range: StatsRangeKey) => envelope<AdminStats>(`/admin/stats?range=${encodeURIComponent(range)}`),
  modelConfig: () => envelope<AdminModelConfig>('/admin/models/config'),
  saveModelConfig: (payload: { defaultModel: string; disabledModels: string[] }) =>
    envelope<AdminModelConfig>('/admin/models/config', {
      method: 'PUT',
      body: JSON.stringify(payload),
    }),
  capacity: () => envelope<{ settings: CapacitySettings; runtime: CapacityRuntime }>('/admin/capacity'),
  saveCapacity: (payload: CapacitySettings) =>
    envelope<{ settings: CapacitySettings }>('/admin/capacity', {
      method: 'PUT',
      body: JSON.stringify(payload),
    }),
  settings: () => envelope<AdminSettings>('/admin/settings'),
  saveSettings: (payload: AdminSettings) =>
    envelope<Record<string, never>>('/admin/settings', {
      method: 'PUT',
      body: JSON.stringify(payload),
    }),
};
