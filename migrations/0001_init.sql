CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS admin_sessions (
  token_hash TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL,
  label TEXT,
  status TEXT NOT NULL DEFAULT 'ready',
  priority INTEGER NOT NULL DEFAULT 0,
  max_concurrent INTEGER NOT NULL DEFAULT 1,
  current_concurrent INTEGER NOT NULL DEFAULT 0,
  proxy_id INTEGER,
  cooldown_until TEXT,
  last_error TEXT,
  credentials_json TEXT,
  credential_mask TEXT,
  auth_method TEXT,
  api_server_url TEXT,
  tier TEXT NOT NULL DEFAULT 'unknown',
  tier_manual INTEGER NOT NULL DEFAULT 0,
  error_count INTEGER NOT NULL DEFAULT 0,
  last_used_at TEXT,
  last_probed_at TEXT,
  rate_limited_until TEXT,
  rate_limit_probe_after TEXT,
  rpm_used INTEGER NOT NULL DEFAULT 0,
  rpm_limit INTEGER NOT NULL DEFAULT 60,
  credits_json TEXT,
  user_status_json TEXT,
  available_models_json TEXT,
  tier_models_json TEXT,
  blocked_models_json TEXT,
  last_login_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS proxies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'unknown',
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS login_jobs (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  total INTEGER NOT NULL,
  success_count INTEGER NOT NULL DEFAULT 0,
  failed_count INTEGER NOT NULL DEFAULT 0,
  cancelled INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS login_job_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS login_lockouts (
  email TEXT PRIMARY KEY,
  failure_count INTEGER NOT NULL DEFAULT 0,
  locked_until TEXT,
  last_reason TEXT,
  last_activity TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS request_traces (
  id TEXT PRIMARY KEY,
  model TEXT,
  stream INTEGER NOT NULL DEFAULT 0,
  account_id INTEGER,
  status TEXT NOT NULL,
  end_reason TEXT,
  error_summary TEXT,
  started_at TEXT NOT NULL,
  ended_at TEXT
);

CREATE TABLE IF NOT EXISTS request_trace_chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trace_id TEXT NOT NULL,
  layer TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS account_rpm_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id INTEGER NOT NULL,
  model TEXT NOT NULL,
  reservation_id TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_account_rpm_events_account_created
  ON account_rpm_events(account_id, created_at);

CREATE TABLE IF NOT EXISTS account_model_rate_limits (
  account_id INTEGER NOT NULL,
  model TEXT NOT NULL,
  limited_until TEXT NOT NULL,
  reason TEXT,
  probe_after TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (account_id, model)
);

CREATE TABLE IF NOT EXISTS account_model_inflight (
  reservation_id TEXT PRIMARY KEY,
  account_id INTEGER NOT NULL,
  model TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_account_model_inflight_model
  ON account_model_inflight(model, created_at);

CREATE TABLE IF NOT EXISTS sticky_sessions (
  caller_key TEXT NOT NULL,
  model TEXT NOT NULL,
  account_id INTEGER NOT NULL,
  api_key_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  last_used_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  PRIMARY KEY (caller_key, model)
);

CREATE INDEX IF NOT EXISTS idx_sticky_sessions_account
  ON sticky_sessions(account_id, expires_at);
