import { useEffect, useMemo, useState } from 'react';
import { Ban, RefreshCw, RotateCcw, ShieldAlert } from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { useToast } from '../components/Toast';
import { Account, api } from '../lib/api';
import { accountIssueText, formatDateTime, primaryAccountState } from '../lib/display';

function accountName(account: Account) {
  return account.label || account.email;
}

function hasCredentialIssue(account: Account) {
  return account.availability?.kind === 'credential_missing' || (!account.credentialMask && !account.apiKey);
}

function hasIssue(account: Account) {
  return (
    !['ready', 'active', 'ok'].includes(account.status) ||
    account.errorCount > 0 ||
    !!account.lastError ||
    !!account.rateLimited ||
    hasCredentialIssue(account) ||
    Object.keys(account.modelRateLimits || {}).length > 0
  );
}

function issueReason(account: Account) {
  if (hasCredentialIssue(account)) return '需要重新保存密钥';
  if (account.rateLimited) return '账号暂时不可用';
  if (Object.keys(account.modelRateLimits || {}).length > 0) return '部分模型暂时不可用';
  return accountIssueText(account);
}

export function BansPage() {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [filter, setFilter] = useState<'all' | 'limited' | 'error' | 'credential'>('all');
  const [busy, setBusy] = useState('');
  const { showToast } = useToast();

  async function load() {
    const data = await api.accounts();
    setAccounts(data.accounts);
  }

  useEffect(() => {
    load().catch((err) => showToast(err instanceof Error ? err.message : '读取失败', 'error'));
  }, []);

  const issueAccounts = useMemo(() => accounts.filter(hasIssue), [accounts]);
  const visible = issueAccounts.filter((account) => {
    if (filter === 'limited') return account.rateLimited || Object.keys(account.modelRateLimits || {}).length > 0;
    if (filter === 'error') return account.errorCount > 0 || !!account.lastError || ['error', 'banned'].includes(account.status);
    if (filter === 'credential') return hasCredentialIssue(account);
    return true;
  });

  async function run(key: string, action: () => Promise<void>) {
    setBusy(key);
    try {
      await action();
      await load();
    } catch (err) {
      showToast(err instanceof Error ? err.message : '操作失败', 'error');
    } finally {
      setBusy('');
    }
  }

  return (
    <>
      <PageHeader title="异常监测" subtitle="集中处理暂时不可用、调用失败和需要补充信息的账号。" />
      <section className="metric-grid issue-metrics">
        <div className="metric-card">
          <ShieldAlert size={18} />
          <span>需要处理</span>
          <strong>{issueAccounts.length}</strong>
        </div>
        <div className="metric-card">
          <Ban size={18} />
          <span>暂时不可用</span>
          <strong>{issueAccounts.filter((item) => item.rateLimited).length}</strong>
        </div>
        <div className="metric-card">
          <RotateCcw size={18} />
          <span>最近失败</span>
          <strong>{issueAccounts.filter((item) => item.errorCount > 0 || item.lastError).length}</strong>
        </div>
        <div className="metric-card">
          <RefreshCw size={18} />
          <span>需要补充信息</span>
          <strong>{issueAccounts.filter(hasCredentialIssue).length}</strong>
        </div>
      </section>
      <div className="range-tabs">
        {[
          ['all', '全部'],
          ['limited', '暂时不可用'],
          ['error', '调用失败'],
          ['credential', '需要补充信息'],
        ].map(([key, label]) => (
          <button key={key} className={filter === key ? 'active' : ''} type="button" onClick={() => setFilter(key as typeof filter)}>
            {label}
          </button>
        ))}
      </div>
      <section className="panel issue-panel">
        <table>
          <thead>
            <tr>
              <th>账号</th>
              <th>状态</th>
              <th>原因</th>
              <th>最近使用</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((account) => {
              const state = primaryAccountState(account);
              return (
                <tr key={account.id}>
                  <td>
                    <div className="model-name-cell">
                      <strong>{accountName(account)}</strong>
                      <span>{account.email}</span>
                    </div>
                  </td>
                  <td><span className={`status-badge ${state.className}`}>{state.label}</span></td>
                  <td>{issueReason(account)}</td>
                  <td>{formatDateTime(account.lastUsed)}</td>
                  <td>
                    <div className="row-actions">
                      <button className="text-button" type="button" disabled={!!busy} onClick={() => run(`reset-${account.id}`, async () => { await api.resetAccountErrors(account.id); })}>
                        重置错误
                      </button>
                      <button className="text-button" type="button" disabled={!!busy} onClick={() => run(`limit-${account.id}`, async () => { await api.clearAccountRateLimit(account.id); })}>
                        解除限制
                      </button>
                      <button className="text-button" type="button" disabled={!!busy} onClick={() => run(`credits-${account.id}`, () => api.refreshAccountCredits(account.id).then(() => undefined))}>
                        刷新状态
                      </button>
                    </div>
                  </td>
                </tr>
              );
            })}
            {visible.length === 0 ? (
              <tr>
                <td colSpan={5}>
                  <StateBlock message="当前没有需要处理的账号。" />
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </section>
    </>
  );
}
