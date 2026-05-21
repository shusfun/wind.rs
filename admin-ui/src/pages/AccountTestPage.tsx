import { FormEvent, useEffect, useMemo, useState } from 'react';
import { Play, Square } from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { Account, AccountModel, api } from '../lib/api';

function modelLabel(model: AccountModel) {
  return model.label || model.id;
}

function accountLabel(account: Account) {
  return account.label || account.email || `账号 ${account.id}`;
}

export function AccountTestPage() {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [models, setModels] = useState<AccountModel[]>([]);
  const [accountId, setAccountId] = useState('');
  const [model, setModel] = useState('claude-opus-4.7');
  const [message, setMessage] = useState('用一句话确认这个账号可以正常回复。');
  const [output, setOutput] = useState('');
  const [status, setStatus] = useState('');
  const [running, setRunning] = useState(false);
  const [aborter, setAborter] = useState<AbortController | null>(null);

  useEffect(() => {
    Promise.all([api.accounts(), api.accountTestDefaults()])
      .then(([accountData, defaults]) => {
        setAccounts(accountData.accounts);
        setModels(defaults.models);
        setModel(defaults.model || 'claude-opus-4.7');
        setMessage(defaults.message || '用一句话确认这个账号可以正常回复。');
      })
      .catch((err) => setStatus(err instanceof Error ? err.message : '读取失败'));
  }, []);

  const activeAccounts = useMemo(
    () => accounts.filter((account) => ['ready', 'active', 'ok'].includes(account.status)),
    [accounts],
  );

  async function submit(event: FormEvent) {
    event.preventDefault();
    const controller = new AbortController();
    setAborter(controller);
    setRunning(true);
    setOutput('');
    setStatus('正在发送');
    try {
      const resp = await api.runAccountTest({
        accountId: accountId ? Number(accountId) : undefined,
        model,
        message,
        stream: true,
        saveDefaults: true,
      });
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
            setStatus(`已选择账号 #${payload.accountId}`);
          }
          const text = payload.delta?.text || payload.delta?.text_delta || payload.delta?.text || payload.delta?.textDelta;
          const nested = payload.delta?.type === 'text_delta' ? payload.delta.text : '';
          const next = text || nested || '';
          if (next) {
            setOutput((current) => current + next);
          }
          if (payload.type === 'message_stop') {
            setStatus('测试完成');
          }
        }
      }
    } catch (err) {
      if (!controller.signal.aborted) {
        setStatus(err instanceof Error ? err.message : '测试失败');
      }
    } finally {
      setRunning(false);
      setAborter(null);
    }
  }

  function stop() {
    aborter?.abort();
    setRunning(false);
    setStatus('已停止');
  }

  return (
    <>
      <PageHeader title="账号测试" subtitle="选择模型和内容，查看账号是否能正常返回。" />
      <section className="two-column wide">
        <form className="panel stack" onSubmit={submit}>
          <label>
            账号
            <select value={accountId} onChange={(event) => setAccountId(event.target.value)}>
              <option value="">自动选择可用账号</option>
              {activeAccounts.map((account) => (
                <option key={account.id} value={account.id}>
                  #{account.id} {accountLabel(account)}
                </option>
              ))}
            </select>
          </label>
          <label>
            模型
            <select value={model} onChange={(event) => setModel(event.target.value)}>
              {models.map((item) => (
                <option key={item.id} value={item.id}>
                  {modelLabel(item)}
                </option>
              ))}
            </select>
          </label>
          <label>
            发送内容
            <textarea rows={8} value={message} onChange={(event) => setMessage(event.target.value)} />
          </label>
          <div className="modal-actions">
            {running ? (
              <button className="secondary-button" type="button" onClick={stop}>
                <Square size={16} />
                停止
              </button>
            ) : null}
            <button className="primary-button" type="submit" disabled={running}>
              <Play size={16} />
              开始测试
            </button>
          </div>
          {status ? <StateBlock message={status} /> : null}
        </form>
        <section className="panel stack">
          <div className="section-heading flat">
            <div>
              <h2>返回内容</h2>
              <p>开始测试后会实时显示输出。</p>
            </div>
          </div>
          <pre className="stream-output">{output || '等待返回'}</pre>
        </section>
      </section>
    </>
  );
}
