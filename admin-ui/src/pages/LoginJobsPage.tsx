import { FormEvent, useEffect, useMemo, useState } from 'react';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { LoginJob, LoginJobEvent, api, getToken, handleAuthResponse } from '../lib/api';
import { eventTypeText, formatDateTime, jobStatusText } from '../lib/display';

export function LoginJobsPage() {
  const [jobs, setJobs] = useState<LoginJob[]>([]);
  const [text, setText] = useState('');
  const [delayMinSecs, setDelayMinSecs] = useState(15);
  const [delayMaxSecs, setDelayMaxSecs] = useState(45);
  const [failDelayMinSecs, setFailDelayMinSecs] = useState(60);
  const [failDelayMaxSecs, setFailDelayMaxSecs] = useState(180);
  const [error, setError] = useState('');
  const [activeJobId, setActiveJobId] = useState('');
  const [events, setEvents] = useState<LoginJobEvent[]>([]);
  const [waiting, setWaiting] = useState<LoginJobEvent | null>(null);

  const lineCount = useMemo(() => text.split('\n').map((line) => line.trim()).filter(Boolean).length, [text]);

  async function load() {
    const data = await api.loginJobs();
    setJobs(data.jobs);
  }

  useEffect(() => {
    load().catch((err) => setError(err instanceof Error ? err.message : '读取失败'));
    const timer = window.setInterval(() => load().catch(() => undefined), 3000);
    return () => window.clearInterval(timer);
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError('');
    try {
      const result = await api.createLoginJob({ text, delayMinSecs, delayMaxSecs, failDelayMinSecs, failDelayMaxSecs });
      setActiveJobId(result.id);
      setEvents([]);
      followJob(result.id);
      setText('');
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : '创建失败');
    }
  }

  async function cancel(id: string) {
    await api.cancelLoginJob(id);
    await load();
  }

  async function followJob(id: string) {
    const resp = await fetch(`/admin/login-jobs/${id}/events`, {
      headers: { authorization: `Bearer ${getToken()}` },
    });
    if (!resp.ok) {
      handleAuthResponse(resp);
      throw new Error(`读取进度失败：${resp.status}`);
    }
    if (!resp.body) {
      return;
    }
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
        if (eventType === 'close') return;
        if (eventType === 'waiting' && payload.seconds) {
          setWaiting({ ...payload, type: eventType });
        } else {
          setWaiting(null);
        }
        setEvents((current) => [{ ...payload, type: eventType }, ...current].slice(0, 80));
      }
    }
  }

  return (
    <>
      <PageHeader title="批量登录" subtitle="每行一个账号，可在最前面加代理；系统会逐个处理并在两次尝试之间等待。" />
      <section className="two-column">
        <form className="panel stack" onSubmit={submit}>
          <label>
            账号内容
            <textarea rows={12} value={text} onChange={(event) => setText(event.target.value)} placeholder="email@example.com password 或 http://user:pass@host:port email@example.com password" />
          </label>
          <div className="field-grid">
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
          {error ? <StateBlock message={error} /> : null}
          <button className="primary-button" disabled={lineCount === 0} type="submit">
            创建 {lineCount} 行任务
          </button>
        </form>
        <section className="panel">
          <h2>任务列表</h2>
          <table>
            <thead>
              <tr>
                <th>任务</th>
                <th>状态</th>
                <th>进度</th>
                <th>创建时间</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.id}>
                  <td>
                    <button className="text-button" type="button" onClick={() => { setActiveJobId(job.id); setEvents([]); followJob(job.id); }}>
                      {job.id.slice(0, 8)}
                    </button>
                  </td>
                  <td>{jobStatusText(job.status)}</td>
                  <td>
                    {job.successCount + job.failedCount}/{job.total}
                  </td>
                  <td>{formatDateTime(job.createdAt)}</td>
                  <td>
                    {job.status === 'running' ? (
                      <button className="text-button" type="button" onClick={() => cancel(job.id)}>
                        停止
                      </button>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </section>
      <section className="panel job-events">
        <h2>实时进度 {activeJobId ? activeJobId.slice(0, 8) : ''}</h2>
        {waiting ? <StateBlock message={`${waiting.reason === 'failed' ? '失败后继续' : '稍后继续'}，剩余 ${waiting.seconds || 0} 秒。`} /> : null}
        {events.length === 0 ? <StateBlock message="创建或选择一个任务后查看进度。" /> : null}
        <div className="event-list">
          {events.map((event, index) => (
              <article className={`event-item event-${event.type}`} key={`${event.type}-${index}`}>
                <strong>{eventTypeText(event.type)}</strong>
                <span>{event.emailMasked || event.message || event.errorCode || '-'}</span>
                {event.waitingUntil ? <small>{formatDateTime(event.waitingUntil)}</small> : null}
                {event.index ? <small>第 {event.index} 行</small> : null}
              </article>
          ))}
        </div>
      </section>
    </>
  );
}
