import { useEffect, useState } from 'react';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { RequestTrace, TraceChunk, api } from '../lib/api';

export function RequestsPage() {
  const [requests, setRequests] = useState<RequestTrace[]>([]);
  const [chunks, setChunks] = useState<TraceChunk[]>([]);
  const [selected, setSelected] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    api.requests()
      .then((data) => setRequests(data.requests))
      .catch((err) => setError(err instanceof Error ? err.message : '读取失败'));
  }, []);

  async function open(id: string) {
    setSelected(id);
    const data = await api.requestDetail(id);
    setChunks(data.chunks);
  }

  return (
    <>
      <PageHeader title="调用记录" subtitle="查看输入、输出和结束原因，用来定位异常请求。" />
      {error ? <StateBlock message={error} /> : null}
      <section className="two-column wide">
        <section className="panel">
          <h2>最近请求</h2>
          <table>
            <thead>
              <tr>
                <th>请求</th>
                <th>模型</th>
                <th>状态</th>
                <th>结束原因</th>
              </tr>
            </thead>
            <tbody>
              {requests.map((item) => (
                <tr key={item.id} className={selected === item.id ? 'selected-row' : ''} onClick={() => open(item.id)}>
                  <td>{item.id.slice(0, 8)}</td>
                  <td>{item.model || '-'}</td>
                  <td>{item.status}</td>
                  <td>{item.endReason || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
        <section className="panel">
          <h2>详情</h2>
          {chunks.length === 0 ? <StateBlock message="选择一条请求后查看详情。" /> : null}
          <div className="trace-list">
            {chunks.map((chunk) => (
              <article key={chunk.id} className="trace-item">
                <strong>{chunk.layer}</strong>
                <pre>{chunk.payload}</pre>
              </article>
            ))}
          </div>
        </section>
      </section>
    </>
  );
}
