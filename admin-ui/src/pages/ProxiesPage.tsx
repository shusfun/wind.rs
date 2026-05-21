import { FormEvent, useEffect, useState } from 'react';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { ProxyItem, api } from '../lib/api';

export function ProxiesPage() {
  const [proxies, setProxies] = useState<ProxyItem[]>([]);
  const [name, setName] = useState('');
  const [url, setUrl] = useState('');
  const [error, setError] = useState('');

  async function load() {
    const data = await api.proxies();
    setProxies(data.proxies);
  }

  useEffect(() => {
    load().catch((err) => setError(err instanceof Error ? err.message : '读取失败'));
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError('');
    try {
      await api.createProxy({ name, url });
      setName('');
      setUrl('');
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : '保存失败');
    }
  }

  return (
    <>
      <PageHeader title="代理" subtitle="维护账号可使用的代理地址。" />
      <section className="panel">
        <form className="inline-form" onSubmit={submit}>
          <input placeholder="名称" value={name} onChange={(event) => setName(event.target.value)} />
          <input placeholder="代理地址" value={url} onChange={(event) => setUrl(event.target.value)} />
          <button className="primary-button" type="submit">
            添加代理
          </button>
        </form>
        {error ? <StateBlock message={error} /> : null}
        <table>
          <thead>
            <tr>
              <th>名称</th>
              <th>地址</th>
              <th>状态</th>
              <th>最近错误</th>
            </tr>
          </thead>
          <tbody>
            {proxies.map((proxy) => (
              <tr key={proxy.id}>
                <td>{proxy.name}</td>
                <td>{proxy.url}</td>
                <td>{proxy.status}</td>
                <td>{proxy.lastError || '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </>
  );
}
