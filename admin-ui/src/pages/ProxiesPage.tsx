import { FormEvent, useEffect, useState } from 'react';
import { PageHeader } from '../components/PageHeader';
import { useToast } from '../components/Toast';
import { ProxyItem, api } from '../lib/api';

function proxyStatusText(value: string) {
  const labels: Record<string, string> = {
    ok: '可用',
    ready: '可用',
    unknown: '未检测',
    error: '异常',
    disabled: '已停用',
  };
  return labels[value] || '未检测';
}

export function ProxiesPage() {
  const [proxies, setProxies] = useState<ProxyItem[]>([]);
  const [name, setName] = useState('');
  const [url, setUrl] = useState('');
  const { showToast } = useToast();

  async function load() {
    const data = await api.proxies();
    setProxies(data.proxies);
  }

  useEffect(() => {
    load().catch((err) => showToast(err instanceof Error ? err.message : '读取失败', 'error'));
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    try {
      await api.createProxy({ name, url });
      setName('');
      setUrl('');
      showToast('代理已添加', 'success');
      await load();
    } catch (err) {
      showToast(err instanceof Error ? err.message : '保存失败', 'error');
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
                <td>{proxyStatusText(proxy.status)}</td>
                <td>{proxy.lastError || '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </>
  );
}
