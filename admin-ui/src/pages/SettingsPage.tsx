import { FormEvent, useEffect, useState } from 'react';
import { Save } from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { api, CapacitySettings } from '../lib/api';

const fallbackSettings: CapacitySettings = {
  queueCapacity: 300,
  queueTimeoutSecs: 120,
  globalConcurrency: 12,
  modelConcurrency: 8,
  accountConcurrency: 1,
  maxRetries: 3,
  fallbackDelayMs: 350,
  modelCooldownSecs: 180,
  suspiciousCooldownSecs: 900,
  stickySessionMinutes: 30,
};

export function SettingsPage() {
  const [settings, setSettings] = useState<CapacitySettings>(fallbackSettings);
  const [globalInflight, setGlobalInflight] = useState(0);
  const [message, setMessage] = useState('');

  useEffect(() => {
    api.capacity()
      .then((data) => {
        setSettings(data.settings);
        setGlobalInflight(data.runtime.globalInflight);
      })
      .catch((err) => setMessage(err instanceof Error ? err.message : '读取失败'));
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    const data = await api.saveCapacity(settings);
    setSettings(data.settings);
    setMessage('设置已保存');
  }

  function setNumber(key: keyof CapacitySettings, value: string) {
    setSettings((current) => ({ ...current, [key]: Number(value) || 0 }));
  }

  return (
    <>
      <PageHeader title="容量设置" subtitle="调整请求排队、执行槽和重试节奏。" />
      <form className="panel stack capacity-panel" onSubmit={submit}>
        <div className="capacity-runtime">
          <span>正在执行</span>
          <strong>{globalInflight}</strong>
        </div>
        <div className="field-grid compact">
          <label>
            排队容量
            <input type="number" min={1} value={settings.queueCapacity} onChange={(event) => setNumber('queueCapacity', event.target.value)} />
          </label>
          <label>
            等待秒数
            <input type="number" min={1} value={settings.queueTimeoutSecs} onChange={(event) => setNumber('queueTimeoutSecs', event.target.value)} />
          </label>
          <label>
            全局执行
            <input type="number" min={1} value={settings.globalConcurrency} onChange={(event) => setNumber('globalConcurrency', event.target.value)} />
          </label>
          <label>
            单模型执行
            <input type="number" min={1} value={settings.modelConcurrency} onChange={(event) => setNumber('modelConcurrency', event.target.value)} />
          </label>
          <label>
            单账号执行
            <input type="number" min={1} value={settings.accountConcurrency} onChange={(event) => setNumber('accountConcurrency', event.target.value)} />
          </label>
          <label>
            最大重试
            <input type="number" min={0} value={settings.maxRetries} onChange={(event) => setNumber('maxRetries', event.target.value)} />
          </label>
          <label>
            换号间隔毫秒
            <input type="number" min={0} value={settings.fallbackDelayMs} onChange={(event) => setNumber('fallbackDelayMs', event.target.value)} />
          </label>
          <label>
            模型冷却秒数
            <input type="number" min={1} value={settings.modelCooldownSecs} onChange={(event) => setNumber('modelCooldownSecs', event.target.value)} />
          </label>
          <label>
            异常冷却秒数
            <input type="number" min={1} value={settings.suspiciousCooldownSecs} onChange={(event) => setNumber('suspiciousCooldownSecs', event.target.value)} />
          </label>
          <label>
            会话保持分钟
            <input type="number" min={1} value={settings.stickySessionMinutes} onChange={(event) => setNumber('stickySessionMinutes', event.target.value)} />
          </label>
        </div>
        {message ? <StateBlock message={message} /> : null}
        <button className="primary-button" type="submit">
          <Save size={16} />
          保存设置
        </button>
      </form>
    </>
  );
}
