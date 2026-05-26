import { FormEvent, useEffect, useState } from 'react';
import { Save } from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { useToast } from '../components/Toast';
import { api, AdminSettings, CapacitySettings, SystemPromptMode } from '../lib/api';

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

const fallbackAdminSettings: AdminSettings = {
  systemPromptMode: 'strip-identity',
};

const systemPromptModeOptions: Array<{ value: SystemPromptMode; label: string; description: string }> = [
  {
    value: 'passthrough',
    label: '保持原文',
    description: '按调用方提交的内容发送。',
  },
  {
    value: 'strip-identity',
    label: '清理身份描述',
    description: '保留用户要求，减少额外身份说明。',
  },
  {
    value: 'windsurf-wrap',
    label: '增强兼容性',
    description: '让请求内容更贴近目标服务的格式。',
  },
];

export function SettingsPage() {
  const [settings, setSettings] = useState<CapacitySettings>(fallbackSettings);
  const [adminSettings, setAdminSettings] = useState<AdminSettings>(fallbackAdminSettings);
  const [globalInflight, setGlobalInflight] = useState(0);
  const { showToast } = useToast();

  useEffect(() => {
    api.capacity()
      .then((data) => {
        setSettings(data.settings);
        setGlobalInflight(data.runtime.globalInflight);
      })
      .catch((err) => showToast(err instanceof Error ? err.message : '读取失败', 'error'));
    api.settings()
      .then((data) => {
        setAdminSettings({
          systemPromptMode: data.systemPromptMode || fallbackAdminSettings.systemPromptMode,
        });
      })
      .catch((err) => showToast(err instanceof Error ? err.message : '读取失败', 'error'));
  }, []);

  async function submitCapacity(event: FormEvent) {
    event.preventDefault();
    const data = await api.saveCapacity(settings);
    setSettings(data.settings);
    showToast('设置已保存', 'success');
  }

  async function submitAdminSettings(event: FormEvent) {
    event.preventDefault();
    await api.saveSettings(adminSettings);
    showToast('请求设置已保存', 'success');
  }

  function setNumber(key: keyof CapacitySettings, value: string) {
    setSettings((current) => ({ ...current, [key]: Number(value) || 0 }));
  }

  return (
    <>
      <PageHeader title="容量设置" subtitle="调整请求排队、执行槽和重试节奏。" />
      <form className="panel stack capacity-panel" onSubmit={submitAdminSettings}>
        <section className="settings-section">
          <div>
            <h3>请求内容</h3>
            <p>选择发送前的内容处理方式，保存后新的调用会使用该设置。</p>
          </div>
          <label>
            内容处理
            <select
              value={adminSettings.systemPromptMode}
              onChange={(event) =>
                setAdminSettings((current) => ({
                  ...current,
                  systemPromptMode: event.target.value as SystemPromptMode,
                }))
              }
            >
              {systemPromptModeOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <div className="setting-option-hints">
            {systemPromptModeOptions.map((option) => (
              <span key={option.value} className={option.value === adminSettings.systemPromptMode ? 'active' : ''}>
                <strong>{option.label}</strong>
                {option.description}
              </span>
            ))}
          </div>
          <button className="primary-button" type="submit">
            <Save size={16} />
            保存内容设置
          </button>
        </section>
      </form>
      <form className="panel stack capacity-panel" onSubmit={submitCapacity}>
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
        <button className="primary-button" type="submit">
          <Save size={16} />
          保存设置
        </button>
      </form>
    </>
  );
}
