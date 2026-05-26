import { FormEvent, useEffect, useMemo, useState } from 'react';
import { Save, SlidersHorizontal } from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { useToast } from '../components/Toast';
import { AdminModelConfig, api } from '../lib/api';
import { providerText } from '../lib/display';

function modelLabel(item: { id: string; label?: string }) {
  return item.label || item.id;
}

export function ModelsPage() {
  const [config, setConfig] = useState<AdminModelConfig | null>(null);
  const [defaultModel, setDefaultModel] = useState('');
  const [disabledModels, setDisabledModels] = useState<Set<string>>(new Set());
  const [error, setError] = useState('');
  const { showToast } = useToast();

  async function load() {
    const data = await api.modelConfig();
    setConfig(data);
    setDefaultModel(data.defaultModel);
    setDisabledModels(new Set(data.disabledModels));
  }

  useEffect(() => {
    load().catch((err) => setError(err instanceof Error ? err.message : '读取失败'));
  }, []);

  const enabledCount = useMemo(() => config?.models.filter((item) => !disabledModels.has(item.id)).length || 0, [config, disabledModels]);

  function toggleModel(id: string, enabled: boolean) {
    setDisabledModels((current) => {
      const next = new Set(current);
      if (enabled) {
        next.delete(id);
      } else {
        next.add(id);
        if (defaultModel === id) {
          const replacement = config?.models.find((item) => item.id !== id && !next.has(item.id));
          if (replacement) setDefaultModel(replacement.id);
        }
      }
      return next;
    });
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!defaultModel) {
      showToast('请选择默认模型', 'error');
      return;
    }
    try {
      const data = await api.saveModelConfig({ defaultModel, disabledModels: Array.from(disabledModels) });
      setConfig(data);
      setDefaultModel(data.defaultModel);
      setDisabledModels(new Set(data.disabledModels));
      showToast('模型设置已保存', 'success');
    } catch (err) {
      showToast(err instanceof Error ? err.message : '保存失败', 'error');
    }
  }

  return (
    <>
      <PageHeader title="模型控制" subtitle="选择默认模型，并停用暂时不希望被调用的模型。" />
      {error ? <StateBlock message={error} /> : null}
      {!config ? <StateBlock message="正在读取模型设置。" /> : null}
      {config ? (
        <form className="stack" onSubmit={submit}>
          <section className="model-control-bar">
            <div>
              <SlidersHorizontal size={18} />
              <span>可用模型</span>
              <strong>{enabledCount}/{config.models.length}</strong>
            </div>
            <label>
              默认模型
              <select value={defaultModel} onChange={(event) => setDefaultModel(event.target.value)}>
                {config.models
                  .filter((item) => !disabledModels.has(item.id))
                  .map((item) => (
                    <option key={item.id} value={item.id}>
                      {modelLabel(item)}
                    </option>
                  ))}
              </select>
            </label>
            <button className="primary-button" type="submit">
              <Save size={16} />
              保存模型设置
            </button>
          </section>
          <section className="panel models-panel">
            <table>
              <thead>
                <tr>
                  <th>模型</th>
                  <th>提供方</th>
                  <th>倍率</th>
                  <th>覆盖账号</th>
                  <th>限流账号</th>
                  <th>近 24 小时失败</th>
                  <th>状态</th>
                </tr>
              </thead>
              <tbody>
                {config.models.map((item) => {
                  const enabled = !disabledModels.has(item.id);
                  const isDefault = defaultModel === item.id;
                  return (
                    <tr key={item.id}>
                      <td>
                        <div className="model-name-cell">
                          <strong>{modelLabel(item)}</strong>
                          <span>{item.id}</span>
                          {isDefault ? <small>默认使用</small> : null}
                        </div>
                      </td>
                      <td>{providerText(item.provider)}</td>
                      <td>{item.creditMultiplier ?? '-'}</td>
                      <td>{item.accountCount}</td>
                      <td>{item.limitedAccountCount}</td>
                      <td>{item.recentFailures}</td>
                      <td>
                        <label className="switch-row">
                          <input type="checkbox" checked={enabled} disabled={isDefault} onChange={(event) => toggleModel(item.id, event.target.checked)} />
                          <span>{enabled ? '可调用' : '已停用'}</span>
                        </label>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </section>
        </form>
      ) : null}
    </>
  );
}
