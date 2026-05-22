import { FormEvent, useEffect, useState } from 'react';
import { Check, KeyRound, Plus, Trash2, X } from 'lucide-react';
import { useToast } from '../components/Toast';
import { ClientApiKey, api } from '../lib/api';

function formatTime(value?: string | null) {
  return value ? new Date(value).toLocaleString() : '-';
}

export function ClientApiKeysPage() {
  const [keys, setKeys] = useState<ClientApiKey[]>([]);
  const [name, setName] = useState('');
  const [manualKey, setManualKey] = useState('');
  const [visibleKey, setVisibleKey] = useState<{ id: number; key: string } | null>(null);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editingName, setEditingName] = useState('');
  const [addOpen, setAddOpen] = useState(false);
  const [busy, setBusy] = useState('');
  const { showToast } = useToast();

  async function load() {
    const data = await api.clientApiKeys();
    setKeys(data.keys);
  }

  useEffect(() => {
    load().catch((err) => showToast(err instanceof Error ? err.message : '读取失败', 'error'));
  }, []);

  async function createKey(event: FormEvent) {
    event.preventDefault();
    setBusy('create');
    setVisibleKey(null);
    try {
      const created = await api.createClientApiKey({
        name,
        key: manualKey,
        enabled: true,
      });
      setName('');
      setManualKey('');
      setVisibleKey({ id: created.id, key: created.key });
      setAddOpen(false);
      showToast('密钥已添加', 'success');
      await load();
    } catch (err) {
      showToast(err instanceof Error ? err.message : '添加失败', 'error');
    } finally {
      setBusy('');
    }
  }

  function startEditName(item: ClientApiKey) {
    setEditingId(item.id);
    setEditingName(item.name);
  }

  function cancelEditName() {
    setEditingId(null);
    setEditingName('');
  }

  async function updateName(item: ClientApiKey) {
    setBusy(`name-${item.id}`);
    try {
      await api.updateClientApiKey(item.id, { name: editingName });
      setEditingId(null);
      setEditingName('');
      showToast('名称已保存', 'success');
      await load();
    } catch (err) {
      showToast(err instanceof Error ? err.message : '保存失败', 'error');
    } finally {
      setBusy('');
    }
  }

  async function setEnabled(item: ClientApiKey, enabled: boolean) {
    setBusy(`enabled-${item.id}`);
    try {
      await api.updateClientApiKey(item.id, { enabled });
      showToast(enabled ? '密钥已启用' : '密钥已停用', 'success');
      await load();
    } catch (err) {
      showToast(err instanceof Error ? err.message : '保存失败', 'error');
    } finally {
      setBusy('');
    }
  }

  async function deleteKey(item: ClientApiKey) {
    setBusy(`delete-${item.id}`);
    try {
      await api.deleteClientApiKey(item.id);
      showToast('密钥已删除', 'success');
      await load();
    } catch (err) {
      showToast(err instanceof Error ? err.message : '删除失败', 'error');
    } finally {
      setBusy('');
    }
  }

  async function copyKey(item: ClientApiKey) {
    const value = visibleKey?.id === item.id ? visibleKey.key : item.key;
    if (!value) {
      showToast('这个密钥不能查看，请新增一个密钥', 'error');
      return;
    }
    await navigator.clipboard.writeText(value);
    showToast('密钥已复制', 'success');
  }

  return (
    <>
      <header className="page-header">
        <div>
          <h1>调用密钥</h1>
          <p>管理可调用模型和对话接口的密钥。</p>
        </div>
        <button className="primary-button" type="button" onClick={() => setAddOpen(true)}>
          <Plus size={16} />
          新增
        </button>
      </header>
      <section className="stack">
        <section className="panel api-key-table-panel">
          <div className="table-title">
            <h2>已有密钥</h2>
            <span>{keys.length} 个</span>
          </div>
          <table>
            <thead>
              <tr>
                <th>名称</th>
                <th>密钥</th>
                <th>状态</th>
                <th>最近使用</th>
                <th>创建时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {keys.map((item) => (
                <tr key={item.id}>
                  <td>
                    {editingId === item.id ? (
                      <div className="inline-edit">
                        <input value={editingName} autoFocus onChange={(event) => setEditingName(event.target.value)} />
                        <button className="icon-button" disabled={busy === `name-${item.id}`} type="button" onClick={() => updateName(item)} title="保存">
                          <Check size={15} />
                        </button>
                        <button className="icon-button" type="button" onClick={cancelEditName} title="取消">
                          <X size={15} />
                        </button>
                      </div>
                    ) : (
                      <button className="name-button" type="button" onClick={() => startEditName(item)}>
                        {item.name}
                      </button>
                    )}
                  </td>
                  <td>
                    <button className="key-code key-copy" type="button" onClick={() => copyKey(item)}>
                      {visibleKey?.id === item.id ? visibleKey.key : item.key || item.keyMask}
                    </button>
                  </td>
                  <td>
                    <span className={item.enabled ? 'status-badge' : 'status-badge status-disabled'}>{item.enabled ? '可用' : '已停用'}</span>
                  </td>
                  <td>{formatTime(item.lastUsedAt)}</td>
                  <td>{formatTime(item.createdAt)}</td>
                  <td>
                    <div className="row-actions">
                      <button className="text-button" disabled={busy === `enabled-${item.id}`} type="button" onClick={() => setEnabled(item, !item.enabled)}>
                        {item.enabled ? '停用' : '启用'}
                      </button>
                      <button className="icon-button danger" disabled={busy === `delete-${item.id}`} type="button" onClick={() => deleteKey(item)} title="删除">
                        <Trash2 size={15} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {keys.length === 0 ? (
            <div className="empty-panel inline-empty">
              <KeyRound size={30} />
              <strong>还没有密钥</strong>
              <span>添加后，请求带上密钥即可使用模型和对话接口。</span>
            </div>
          ) : null}
        </section>
      </section>
      {addOpen ? (
        <div className="modal-backdrop">
          <form className="modal-panel api-key-modal" onSubmit={createKey}>
            <div className="modal-head">
              <div>
                <h2>新增调用密钥</h2>
                <p>添加后，请求带上这个密钥即可使用模型和对话接口。</p>
              </div>
              <button className="icon-button" type="button" onClick={() => setAddOpen(false)} title="关闭">
                <X size={16} />
              </button>
            </div>
            <section className="modal-section stack">
              <label>
                名称
                <input value={name} onChange={(event) => setName(event.target.value)} placeholder="例如：本地 Claude Code" />
              </label>
              <label>
                密钥
                <input value={manualKey} onChange={(event) => setManualKey(event.target.value)} placeholder="留空会自动生成" />
              </label>
              <div className="modal-actions">
                <button className="secondary-button" type="button" onClick={() => setAddOpen(false)}>
                  取消
                </button>
                <button className="primary-button" disabled={busy === 'create'} type="submit">
                  <Plus size={16} />
                  添加
                </button>
              </div>
            </section>
          </form>
        </div>
      ) : null}
    </>
  );
}
