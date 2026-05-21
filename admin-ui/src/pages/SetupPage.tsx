import { FormEvent, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { SecretInput } from '../components/SecretInput';
import { api, setToken } from '../lib/api';

export function SetupPage() {
  const navigate = useNavigate();
  const [adminKey, setAdminKey] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError('');
    try {
      await api.install(adminKey);
      const login = await api.login(adminKey);
      setToken(login.token);
      navigate('/dashboard', { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : '初始化失败');
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="auth-screen">
      <form className="auth-panel" onSubmit={submit}>
        <span className="eyebrow">首次使用</span>
        <h1>设置管理 key</h1>
        <p>设置后可进入管理台，后续登录时使用同一个 key。</p>
        <label>
          管理 key
          <SecretInput autoComplete="new-password" minLength={12} value={adminKey} onChange={setAdminKey} />
        </label>
        {error ? <div className="form-error">{error}</div> : null}
        <button className="primary-button" disabled={busy} type="submit">
          {busy ? '正在保存' : '完成设置'}
        </button>
      </form>
    </main>
  );
}
