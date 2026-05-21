import { FormEvent, useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { SecretInput } from '../components/SecretInput';
import { api, setToken } from '../lib/api';

export function LoginPage() {
  const navigate = useNavigate();
  const [adminKey, setAdminKey] = useState('');
  const [needsSetup, setNeedsSetup] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.setupStatus().then((data) => setNeedsSetup(data.needsSetup)).catch(() => setNeedsSetup(false));
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError('');
    try {
      const data = await api.login(adminKey);
      setToken(data.token);
      navigate('/dashboard', { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : '登录失败');
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="auth-screen">
      <form className="auth-panel" onSubmit={submit}>
        <span className="eyebrow">管理台</span>
        <h1>输入管理 key</h1>
        <p>登录后可以管理账号、批量登录任务、代理和调用记录。</p>
        <label>
          管理 key
          <SecretInput autoComplete="current-password" value={adminKey} onChange={setAdminKey} />
        </label>
        {error ? <div className="form-error">{error}</div> : null}
        <button className="primary-button" disabled={busy} type="submit">
          {busy ? '正在登录' : '进入管理台'}
        </button>
        {needsSetup ? <Link to="/setup">还没有设置管理 key</Link> : null}
      </form>
    </main>
  );
}
