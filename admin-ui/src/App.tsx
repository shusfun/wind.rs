import { NavLink, Navigate, Route, Routes, useNavigate } from 'react-router-dom';
import type { ReactNode } from 'react';
import {
  Activity,
  BarChart3,
  Cable,
  Gauge,
  HeartPulse,
  KeyRound,
  LayoutDashboard,
  LogOut,
  ServerCog,
  Settings,
  SlidersHorizontal,
  Users,
} from 'lucide-react';
import { clearToken, getToken } from './lib/api';
import { ToastProvider } from './components/Toast';
import { AccountsPage } from './pages/AccountsPage';
import { ClientApiKeysPage } from './pages/ClientApiKeysPage';
import { DashboardPage } from './pages/DashboardPage';
import { LoginPage } from './pages/LoginPage';
import { ProxiesPage } from './pages/ProxiesPage';
import { RequestsPage } from './pages/RequestsPage';
import { SettingsPage } from './pages/SettingsPage';
import { SetupPage } from './pages/SetupPage';

function RequireLogin({ children }: { children: ReactNode }) {
  return getToken() ? children : <Navigate to="/login" replace />;
}

const navGroups = [
  {
    label: '概览',
    items: [
      { to: '/dashboard', label: '仪表盘', icon: LayoutDashboard },
      { to: '/stats', label: '统计分析', icon: BarChart3 },
    ],
  },
  {
    label: '账号',
    items: [
      { to: '/accounts', label: '账号管理', icon: Users },
      { to: '/bans', label: '异常监测', icon: HeartPulse },
    ],
  },
  {
    label: '系统',
    items: [
      { to: '/models', label: '模型控制', icon: SlidersHorizontal },
      { to: '/proxies', label: '代理配置', icon: Cable },
      { to: '/client-api-keys', label: '调用密钥', icon: KeyRound },
      { to: '/requests', label: '调用记录', icon: Activity },
      { to: '/settings', label: '容量设置', icon: Settings },
    ],
  },
];

function PlaceholderPage({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <>
      <header className="page-header">
        <div>
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
      </header>
      <section className="panel empty-panel">
        <Gauge size={34} />
        <strong>稍后在这里查看</strong>
        <span>当前先完成账号管理和调用排查，其他页面会按这个界面结构补齐。</span>
      </section>
    </>
  );
}

function Shell() {
  const navigate = useNavigate();
  const logout = () => {
    clearToken();
    navigate('/login', { replace: true });
  };

  return (
    <ToastProvider>
      <div className="shell">
        <aside className="sidebar">
          <div className="brand">
            <ServerCog size={26} />
            <div>
              <strong>Windsurf</strong>
              <span>管理控制台</span>
            </div>
          </div>
          <nav>
            {navGroups.map((group) => (
              <div className="nav-group" key={group.label}>
                <div className="nav-group-label">{group.label}</div>
                {group.items.map((item) => {
                  const Icon = item.icon;
                  return (
                    <NavLink key={item.to} to={item.to} className={({ isActive }) => (isActive ? 'nav-item active' : 'nav-item')}>
                      <Icon size={18} />
                      <span>{item.label}</span>
                    </NavLink>
                  );
                })}
              </div>
            ))}
          </nav>
          <button className="ghost-button logout" type="button" onClick={logout}>
            <LogOut size={18} />
            退出
          </button>
        </aside>
        <main className="main-panel">
          <Routes>
            <Route path="/dashboard" element={<DashboardPage />} />
            <Route path="/stats" element={<PlaceholderPage title="统计分析" subtitle="查看请求量、成功率和账号使用情况。" />} />
            <Route path="/accounts" element={<AccountsPage />} />
            <Route path="/bans" element={<PlaceholderPage title="异常监测" subtitle="查看需要处理的账号和最近失败原因。" />} />
            <Route path="/models" element={<PlaceholderPage title="模型控制" subtitle="配置可使用的模型范围。" />} />
            <Route path="/client-api-keys" element={<ClientApiKeysPage />} />
            <Route path="/requests" element={<RequestsPage />} />
            <Route path="/proxies" element={<ProxiesPage />} />
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="*" element={<Navigate to="/dashboard" replace />} />
          </Routes>
        </main>
      </div>
    </ToastProvider>
  );
}

export function App() {
  return (
    <Routes>
      <Route path="/setup" element={<SetupPage />} />
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/*"
        element={
          <RequireLogin>
            <Shell />
          </RequireLogin>
        }
      />
      <Route
        path="*"
        element={
          <RequireLogin>
            <Shell />
          </RequireLogin>
        }
      />
    </Routes>
  );
}
