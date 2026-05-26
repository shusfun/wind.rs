import { useEffect, useState } from 'react';
import { Activity, Clock3, ShieldAlert, TrendingUp } from 'lucide-react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { AdminStats, StatsRangeKey, api } from '../lib/api';

const ranges: Array<{ key: StatsRangeKey; label: string }> = [
  { key: '24h', label: '近 24 小时' },
  { key: '7d', label: '近 7 天' },
  { key: '30d', label: '近 30 天' },
];

function formatDuration(ms: number) {
  if (!ms) return '0 ms';
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(1)} s`;
}

function rateClass(value: number) {
  if (value >= 95) return 'success';
  if (value >= 80) return 'warn';
  return 'danger';
}

const chartColors = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#38bdf8', '#a855f7'];

function chartTooltipStyle() {
  return {
    background: '#111114',
    border: '1px solid #32323c',
    borderRadius: 8,
    color: '#f4f4f5',
  };
}

export function StatsPage() {
  const [range, setRange] = useState<StatsRangeKey>('24h');
  const [stats, setStats] = useState<AdminStats | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    setError('');
    api.stats(range)
      .then(setStats)
      .catch((err) => setError(err instanceof Error ? err.message : '读取失败'));
  }, [range]);

  if (error) {
    return <StateBlock message={error} />;
  }

  return (
    <>
      <PageHeader title="统计分析" subtitle="查看调用、账号和任务的整体表现。" />
      <div className="range-tabs">
        {ranges.map((item) => (
          <button key={item.key} className={range === item.key ? 'active' : ''} type="button" onClick={() => setRange(item.key)}>
            {item.label}
          </button>
        ))}
      </div>
      {!stats ? <StateBlock message="正在读取统计数据。" /> : null}
      {stats ? (
        <>
          <section className="metric-grid stats-metrics">
            <div className="metric-card">
              <Activity size={18} />
              <span>调用次数</span>
              <strong>{stats.overview.requests}</strong>
            </div>
            <div className="metric-card">
              <TrendingUp size={18} />
              <span>成功率</span>
              <strong className={rateClass(stats.overview.successRate)}>{stats.overview.successRate}%</strong>
            </div>
            <div className="metric-card">
              <ShieldAlert size={18} />
              <span>需要处理的账号</span>
              <strong>{stats.overview.issueAccounts}</strong>
            </div>
            <div className="metric-card">
              <Clock3 size={18} />
              <span>平均响应时间</span>
              <strong>{formatDuration(stats.overview.avgLatencyMs)}</strong>
            </div>
          </section>
          <section className="chart-grid">
            <section className="panel chart-panel wide">
              <div className="section-heading flat">
                <div>
                  <h2>调用趋势</h2>
                  <p>查看不同时间段内的成功和失败调用。</p>
                </div>
              </div>
              <div className="chart-box">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={stats.timeline}>
                    <CartesianGrid stroke="#26262e" vertical={false} />
                    <XAxis dataKey="label" stroke="#71717a" tickLine={false} axisLine={false} />
                    <YAxis stroke="#71717a" tickLine={false} axisLine={false} allowDecimals={false} />
                    <Tooltip contentStyle={chartTooltipStyle()} />
                    <Legend />
                    <Area type="monotone" dataKey="succeeded" name="成功" stackId="1" stroke="#22c55e" fill="#22c55e" fillOpacity={0.28} />
                    <Area type="monotone" dataKey="failed" name="失败" stackId="1" stroke="#ef4444" fill="#ef4444" fillOpacity={0.26} />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </section>
            <section className="panel chart-panel">
              <div className="section-heading flat">
                <div>
                  <h2>成功分布</h2>
                  <p>查看当前范围内调用结果占比。</p>
                </div>
              </div>
              <div className="chart-box compact">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={[
                        { name: '成功', value: stats.overview.succeeded },
                        { name: '失败', value: stats.overview.failed },
                        { name: '处理中', value: stats.overview.running },
                      ].filter((item) => item.value > 0)}
                      dataKey="value"
                      nameKey="name"
                      innerRadius={58}
                      outerRadius={88}
                      paddingAngle={3}
                    >
                      {chartColors.map((color) => <Cell key={color} fill={color} />)}
                    </Pie>
                    <Tooltip contentStyle={chartTooltipStyle()} />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </section>
            <section className="panel chart-panel">
              <div className="section-heading flat">
                <div>
                  <h2>模型调用量</h2>
                  <p>调用最多的模型。</p>
                </div>
              </div>
              <div className="chart-box compact">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={stats.models.slice(0, 8)}>
                    <CartesianGrid stroke="#26262e" vertical={false} />
                    <XAxis dataKey="model" stroke="#71717a" tickLine={false} axisLine={false} hide />
                    <YAxis stroke="#71717a" tickLine={false} axisLine={false} allowDecimals={false} />
                    <Tooltip contentStyle={chartTooltipStyle()} />
                    <Bar dataKey="requests" name="调用" fill="#6366f1" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </section>
          </section>
          <section className="stats-grid">
            <section className="panel stat-panel">
              <div className="section-heading flat">
                <div>
                  <h2>模型调用</h2>
                  <p>按模型查看调用量和成功率。</p>
                </div>
              </div>
              <table>
                <thead>
                  <tr>
                    <th>模型</th>
                    <th>调用</th>
                    <th>成功率</th>
                    <th>失败</th>
                  </tr>
                </thead>
                <tbody>
                  {stats.models.slice(0, 10).map((item) => (
                    <tr key={item.model}>
                      <td>{item.model}</td>
                      <td>{item.requests}</td>
                      <td>
                        <span className={`rate-pill ${rateClass(item.successRate)}`}>{item.successRate}%</span>
                      </td>
                      <td>{item.failed}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
            <section className="panel stat-panel">
              <div className="section-heading flat">
                <div>
                  <h2>账号调用</h2>
                  <p>查看账号使用量和失败情况。</p>
                </div>
              </div>
              <table>
                <thead>
                  <tr>
                    <th>账号</th>
                    <th>调用</th>
                    <th>成功率</th>
                    <th>失败</th>
                  </tr>
                </thead>
                <tbody>
                  {stats.accounts.slice(0, 10).map((item) => (
                    <tr key={item.accountId}>
                      <td>{item.name}</td>
                      <td>{item.requests}</td>
                      <td>
                        <span className={`rate-pill ${rateClass(item.successRate)}`}>{item.successRate}%</span>
                      </td>
                      <td>{item.failed}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
            <section className="panel stat-panel">
              <div className="section-heading flat">
                <div>
                  <h2>失败原因</h2>
                  <p>优先处理出现次数最多的问题。</p>
                </div>
              </div>
              <div className="rank-list">
                {stats.errors.slice(0, 8).map((item) => (
                  <article key={item.message}>
                    <strong>{item.count}</strong>
                    <span>{item.message}</span>
                  </article>
                ))}
                {stats.errors.length === 0 ? <StateBlock message="当前范围内没有失败记录。" /> : null}
              </div>
            </section>
            <section className="panel stat-panel">
              <div className="section-heading flat">
                <div>
                  <h2>账号状态</h2>
                  <p>查看账号池整体健康情况。</p>
                </div>
              </div>
              <div className="status-summary">
                <div>
                  <span>全部账号</span>
                  <strong>{stats.overview.accounts}</strong>
                </div>
                <div>
                  <span>可用账号</span>
                  <strong>{stats.overview.availableAccounts}</strong>
                </div>
                <div>
                  <span>批量任务</span>
                  <strong>{stats.loginJobs.total}</strong>
                </div>
                <div>
                  <span>导入成功</span>
                  <strong>{stats.loginJobs.succeeded}</strong>
                </div>
              </div>
            </section>
          </section>
        </>
      ) : null}
    </>
  );
}
