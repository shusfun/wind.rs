import { useEffect, useState } from 'react';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';
import { api, Account, LoginJob, RequestTrace } from '../lib/api';
import { formatDateTime, jobStatusText } from '../lib/display';

export function DashboardPage() {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [jobs, setJobs] = useState<LoginJob[]>([]);
  const [requests, setRequests] = useState<RequestTrace[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    Promise.all([api.accounts(), api.loginJobs(), api.requests()])
      .then(([accountData, jobData, requestData]) => {
        setAccounts(accountData.accounts);
        setJobs(jobData.jobs);
        setRequests(requestData.requests);
      })
      .catch((err) => setError(err instanceof Error ? err.message : '读取失败'));
  }, []);

  if (error) {
    return <StateBlock message={error} />;
  }

  const running = jobs.filter((job) => job.status === 'running').length;
  const failed = requests.filter((item) => item.status !== 'ok').length;

  return (
    <>
      <PageHeader title="运行概览" subtitle="查看账号、批量任务和最近调用情况。" />
      <section className="metric-grid">
        <div className="metric-card">
          <span>账号数量</span>
          <strong>{accounts.length}</strong>
        </div>
        <div className="metric-card">
          <span>进行中的任务</span>
          <strong>{running}</strong>
        </div>
        <div className="metric-card">
          <span>最近调用</span>
          <strong>{requests.length}</strong>
        </div>
        <div className="metric-card">
          <span>异常调用</span>
          <strong>{failed}</strong>
        </div>
      </section>
      <section className="panel">
        <h2>最近批量任务</h2>
        <table>
          <thead>
            <tr>
              <th>任务</th>
              <th>状态</th>
              <th>创建时间</th>
              <th>成功</th>
              <th>失败</th>
            </tr>
          </thead>
          <tbody>
            {jobs.slice(0, 6).map((job) => (
              <tr key={job.id}>
                <td>{job.id.slice(0, 8)}</td>
                <td>{jobStatusText(job.status)}</td>
                <td>{formatDateTime(job.createdAt)}</td>
                <td>{job.successCount}</td>
                <td>{job.failedCount}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </>
  );
}
