import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { PageHeader } from '../components/PageHeader';
import { StateBlock } from '../components/StateBlock';

export function AccountTestPage() {
  const navigate = useNavigate();

  useEffect(() => {
    navigate('/accounts', { replace: true });
  }, [navigate]);

  return (
    <>
      <PageHeader title="账号管理" subtitle="账号探测已移到账号列表操作中。" />
      <section className="panel">
        <StateBlock message="正在打开账号列表。" />
      </section>
    </>
  );
}
