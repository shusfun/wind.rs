import { createContext, ReactNode, useCallback, useContext, useMemo, useState } from 'react';
import { X } from 'lucide-react';

type ToastKind = 'info' | 'success' | 'error';

type ToastItem = {
  id: number;
  kind: ToastKind;
  message: string;
};

type ToastContextValue = {
  showToast: (message: string, kind?: ToastKind) => void;
};

const ToastContext = createContext<ToastContextValue | null>(null);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([]);

  const remove = useCallback((id: number) => {
    setItems((current) => current.filter((item) => item.id !== id));
  }, []);

  const showToast = useCallback(
    (message: string, kind: ToastKind = 'info') => {
      const id = Date.now() + Math.floor(Math.random() * 1000);
      setItems((current) => [...current.slice(-2), { id, kind, message }]);
      window.setTimeout(() => remove(id), 2600);
    },
    [remove],
  );

  const value = useMemo(() => ({ showToast }), [showToast]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div className="toast-viewport" aria-live="polite" aria-atomic="true">
        {items.map((item) => (
          <div className={`toast-item toast-${item.kind}`} key={item.id}>
            <span>{item.message}</span>
            <button type="button" onClick={() => remove(item.id)} title="关闭">
              <X size={14} />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast() {
  const context = useContext(ToastContext);
  if (!context) {
    throw new Error('useToast must be used inside ToastProvider');
  }
  return context;
}
