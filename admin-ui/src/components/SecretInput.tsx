import { useState } from 'react';
import { Eye, EyeOff } from 'lucide-react';

type SecretInputProps = {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  minLength?: number;
  autoComplete?: string;
};

export function SecretInput({ value, onChange, placeholder, minLength, autoComplete }: SecretInputProps) {
  const [visible, setVisible] = useState(false);
  return (
    <div className="secret-input">
      <input
        autoComplete={autoComplete}
        minLength={minLength}
        placeholder={placeholder}
        type={visible ? 'text' : 'password'}
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
      <button
        aria-label={visible ? '隐藏内容' : '显示内容'}
        className="secret-toggle"
        type="button"
        onClick={() => setVisible((current) => !current)}
      >
        {visible ? <EyeOff size={16} /> : <Eye size={16} />}
      </button>
    </div>
  );
}
