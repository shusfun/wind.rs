export function StatusBadge({ value, label }: { value: string; label?: string }) {
  return <span className={`status-badge status-${value}`}>{label || value}</span>;
}
