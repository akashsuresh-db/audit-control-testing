import { clsx } from 'clsx'

interface ConfidenceBarProps {
  value: number
  showLabel?: boolean
}

export default function ConfidenceBar({ value, showLabel = true }: ConfidenceBarProps) {
  const pct = Math.round(value * 100)
  const color =
    pct >= 80 ? 'bg-verdict-pass' :
    pct >= 60 ? 'bg-verdict-insufficient' :
    'bg-verdict-fail'

  return (
    <div className="flex items-center gap-3">
      <div className="progress-bar flex-1">
        <div
          className={clsx('progress-fill', color)}
          style={{ width: `${pct}%` }}
        />
      </div>
      {showLabel && (
        <span className="text-xs font-mono text-slate-400 w-10 text-right">{pct}%</span>
      )}
    </div>
  )
}
