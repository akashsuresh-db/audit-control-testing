import type { LucideIcon } from 'lucide-react'
import { clsx } from 'clsx'

interface StatCardProps {
  label: string
  value: string | number
  icon: LucideIcon
  trend?: { value: number; label: string }
  variant?: 'default' | 'success' | 'danger' | 'warning'
}

const variantStyles = {
  default: 'text-brand-400 bg-brand-500/10',
  success: 'text-verdict-pass bg-verdict-pass/10',
  danger: 'text-verdict-fail bg-verdict-fail/10',
  warning: 'text-verdict-insufficient bg-verdict-insufficient/10',
}

export default function StatCard({ label, value, icon: Icon, trend, variant = 'default' }: StatCardProps) {
  return (
    <div className="card p-5">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-slate-400 font-medium">{label}</p>
          <p className="text-3xl font-bold text-white mt-2">{value}</p>
          {trend && (
            <p className={clsx('text-xs mt-2 font-medium', trend.value >= 0 ? 'text-verdict-pass' : 'text-verdict-fail')}>
              {trend.value >= 0 ? '+' : ''}{trend.value}% {trend.label}
            </p>
          )}
        </div>
        <div className={clsx('w-10 h-10 rounded-lg flex items-center justify-center', variantStyles[variant])}>
          <Icon className="w-5 h-5" />
        </div>
      </div>
    </div>
  )
}
