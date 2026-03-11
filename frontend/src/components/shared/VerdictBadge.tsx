import { CheckCircle, XCircle, AlertCircle, Clock } from 'lucide-react'

interface VerdictBadgeProps {
  verdict: string | null
  size?: 'sm' | 'md' | 'lg'
}

export default function VerdictBadge({ verdict, size = 'md' }: VerdictBadgeProps) {
  const sizeClass = size === 'sm' ? 'text-xs px-2 py-0.5' : size === 'lg' ? 'text-sm px-3 py-1.5' : 'text-xs px-2.5 py-1'
  const iconSize = size === 'sm' ? 'w-3 h-3' : size === 'lg' ? 'w-5 h-5' : 'w-4 h-4'

  switch (verdict) {
    case 'PASS':
      return (
        <span className={`badge-pass ${sizeClass} inline-flex items-center gap-1`}>
          <CheckCircle className={iconSize} /> PASS
        </span>
      )
    case 'FAIL':
      return (
        <span className={`badge-fail ${sizeClass} inline-flex items-center gap-1`}>
          <XCircle className={iconSize} /> FAIL
        </span>
      )
    case 'INSUFFICIENT_EVIDENCE':
      return (
        <span className={`badge-insufficient ${sizeClass} inline-flex items-center gap-1`}>
          <AlertCircle className={iconSize} /> INSUFFICIENT
        </span>
      )
    default:
      return (
        <span className={`badge-pending ${sizeClass} inline-flex items-center gap-1`}>
          <Clock className={iconSize} /> PENDING
        </span>
      )
  }
}
