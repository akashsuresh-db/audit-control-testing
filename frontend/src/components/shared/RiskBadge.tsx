import { clsx } from 'clsx'

interface RiskBadgeProps {
  level: string
}

export default function RiskBadge({ level }: RiskBadgeProps) {
  return (
    <span
      className={clsx(
        'badge border',
        level === 'HIGH' && 'risk-high',
        level === 'MEDIUM' && 'risk-medium',
        level === 'LOW' && 'risk-low',
      )}
    >
      {level}
    </span>
  )
}
