import { useState } from 'react'
import PageHeader from '../components/shared/PageHeader'
import { ShieldAlert, Play, Loader2, AlertTriangle, ShieldOff, UserX, GitBranch, Clock } from 'lucide-react'
import axios from 'axios'

interface Violation {
  type: string
  severity: string
  control: string
  title: string
  details: string
  entity_id: string
}

interface MonitoringResult {
  total_violations: number
  summary: Record<string, number>
  violations: Violation[]
  checked_at: string
}

const VIOLATION_ICONS: Record<string, typeof AlertTriangle> = {
  PRIVILEGED_NO_MFA: ShieldOff,
  TERMINATED_ACTIVE_ACCESS: UserX,
  SELF_APPROVED_CHANGE: GitBranch,
  DEPLOYED_BEFORE_APPROVAL: AlertTriangle,
  OVERDUE_ACCESS_REVIEW: Clock,
}

const SEVERITY_COLORS: Record<string, string> = {
  CRITICAL: 'bg-verdict-fail/15 text-verdict-fail border-verdict-fail/30',
  HIGH: 'bg-verdict-insufficient/15 text-verdict-insufficient border-verdict-insufficient/30',
  MEDIUM: 'bg-brand-500/15 text-brand-400 border-brand-500/30',
  LOW: 'bg-slate-500/15 text-slate-400 border-slate-500/30',
  INFO: 'bg-slate-500/15 text-slate-500 border-slate-500/30',
}

export default function MonitoringPage() {
  const [result, setResult] = useState<MonitoringResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [filterType, setFilterType] = useState('ALL')

  const runMonitoring = async () => {
    setLoading(true)
    try {
      const resp = await axios.get('/api/monitoring/violations')
      setResult(resp.data)
    } catch (e) { console.error(e) }
    setLoading(false)
  }

  const filtered = result?.violations.filter(v => filterType === 'ALL' || v.type === filterType) || []
  const types = result ? Object.keys(result.summary) : []

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <PageHeader
        title="Continuous Monitoring"
        subtitle="Automated detection of control violations in operational data"
        actions={
          <button onClick={runMonitoring} className="btn-primary" disabled={loading}>
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            {loading ? 'Scanning...' : 'Run Monitoring Checks'}
          </button>
        }
      />

      {result && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-6">
            <div className="card p-4 text-center">
              <p className="text-3xl font-bold text-verdict-fail">{result.total_violations}</p>
              <p className="text-xs text-slate-500 mt-1">Total Violations</p>
            </div>
            {Object.entries(result.summary).map(([type, count]) => {
              const Icon = VIOLATION_ICONS[type] || AlertTriangle
              return (
                <div key={type} className="card p-4 text-center">
                  <div className="flex items-center justify-center gap-2 mb-1">
                    <Icon className="w-4 h-4 text-slate-400" />
                    <p className="text-2xl font-bold text-white">{count}</p>
                  </div>
                  <p className="text-xs text-slate-500">{type.replace(/_/g, ' ')}</p>
                </div>
              )
            })}
          </div>

          {/* Filters */}
          <div className="flex gap-2 mb-4 flex-wrap">
            <button onClick={() => setFilterType('ALL')} className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${filterType === 'ALL' ? 'bg-brand-500/20 text-brand-400 border border-brand-500/30' : 'bg-surface-card text-slate-400 border border-surface-border'}`}>
              All ({result.total_violations})
            </button>
            {types.map(t => (
              <button key={t} onClick={() => setFilterType(t)} className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${filterType === t ? 'bg-brand-500/20 text-brand-400 border border-brand-500/30' : 'bg-surface-card text-slate-400 border border-surface-border'}`}>
                {t.replace(/_/g, ' ')} ({result.summary[t]})
              </button>
            ))}
          </div>

          {/* Violations List */}
          <div className="space-y-2">
            {filtered.map((v, i) => {
              const Icon = VIOLATION_ICONS[v.type] || AlertTriangle
              return (
                <div key={i} className="card px-4 py-3 flex items-start gap-3">
                  <div className={`w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 ${v.severity === 'CRITICAL' ? 'bg-verdict-fail/15' : 'bg-verdict-insufficient/15'}`}>
                    <Icon className={`w-4 h-4 ${v.severity === 'CRITICAL' ? 'text-verdict-fail' : 'text-verdict-insufficient'}`} />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-0.5">
                      <span className={`badge border text-xs ${SEVERITY_COLORS[v.severity] || ''}`}>{v.severity}</span>
                      {v.control && <span className="text-xs font-mono text-brand-400">{v.control}</span>}
                      <span className="text-xs text-slate-600">{v.entity_id}</span>
                    </div>
                    <p className="text-sm text-slate-200">{v.title}</p>
                    {v.details && <p className="text-xs text-slate-500 mt-0.5">{v.details}</p>}
                  </div>
                </div>
              )
            })}
          </div>

          <p className="text-xs text-slate-600 mt-4 text-center">
            Last checked: {new Date(result.checked_at).toLocaleString()}
          </p>
        </>
      )}

      {!result && !loading && (
        <div className="text-center py-20">
          <ShieldAlert className="w-16 h-16 text-slate-600 mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-slate-300 mb-1">Continuous Monitoring</h3>
          <p className="text-sm text-slate-500 max-w-md mx-auto mb-6">
            Scan operational data (200 users, 500 change tickets, 300 access reviews) for control violations including unauthorized access, self-approved changes, and overdue reviews.
          </p>
          <button onClick={runMonitoring} className="btn-primary">
            <Play className="w-4 h-4" /> Run First Scan
          </button>
        </div>
      )}
    </div>
  )
}
