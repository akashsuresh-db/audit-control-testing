import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import VerdictBadge from '../components/shared/VerdictBadge'
import RiskBadge from '../components/shared/RiskBadge'
import ConfidenceBar from '../components/shared/ConfidenceBar'
import EmptyState from '../components/shared/EmptyState'
import { AlertTriangle, Filter, Search, Eye } from 'lucide-react'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'

export default function FindingsPage() {
  const { currentAudit, results, controls, selectControl } = useStore()
  const navigate = useNavigate()
  const [filter, setFilter] = useState<string>('ALL')
  const [search, setSearch] = useState('')

  const failures = results.filter(r => {
    const matchFilter = filter === 'ALL' ||
      (filter === 'FAIL' && r.ai_verdict === 'FAIL') ||
      (filter === 'INSUFFICIENT' && r.ai_verdict === 'INSUFFICIENT_EVIDENCE') ||
      (filter === 'UNREVIEWED' && !r.auditor_verdict) ||
      (filter === 'HIGH_RISK' && r.risk_level === 'HIGH' && r.ai_verdict === 'FAIL')
    const matchSearch = !search ||
      r.control_code.toLowerCase().includes(search.toLowerCase()) ||
      r.control_title.toLowerCase().includes(search.toLowerCase()) ||
      r.ai_reasoning.toLowerCase().includes(search.toLowerCase())
    return matchFilter && matchSearch && (r.ai_verdict === 'FAIL' || r.ai_verdict === 'INSUFFICIENT_EVIDENCE')
  })

  const handleInvestigate = (controlId: string) => {
    selectControl(controlId)
    navigate('/investigation')
  }

  if (!currentAudit) {
    return (
      <div className="p-6">
        <EmptyState icon={AlertTriangle} title="No Audit Selected" description="Select an audit to view findings." />
      </div>
    )
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <PageHeader
        title="Findings"
        subtitle={`${failures.length} findings requiring attention`}
      />

      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-6">
        <div className="relative flex-1 min-w-[200px] max-w-md">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
          <input className="input-field pl-9" placeholder="Search findings..." value={search} onChange={e => setSearch(e.target.value)} />
        </div>
        <div className="flex gap-2">
          {['ALL', 'FAIL', 'INSUFFICIENT', 'UNREVIEWED', 'HIGH_RISK'].map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
                filter === f ? 'bg-brand-500/20 text-brand-400 border border-brand-500/30' : 'bg-surface-card text-slate-400 border border-surface-border hover:text-slate-200'
              }`}
            >
              {f.replace('_', ' ')}
            </button>
          ))}
        </div>
      </div>

      {/* Findings List */}
      {failures.length > 0 ? (
        <div className="space-y-3">
          {failures.map(r => (
            <div key={r.evaluation_id} className="card-hover p-5">
              <div className="flex items-start gap-4">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-3 mb-2">
                    <span className="text-xs font-mono text-brand-400">{r.control_code}</span>
                    <VerdictBadge verdict={r.ai_verdict} size="sm" />
                    <RiskBadge level={r.risk_level} />
                    {!r.auditor_verdict && (
                      <span className="badge bg-verdict-insufficient/15 text-verdict-insufficient text-xs">Needs Review</span>
                    )}
                  </div>
                  <h3 className="text-sm font-semibold text-white mb-1">{r.control_title}</h3>
                  <p className="text-xs text-slate-400 line-clamp-2 mb-3">{r.ai_reasoning}</p>
                  <div className="flex items-center gap-4">
                    <div className="w-32">
                      <ConfidenceBar value={r.ai_confidence} />
                    </div>
                    <span className="text-xs text-slate-500">{r.control_category}</span>
                  </div>
                </div>
                <button
                  onClick={() => handleInvestigate(r.control_id)}
                  className="btn-secondary text-xs flex-shrink-0"
                >
                  <Eye className="w-3.5 h-3.5" /> Investigate
                </button>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <EmptyState
          icon={AlertTriangle}
          title="No Findings"
          description={results.length > 0 ? 'No control failures or insufficient evidence found.' : 'Run the evaluation pipeline to generate findings.'}
        />
      )}
    </div>
  )
}
