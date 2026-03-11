import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import VerdictBadge from '../components/shared/VerdictBadge'
import RiskBadge from '../components/shared/RiskBadge'
import ConfidenceBar from '../components/shared/ConfidenceBar'
import EmptyState from '../components/shared/EmptyState'
import Modal from '../components/shared/Modal'
import { AlertTriangle, Search, Eye, Plus, ChevronDown, ChevronUp } from 'lucide-react'
import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'

interface SufficiencyItem {
  control_id: string
  control_code: string
  control_title: string
  risk_level: string
  evidence_sources: number
  total_matches: number
  avg_similarity: number
  high_quality_matches: number
  ai_verdict: string | null
  ai_confidence: number
  sufficiency_score: number
  sufficiency_status: string
}

export default function FindingsPage() {
  const { currentAudit, results, controls, selectControl } = useStore()
  const navigate = useNavigate()
  const [filter, setFilter] = useState('ALL')
  const [search, setSearch] = useState('')
  const [sufficiency, setSufficiency] = useState<SufficiencyItem[]>([])
  const [view, setView] = useState<'findings' | 'sufficiency'>('findings')
  const [showNewFinding, setShowNewFinding] = useState(false)
  const [findingForm, setFindingForm] = useState({ control_id: '', title: '', risk_rating: 'HIGH', root_cause: '', impact: '', recommendation: '' })
  const [expandedFinding, setExpandedFinding] = useState<string | null>(null)

  useEffect(() => {
    if (!currentAudit) return
    axios.get(`/api/audits/${currentAudit.audit_id}/sufficiency`)
      .then(r => setSufficiency(r.data))
      .catch(() => {})
  }, [currentAudit])

  const failures = results.filter(r => {
    const matchFilter = filter === 'ALL' ||
      (filter === 'FAIL' && r.ai_verdict === 'FAIL') ||
      (filter === 'INSUFFICIENT' && r.ai_verdict === 'INSUFFICIENT_EVIDENCE') ||
      (filter === 'UNREVIEWED' && !r.auditor_verdict) ||
      (filter === 'HIGH_RISK' && r.risk_level === 'HIGH' && r.ai_verdict === 'FAIL')
    const matchSearch = !search ||
      r.control_code.toLowerCase().includes(search.toLowerCase()) ||
      r.control_title.toLowerCase().includes(search.toLowerCase())
    return matchFilter && matchSearch && (r.ai_verdict === 'FAIL' || r.ai_verdict === 'INSUFFICIENT_EVIDENCE')
  })

  const handleInvestigate = (controlId: string) => {
    selectControl(controlId)
    navigate('/investigation')
  }

  const handleCreateFinding = async () => {
    if (!currentAudit || !findingForm.control_id || !findingForm.title) return
    try {
      await axios.post(`/api/audits/${currentAudit.audit_id}/findings`, findingForm)
      setShowNewFinding(false)
      setFindingForm({ control_id: '', title: '', risk_rating: 'HIGH', root_cause: '', impact: '', recommendation: '' })
    } catch (e) { console.error(e) }
  }

  if (!currentAudit) {
    return <div className="p-6"><EmptyState icon={AlertTriangle} title="No Audit Selected" description="Select an audit to view findings." /></div>
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <PageHeader
        title="Findings & Sufficiency"
        subtitle={`${failures.length} findings | ${sufficiency.filter(s => s.sufficiency_status === 'INSUFFICIENT').length} controls with insufficient evidence`}
        actions={
          <button onClick={() => setShowNewFinding(true)} className="btn-primary">
            <Plus className="w-4 h-4" /> New Finding
          </button>
        }
      />

      {/* View Toggle */}
      <div className="flex gap-2 mb-4">
        <button onClick={() => setView('findings')} className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${view === 'findings' ? 'bg-brand-500/20 text-brand-400 border border-brand-500/30' : 'bg-surface-card text-slate-400 border border-surface-border'}`}>
          Findings ({failures.length})
        </button>
        <button onClick={() => setView('sufficiency')} className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${view === 'sufficiency' ? 'bg-brand-500/20 text-brand-400 border border-brand-500/30' : 'bg-surface-card text-slate-400 border border-surface-border'}`}>
          Evidence Sufficiency
        </button>
      </div>

      {view === 'findings' ? (
        <>
          {/* Filters */}
          <div className="flex flex-wrap gap-3 mb-4">
            <div className="relative flex-1 min-w-[200px] max-w-md">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
              <input className="input-field pl-9" placeholder="Search findings..." value={search} onChange={e => setSearch(e.target.value)} />
            </div>
            <div className="flex gap-2">
              {['ALL', 'FAIL', 'INSUFFICIENT', 'UNREVIEWED', 'HIGH_RISK'].map(f => (
                <button key={f} onClick={() => setFilter(f)} className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${filter === f ? 'bg-brand-500/20 text-brand-400 border border-brand-500/30' : 'bg-surface-card text-slate-400 border border-surface-border'}`}>
                  {f.replace('_', ' ')}
                </button>
              ))}
            </div>
          </div>

          {/* Findings List */}
          {failures.length > 0 ? (
            <div className="space-y-3">
              {failures.map(r => {
                const isExpanded = expandedFinding === r.evaluation_id
                return (
                  <div key={r.evaluation_id} className="card overflow-hidden">
                    <button onClick={() => setExpandedFinding(isExpanded ? null : r.evaluation_id)} className="w-full text-left p-5">
                      <div className="flex items-start gap-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-3 mb-2">
                            <span className="text-xs font-mono text-brand-400">{r.control_code}</span>
                            <VerdictBadge verdict={r.ai_verdict} size="sm" />
                            <RiskBadge level={r.risk_level} />
                            {!r.auditor_verdict && <span className="badge bg-verdict-insufficient/15 text-verdict-insufficient text-xs">Needs Review</span>}
                          </div>
                          <h3 className="text-sm font-semibold text-white mb-1">{r.control_title}</h3>
                          <p className="text-xs text-slate-400 line-clamp-2">{r.ai_reasoning}</p>
                        </div>
                        <div className="flex items-center gap-3 flex-shrink-0">
                          <div className="w-24"><ConfidenceBar value={r.ai_confidence} /></div>
                          {isExpanded ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
                        </div>
                      </div>
                    </button>
                    {isExpanded && (
                      <div className="px-5 pb-5 border-t border-surface-border pt-4 space-y-3">
                        <div>
                          <h4 className="text-xs font-semibold text-slate-400 uppercase mb-1">AI Analysis</h4>
                          <p className="text-sm text-slate-300">{r.ai_reasoning}</p>
                        </div>
                        {r.evidence_summary && (
                          <div>
                            <h4 className="text-xs font-semibold text-slate-400 uppercase mb-1">Evidence Summary</h4>
                            <p className="text-xs text-slate-400">{r.evidence_summary}</p>
                          </div>
                        )}
                        <button onClick={() => handleInvestigate(r.control_id)} className="btn-secondary text-xs">
                          <Eye className="w-3.5 h-3.5" /> Investigate Evidence
                        </button>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          ) : (
            <EmptyState icon={AlertTriangle} title="No Findings" description={results.length > 0 ? 'No failures found.' : 'Run the pipeline first.'} />
          )}
        </>
      ) : (
        /* Evidence Sufficiency View */
        <div className="card overflow-hidden">
          <table className="w-full">
            <thead className="bg-surface-elevated">
              <tr>
                <th className="table-header w-28">Control</th>
                <th className="table-header">Title</th>
                <th className="table-header w-20">Risk</th>
                <th className="table-header w-20">Sources</th>
                <th className="table-header w-20">Matches</th>
                <th className="table-header w-24">Avg Score</th>
                <th className="table-header w-28">Sufficiency</th>
                <th className="table-header w-20">Score</th>
                <th className="table-header w-24">Verdict</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {sufficiency.map(s => (
                <tr key={s.control_id} className="hover:bg-slate-800/30">
                  <td className="table-cell font-mono text-xs text-brand-400">{s.control_code}</td>
                  <td className="table-cell text-sm text-slate-200">{s.control_title}</td>
                  <td className="table-cell"><RiskBadge level={s.risk_level} /></td>
                  <td className="table-cell text-center text-sm text-slate-300">{s.evidence_sources}</td>
                  <td className="table-cell text-center text-sm text-slate-300">{s.total_matches}</td>
                  <td className="table-cell text-center text-xs text-slate-400">{(s.avg_similarity * 100).toFixed(0)}%</td>
                  <td className="table-cell">
                    <span className={`badge text-xs ${
                      s.sufficiency_status === 'SUFFICIENT' ? 'bg-verdict-pass/15 text-verdict-pass' :
                      s.sufficiency_status === 'PARTIAL' ? 'bg-verdict-insufficient/15 text-verdict-insufficient' :
                      'bg-verdict-fail/15 text-verdict-fail'
                    }`}>
                      {s.sufficiency_status}
                    </span>
                  </td>
                  <td className="table-cell">
                    <div className="flex items-center gap-2">
                      <div className="w-12 h-1.5 rounded-full bg-surface-dark overflow-hidden">
                        <div className={`h-full rounded-full ${s.sufficiency_score >= 70 ? 'bg-verdict-pass' : s.sufficiency_score >= 40 ? 'bg-verdict-insufficient' : 'bg-verdict-fail'}`} style={{ width: `${s.sufficiency_score}%` }} />
                      </div>
                      <span className="text-xs text-slate-500">{s.sufficiency_score}</span>
                    </div>
                  </td>
                  <td className="table-cell"><VerdictBadge verdict={s.ai_verdict} size="sm" /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* New Finding Modal */}
      <Modal open={showNewFinding} onClose={() => setShowNewFinding(false)} title="Create Finding">
        <div className="space-y-3">
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1">Control</label>
            <select className="input-field" value={findingForm.control_id} onChange={e => setFindingForm(f => ({ ...f, control_id: e.target.value }))}>
              <option value="">Select control...</option>
              {controls.map(c => <option key={c.control_id} value={c.control_id}>{c.control_code} — {c.control_title}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1">Finding Title</label>
            <input className="input-field" placeholder="e.g., Terminated user access not revoked" value={findingForm.title} onChange={e => setFindingForm(f => ({ ...f, title: e.target.value }))} />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1">Risk Rating</label>
            <select className="input-field" value={findingForm.risk_rating} onChange={e => setFindingForm(f => ({ ...f, risk_rating: e.target.value }))}>
              <option value="CRITICAL">Critical</option>
              <option value="HIGH">High</option>
              <option value="MEDIUM">Medium</option>
              <option value="LOW">Low</option>
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1">Root Cause</label>
            <textarea className="input-field" rows={2} value={findingForm.root_cause} onChange={e => setFindingForm(f => ({ ...f, root_cause: e.target.value }))} />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1">Impact</label>
            <textarea className="input-field" rows={2} value={findingForm.impact} onChange={e => setFindingForm(f => ({ ...f, impact: e.target.value }))} />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1">Recommendation</label>
            <textarea className="input-field" rows={2} value={findingForm.recommendation} onChange={e => setFindingForm(f => ({ ...f, recommendation: e.target.value }))} />
          </div>
          <div className="flex gap-3 pt-2">
            <button onClick={handleCreateFinding} className="btn-primary flex-1" disabled={!findingForm.control_id || !findingForm.title}>Create Finding</button>
            <button onClick={() => setShowNewFinding(false)} className="btn-secondary">Cancel</button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
