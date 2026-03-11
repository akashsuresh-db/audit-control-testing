import { useEffect, useState } from 'react'
import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import VerdictBadge from '../components/shared/VerdictBadge'
import RiskBadge from '../components/shared/RiskBadge'
import ConfidenceBar from '../components/shared/ConfidenceBar'
import EmptyState from '../components/shared/EmptyState'
import { ClipboardList, Download, ChevronDown, ChevronUp, FileText, CheckCircle } from 'lucide-react'
import axios from 'axios'

interface Workpaper {
  control_id: string
  control_code: string
  control_title: string
  control_objective: string
  control_category: string
  risk_level: string
  frequency: string
  testing_procedure: string
  evidence_reviewed: { filename: string; relevance: number }[]
  test_result: string
  ai_confidence: number
  ai_reasoning: string
  auditor_verdict: string | null
  auditor_notes: string
  auditor_id: string
  reviewed_at: string | null
  conclusion: string
}

export default function WorkpapersPage() {
  const { currentAudit } = useStore()
  const [workpapers, setWorkpapers] = useState<Workpaper[]>([])
  const [loading, setLoading] = useState(false)
  const [expandedId, setExpandedId] = useState<string | null>(null)

  useEffect(() => {
    if (!currentAudit) return
    setLoading(true)
    axios.get(`/api/audits/${currentAudit.audit_id}/workpapers`)
      .then(r => setWorkpapers(r.data))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [currentAudit])

  const exportWorkpapers = () => {
    const text = workpapers.map(wp => {
      const evidenceList = wp.evidence_reviewed.map(e => `    - ${e.filename} (${e.relevance}% relevance)`).join('\n')
      return `
════════════════════════════════════════════════════════
CONTROL: ${wp.control_code} — ${wp.control_title}
════════════════════════════════════════════════════════
Category: ${wp.control_category}  |  Risk: ${wp.risk_level}  |  Frequency: ${wp.frequency}

CONTROL OBJECTIVE:
${wp.control_objective}

TESTING PROCEDURE:
${wp.testing_procedure}

EVIDENCE REVIEWED:
${evidenceList || '    None'}

TEST RESULT: ${wp.test_result}
AI CONFIDENCE: ${Math.round(wp.ai_confidence * 100)}%

AI ANALYSIS:
${wp.ai_reasoning}

${wp.auditor_verdict ? `AUDITOR REVIEW: ${wp.auditor_verdict} by ${wp.auditor_id}\n${wp.auditor_notes}` : 'AUDITOR REVIEW: Pending'}

CONCLUSION:
${wp.conclusion}
`
    }).join('\n\n')

    const blob = new Blob([text], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${currentAudit?.audit_name?.replace(/\s+/g, '_')}_workpapers.txt`
    a.click()
    URL.revokeObjectURL(url)
  }

  if (!currentAudit) {
    return <div className="p-6"><EmptyState icon={ClipboardList} title="No Audit Selected" description="Select an audit to view workpapers." /></div>
  }

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <PageHeader
        title="Audit Workpapers"
        subtitle={`Testing documentation for ${currentAudit.audit_name}`}
        actions={
          <button onClick={exportWorkpapers} className="btn-primary" disabled={workpapers.length === 0}>
            <Download className="w-4 h-4" /> Export Workpapers
          </button>
        }
      />

      {loading ? (
        <div className="text-center py-12 text-slate-500">Loading workpapers...</div>
      ) : workpapers.length > 0 ? (
        <div className="space-y-4">
          {workpapers.map(wp => {
            const isExpanded = expandedId === wp.control_id
            return (
              <div key={wp.control_id} className="card overflow-hidden">
                {/* Header */}
                <button
                  onClick={() => setExpandedId(isExpanded ? null : wp.control_id)}
                  className="w-full flex items-center gap-4 px-5 py-4 hover:bg-slate-800/30 transition-colors"
                >
                  <div className="flex-1 text-left">
                    <div className="flex items-center gap-3 mb-1">
                      <span className="text-xs font-mono text-brand-400">{wp.control_code}</span>
                      <VerdictBadge verdict={wp.test_result} size="sm" />
                      <RiskBadge level={wp.risk_level} />
                      {wp.auditor_verdict && (
                        <span className="badge bg-brand-500/15 text-brand-300 text-xs">
                          <CheckCircle className="w-3 h-3 mr-1" />Reviewed
                        </span>
                      )}
                    </div>
                    <h3 className="text-sm font-semibold text-white">{wp.control_title}</h3>
                    <p className="text-xs text-slate-500 mt-0.5">{wp.control_category} — {wp.frequency}</p>
                  </div>
                  <div className="w-24 text-right">
                    <ConfidenceBar value={wp.ai_confidence} />
                  </div>
                  {isExpanded ? <ChevronUp className="w-4 h-4 text-slate-400" /> : <ChevronDown className="w-4 h-4 text-slate-400" />}
                </button>

                {/* Expanded content */}
                {isExpanded && (
                  <div className="px-5 pb-5 border-t border-surface-border space-y-4 pt-4">
                    {/* Control Objective */}
                    <div>
                      <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Control Objective</h4>
                      <p className="text-sm text-slate-300 leading-relaxed">{wp.control_objective}</p>
                    </div>

                    {/* Testing Procedure */}
                    <div>
                      <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Testing Procedure</h4>
                      <pre className="text-sm text-slate-300 whitespace-pre-wrap font-sans leading-relaxed bg-surface-dark rounded-lg p-3">
                        {wp.testing_procedure}
                      </pre>
                    </div>

                    {/* Evidence Reviewed */}
                    <div>
                      <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Evidence Reviewed ({wp.evidence_reviewed.length})</h4>
                      <div className="space-y-1">
                        {wp.evidence_reviewed.map((e, i) => (
                          <div key={i} className="flex items-center gap-2 text-sm">
                            <FileText className="w-3.5 h-3.5 text-slate-500" />
                            <span className="text-slate-300">{e.filename}</span>
                            <span className="text-xs text-slate-500">({e.relevance}% relevance)</span>
                          </div>
                        ))}
                        {wp.evidence_reviewed.length === 0 && (
                          <p className="text-xs text-slate-500 italic">No evidence reviewed</p>
                        )}
                      </div>
                    </div>

                    {/* Test Result & AI Analysis */}
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Test Result</h4>
                        <VerdictBadge verdict={wp.test_result} size="lg" />
                      </div>
                      <div>
                        <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">AI Confidence</h4>
                        <ConfidenceBar value={wp.ai_confidence} />
                      </div>
                    </div>

                    {/* AI Reasoning */}
                    {wp.ai_reasoning && (
                      <div>
                        <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">AI Analysis</h4>
                        <p className="text-sm text-slate-300 leading-relaxed">{wp.ai_reasoning}</p>
                      </div>
                    )}

                    {/* Auditor Review */}
                    {wp.auditor_verdict && (
                      <div className="bg-brand-500/5 border border-brand-500/20 rounded-lg p-3">
                        <h4 className="text-xs font-semibold text-brand-400 mb-1">Auditor Review — {wp.auditor_id}</h4>
                        <VerdictBadge verdict={wp.auditor_verdict} size="sm" />
                        {wp.auditor_notes && <p className="text-xs text-slate-400 mt-1">{wp.auditor_notes}</p>}
                      </div>
                    )}

                    {/* Conclusion */}
                    <div className="bg-surface-dark rounded-lg p-3">
                      <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Conclusion</h4>
                      <p className="text-sm text-slate-300 leading-relaxed">{wp.conclusion}</p>
                    </div>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      ) : (
        <EmptyState icon={ClipboardList} title="No Workpapers" description="Run the evaluation pipeline to generate workpapers." />
      )}
    </div>
  )
}
