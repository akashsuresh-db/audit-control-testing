import { useStore } from '../../store/useStore'
import { useNavigate } from 'react-router-dom'
import { CheckCircle, Circle, ArrowRight } from 'lucide-react'

const STAGES = [
  { key: 'setup', label: 'Audit Setup', path: '/controls', check: (s: any) => s.controls.length > 0 },
  { key: 'evidence', label: 'Evidence Collection', path: '/evidence', check: (s: any) => s.evidence.length > 0 },
  { key: 'evaluation', label: 'Control Evaluation', path: '/dashboard', check: (s: any) => s.results.length > 0 },
  { key: 'investigation', label: 'Investigation', path: '/investigation', check: (s: any) => {
    const reviewed = s.results.filter((r: any) => r.auditor_verdict).length
    return reviewed > 0
  }},
  { key: 'reporting', label: 'Findings & Reporting', path: '/findings', check: (s: any) => {
    const reviewed = s.results.filter((r: any) => r.auditor_verdict).length
    return reviewed === s.results.length && s.results.length > 0
  }},
]

export default function WorkflowTracker() {
  const store = useStore()
  const navigate = useNavigate()
  const { currentAudit, controls, evidence, results } = store

  if (!currentAudit) return null

  // Determine current stage
  const stageStatuses = STAGES.map(s => ({
    ...s,
    complete: s.check(store),
  }))

  const currentStageIdx = stageStatuses.findIndex(s => !s.complete)
  const currentStage = currentStageIdx >= 0 ? stageStatuses[currentStageIdx] : null
  const progressPct = Math.round(
    (stageStatuses.filter(s => s.complete).length / STAGES.length) * 100
  )

  // Generate recommendation
  const failures = results.filter(r => r.ai_verdict === 'FAIL')
  const unreviewed = results.filter(r => !r.auditor_verdict)
  let recommendation = ''
  let recAction = ''
  let recPath = ''

  if (controls.length === 0) {
    recommendation = 'Upload controls to begin audit testing.'
    recAction = 'Upload Controls'
    recPath = '/controls'
  } else if (evidence.length === 0) {
    recommendation = 'Upload evidence documents for control evaluation.'
    recAction = 'Upload Evidence'
    recPath = '/evidence'
  } else if (results.length === 0) {
    recommendation = 'Run the evaluation pipeline to test controls against evidence.'
    recAction = 'Go to Dashboard'
    recPath = '/dashboard'
  } else if (failures.length > 0 && unreviewed.length > 0) {
    recommendation = `${failures.length} control${failures.length > 1 ? 's' : ''} failed evaluation. ${unreviewed.length} pending review.`
    recAction = 'Review Investigation'
    recPath = '/investigation'
  } else if (unreviewed.length > 0) {
    recommendation = `${unreviewed.length} control${unreviewed.length > 1 ? 's' : ''} awaiting auditor review.`
    recAction = 'Review Controls'
    recPath = '/investigation'
  } else {
    recommendation = 'All controls reviewed. Generate workpapers and reports.'
    recAction = 'View Workpapers'
    recPath = '/workpapers'
  }

  return (
    <div className="card p-4 mb-6">
      {/* Progress bar */}
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Audit Progress</h3>
        <span className="text-xs font-bold text-brand-400">{progressPct}%</span>
      </div>
      <div className="h-1.5 rounded-full bg-surface-dark mb-4 overflow-hidden">
        <div
          className="h-full rounded-full bg-brand-500 transition-all duration-500"
          style={{ width: `${progressPct}%` }}
        />
      </div>

      {/* Stage indicators */}
      <div className="flex items-center gap-1 mb-4">
        {stageStatuses.map((stage, i) => (
          <button
            key={stage.key}
            onClick={() => navigate(stage.path)}
            className="flex-1 flex flex-col items-center gap-1 group"
          >
            <div className="flex items-center w-full">
              {stage.complete ? (
                <CheckCircle className="w-5 h-5 text-verdict-pass mx-auto" />
              ) : i === currentStageIdx ? (
                <div className="w-5 h-5 rounded-full border-2 border-brand-400 bg-brand-500/20 mx-auto animate-pulse" />
              ) : (
                <Circle className="w-5 h-5 text-slate-600 mx-auto" />
              )}
            </div>
            <span className={`text-[10px] text-center leading-tight ${
              stage.complete ? 'text-verdict-pass' :
              i === currentStageIdx ? 'text-brand-400 font-semibold' :
              'text-slate-600'
            }`}>
              {stage.label}
            </span>
          </button>
        ))}
      </div>

      {/* Recommendation */}
      <div className="flex items-center gap-3 px-3 py-2.5 rounded-lg bg-brand-500/5 border border-brand-500/20">
        <div className="flex-1">
          <p className="text-xs text-slate-300">{recommendation}</p>
        </div>
        <button onClick={() => navigate(recPath)} className="btn-primary text-xs py-1.5 px-3 flex-shrink-0">
          {recAction} <ArrowRight className="w-3 h-3" />
        </button>
      </div>
    </div>
  )
}
