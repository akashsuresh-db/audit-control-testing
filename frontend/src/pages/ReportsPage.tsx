import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import VerdictBadge from '../components/shared/VerdictBadge'
import EmptyState from '../components/shared/EmptyState'
import { BarChart3, Download, FileText, Printer } from 'lucide-react'
import {
  PieChart, Pie, Cell, ResponsiveContainer,
  RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar,
} from 'recharts'
import * as api from '../api/client'

const VERDICT_COLORS = { PASS: '#22c55e', FAIL: '#ef4444', INSUFFICIENT_EVIDENCE: '#f59e0b' }

export default function ReportsPage() {
  const { currentAudit, controls, results, evidence } = useStore()

  if (!currentAudit) {
    return (
      <div className="p-6">
        <EmptyState icon={BarChart3} title="No Audit Selected" description="Select an audit to view reports." />
      </div>
    )
  }

  const passCount = results.filter(r => r.ai_verdict === 'PASS').length
  const failCount = results.filter(r => r.ai_verdict === 'FAIL').length
  const insuffCount = results.filter(r => r.ai_verdict === 'INSUFFICIENT_EVIDENCE').length
  const reviewedCount = results.filter(r => r.auditor_verdict).length
  const complianceRate = results.length > 0 ? Math.round((passCount / results.length) * 100) : 0

  const pieData = [
    { name: 'Pass', value: passCount, color: VERDICT_COLORS.PASS },
    { name: 'Fail', value: failCount, color: VERDICT_COLORS.FAIL },
    { name: 'Insufficient', value: insuffCount, color: VERDICT_COLORS.INSUFFICIENT_EVIDENCE },
  ].filter(d => d.value > 0)

  // Category compliance radar
  const categories = [...new Set(controls.map(c => c.control_category).filter(Boolean))]
  const radarData = categories.map(cat => {
    const catControls = controls.filter(c => c.control_category === cat)
    const catResults = results.filter(r => catControls.some(c => c.control_id === r.control_id))
    const catPass = catResults.filter(r => r.ai_verdict === 'PASS').length
    return {
      category: cat.length > 12 ? cat.slice(0, 12) + '...' : cat,
      compliance: catResults.length > 0 ? Math.round((catPass / catResults.length) * 100) : 0,
    }
  })

  const handleExport = async () => {
    try {
      const data = await api.exportResults(currentAudit.audit_id)
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${currentAudit.audit_name.replace(/\s+/g, '_')}_report.json`
      a.click()
      URL.revokeObjectURL(url)
    } catch (e) { console.error(e) }
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <PageHeader
        title="Audit Report"
        subtitle={`${currentAudit.audit_name} — ${currentAudit.framework}`}
        actions={
          <div className="flex gap-3">
            <button onClick={handleExport} className="btn-primary">
              <Download className="w-4 h-4" /> Export JSON
            </button>
            <button onClick={() => window.print()} className="btn-secondary">
              <Printer className="w-4 h-4" /> Print
            </button>
          </div>
        }
      />

      {/* Executive Summary */}
      <div className="card p-6 mb-6">
        <h2 className="text-lg font-semibold text-white mb-4">Executive Summary</h2>
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-6">
          <div className="text-center">
            <p className="text-3xl font-bold text-white">{controls.length}</p>
            <p className="text-xs text-slate-400 mt-1">Total Controls</p>
          </div>
          <div className="text-center">
            <p className="text-3xl font-bold text-verdict-pass">{passCount}</p>
            <p className="text-xs text-slate-400 mt-1">Passing</p>
          </div>
          <div className="text-center">
            <p className="text-3xl font-bold text-verdict-fail">{failCount}</p>
            <p className="text-xs text-slate-400 mt-1">Failures</p>
          </div>
          <div className="text-center">
            <p className="text-3xl font-bold text-brand-400">{complianceRate}%</p>
            <p className="text-xs text-slate-400 mt-1">Compliance Rate</p>
          </div>
          <div className="text-center">
            <p className="text-3xl font-bold text-slate-300">{reviewedCount}/{results.length}</p>
            <p className="text-xs text-slate-400 mt-1">Reviewed</p>
          </div>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <div className="card p-6">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">Verdict Distribution</h3>
          {pieData.length > 0 ? (
            <ResponsiveContainer width="100%" height={250}>
              <PieChart>
                <Pie data={pieData} cx="50%" cy="50%" innerRadius={60} outerRadius={100} dataKey="value" label={({ name, value }) => `${name}: ${value}`} strokeWidth={0}>
                  {pieData.map((entry, i) => <Cell key={i} fill={entry.color} />)}
                </Pie>
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[250px] flex items-center justify-center text-slate-500 text-sm">No data</div>
          )}
        </div>

        <div className="card p-6">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">Category Compliance</h3>
          {radarData.length > 0 ? (
            <ResponsiveContainer width="100%" height={250}>
              <RadarChart data={radarData}>
                <PolarGrid stroke="#334155" />
                <PolarAngleAxis dataKey="category" tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <PolarRadiusAxis angle={30} domain={[0, 100]} tick={{ fill: '#64748b', fontSize: 10 }} />
                <Radar dataKey="compliance" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.2} />
              </RadarChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[250px] flex items-center justify-center text-slate-500 text-sm">No data</div>
          )}
        </div>
      </div>

      {/* Detailed Results Table */}
      <div className="card overflow-hidden">
        <div className="px-5 py-4 border-b border-surface-border">
          <h3 className="text-sm font-semibold text-slate-300">Detailed Results</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-surface-elevated">
              <tr>
                <th className="table-header w-28">Code</th>
                <th className="table-header">Control</th>
                <th className="table-header w-24">Risk</th>
                <th className="table-header w-28">AI Verdict</th>
                <th className="table-header w-24">Confidence</th>
                <th className="table-header w-28">Auditor</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {results.map(r => (
                <tr key={r.evaluation_id} className="hover:bg-slate-800/50">
                  <td className="table-cell font-mono text-xs text-brand-400">{r.control_code}</td>
                  <td className="table-cell text-sm text-slate-200">{r.control_title}</td>
                  <td className="table-cell"><span className={`badge border text-xs ${r.risk_level === 'HIGH' ? 'risk-high' : r.risk_level === 'MEDIUM' ? 'risk-medium' : 'risk-low'}`}>{r.risk_level}</span></td>
                  <td className="table-cell"><VerdictBadge verdict={r.ai_verdict} size="sm" /></td>
                  <td className="table-cell text-xs text-slate-400">{Math.round(r.ai_confidence * 100)}%</td>
                  <td className="table-cell">{r.auditor_verdict ? <VerdictBadge verdict={r.auditor_verdict} size="sm" /> : <span className="text-xs text-slate-500">Pending</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
