import { useEffect, useState } from 'react'
import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import StatCard from '../components/shared/StatCard'
import Modal from '../components/shared/Modal'
import WorkflowTracker from '../components/shared/WorkflowTracker'
import {
  Shield,
  FileText,
  AlertTriangle,
  CheckCircle,
  XCircle,
  TrendingUp,
  Plus,
  Play,
  Loader2,
} from 'lucide-react'
import {
  PieChart, Pie, Cell, ResponsiveContainer,
  BarChart, Bar, XAxis, YAxis, Tooltip,
  AreaChart, Area, CartesianGrid,
} from 'recharts'
import * as api from '../api/client'

const VERDICT_COLORS = { PASS: '#22c55e', FAIL: '#ef4444', INSUFFICIENT_EVIDENCE: '#f59e0b', PENDING: '#6b7280' }

export default function DashboardPage() {
  const { currentAudit, audits, controls, evidence, results, selectAudit, loadAudits, createAudit } = useStore()
  const [showNewAudit, setShowNewAudit] = useState(false)
  const [form, setForm] = useState({ audit_name: '', framework: 'SOX', client_name: '', description: '' })
  const [pipelineRunning, setPipelineRunning] = useState(false)
  const [pipelineMsg, setPipelineMsg] = useState('')

  useEffect(() => { loadAudits() }, [loadAudits])

  const passCount = results.filter(r => r.ai_verdict === 'PASS').length
  const failCount = results.filter(r => r.ai_verdict === 'FAIL').length
  const insuffCount = results.filter(r => r.ai_verdict === 'INSUFFICIENT_EVIDENCE').length
  const pendingCount = controls.length - results.length
  const avgConf = results.length > 0 ? results.reduce((s, r) => s + r.ai_confidence, 0) / results.length : 0
  const complianceRate = results.length > 0 ? Math.round((passCount / results.length) * 100) : 0

  const pieData = [
    { name: 'Pass', value: passCount, color: VERDICT_COLORS.PASS },
    { name: 'Fail', value: failCount, color: VERDICT_COLORS.FAIL },
    { name: 'Insufficient', value: insuffCount, color: VERDICT_COLORS.INSUFFICIENT_EVIDENCE },
    { name: 'Pending', value: pendingCount > 0 ? pendingCount : 0, color: VERDICT_COLORS.PENDING },
  ].filter(d => d.value > 0)

  const categoryData = controls.reduce<Record<string, { pass: number; fail: number; insuf: number }>>((acc, c) => {
    const cat = c.control_category || 'Uncategorized'
    if (!acc[cat]) acc[cat] = { pass: 0, fail: 0, insuf: 0 }
    const r = results.find(r => r.control_id === c.control_id)
    if (r?.ai_verdict === 'PASS') acc[cat].pass++
    else if (r?.ai_verdict === 'FAIL') acc[cat].fail++
    else acc[cat].insuf++
    return acc
  }, {})

  const barData = Object.entries(categoryData).map(([name, v]) => ({ name: name.length > 15 ? name.slice(0, 15) + '...' : name, ...v }))

  const riskData = [
    { name: 'HIGH', count: controls.filter(c => c.risk_level === 'HIGH').length, failures: results.filter(r => r.risk_level === 'HIGH' && r.ai_verdict === 'FAIL').length },
    { name: 'MEDIUM', count: controls.filter(c => c.risk_level === 'MEDIUM').length, failures: results.filter(r => r.risk_level === 'MEDIUM' && r.ai_verdict === 'FAIL').length },
    { name: 'LOW', count: controls.filter(c => c.risk_level === 'LOW').length, failures: results.filter(r => r.risk_level === 'LOW' && r.ai_verdict === 'FAIL').length },
  ]

  const handleCreate = async () => {
    const a = await createAudit(form)
    setShowNewAudit(false)
    setForm({ audit_name: '', framework: 'SOX', client_name: '', description: '' })
    selectAudit(a.audit_id)
  }

  const handleRunPipeline = async () => {
    if (!currentAudit) return
    setPipelineRunning(true)
    setPipelineMsg('Pipeline triggered...')
    try {
      await api.triggerPipeline(currentAudit.audit_id)
      const interval = setInterval(async () => {
        try {
          const s = await api.getPipelineStatus(currentAudit.audit_id)
          setPipelineMsg(s.message || s.status)
          if (s.status === 'COMPLETED' || s.status === 'FAILED' || s.status === 'TERMINATED') {
            clearInterval(interval)
            setPipelineRunning(false)
            if (s.status === 'COMPLETED') {
              selectAudit(currentAudit.audit_id)
            }
          }
        } catch {
          clearInterval(interval)
          setPipelineRunning(false)
        }
      }, 3000)
    } catch (e: any) {
      setPipelineMsg('Error: ' + e.message)
      setPipelineRunning(false)
    }
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <PageHeader
        title="Dashboard"
        subtitle={currentAudit ? `${currentAudit.audit_name} — ${currentAudit.framework}` : 'Select or create an audit to begin'}
        actions={
          <div className="flex gap-3">
            {currentAudit && (
              <button onClick={handleRunPipeline} disabled={pipelineRunning} className="btn-primary">
                {pipelineRunning ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                {pipelineRunning ? 'Running...' : 'Run Pipeline'}
              </button>
            )}
            <button onClick={() => setShowNewAudit(true)} className="btn-secondary">
              <Plus className="w-4 h-4" /> New Audit
            </button>
          </div>
        }
      />

      {pipelineMsg && (
        <div className="mb-4 px-4 py-3 rounded-lg bg-brand-500/10 border border-brand-500/30 text-sm text-brand-300">
          {pipelineMsg}
        </div>
      )}

      {!currentAudit ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mt-8">
          {audits.map(a => (
            <button
              key={a.audit_id}
              onClick={() => selectAudit(a.audit_id)}
              className="card-hover p-5 text-left"
            >
              <div className="flex items-start justify-between mb-3">
                <span className="badge bg-brand-500/15 text-brand-400">{a.framework}</span>
                <span className={`badge ${a.status === 'COMPLETED' ? 'bg-verdict-pass/15 text-verdict-pass' : 'bg-brand-500/15 text-brand-300'}`}>
                  {a.status}
                </span>
              </div>
              <h3 className="text-lg font-semibold text-white mb-1">{a.audit_name}</h3>
              <p className="text-sm text-slate-400">{a.client_name}</p>
              <p className="text-xs text-slate-500 mt-2">{a.description}</p>
            </button>
          ))}
        </div>
      ) : (
        <>
          {/* Workflow Progress Tracker */}
          <WorkflowTracker />

          {/* Stats Grid */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            <StatCard label="Controls" value={controls.length} icon={Shield} />
            <StatCard label="Evidence Files" value={evidence.length} icon={FileText} />
            <StatCard label="Compliance Rate" value={`${complianceRate}%`} icon={TrendingUp} variant="success" />
            <StatCard label="Failures" value={failCount} icon={AlertTriangle} variant={failCount > 0 ? 'danger' : 'default'} />
          </div>

          {/* Charts Row */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
            {/* Verdict Pie */}
            <div className="card p-5">
              <h3 className="text-sm font-semibold text-slate-300 mb-4">Verdict Distribution</h3>
              {pieData.length > 0 ? (
                <ResponsiveContainer width="100%" height={200}>
                  <PieChart>
                    <Pie data={pieData} cx="50%" cy="50%" innerRadius={50} outerRadius={80} dataKey="value" strokeWidth={0}>
                      {pieData.map((entry, i) => (
                        <Cell key={i} fill={entry.color} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
                      itemStyle={{ color: '#e2e8f0' }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-[200px] flex items-center justify-center text-slate-500 text-sm">No results yet</div>
              )}
              <div className="flex justify-center gap-4 mt-2">
                {pieData.map(d => (
                  <div key={d.name} className="flex items-center gap-1.5 text-xs text-slate-400">
                    <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: d.color }} />
                    {d.name} ({d.value})
                  </div>
                ))}
              </div>
            </div>

            {/* Category Bar Chart */}
            <div className="card p-5">
              <h3 className="text-sm font-semibold text-slate-300 mb-4">Results by Category</h3>
              {barData.length > 0 ? (
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={barData} layout="vertical" margin={{ left: 60 }}>
                    <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                    <YAxis type="category" dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} width={60} />
                    <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }} />
                    <Bar dataKey="pass" stackId="a" fill={VERDICT_COLORS.PASS} radius={[0, 0, 0, 0]} />
                    <Bar dataKey="fail" stackId="a" fill={VERDICT_COLORS.FAIL} />
                    <Bar dataKey="insuf" stackId="a" fill={VERDICT_COLORS.INSUFFICIENT_EVIDENCE} radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-[220px] flex items-center justify-center text-slate-500 text-sm">No data</div>
              )}
            </div>

            {/* Risk Analysis */}
            <div className="card p-5">
              <h3 className="text-sm font-semibold text-slate-300 mb-4">Risk Exposure</h3>
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={riskData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                  <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                  <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }} />
                  <Area type="monotone" dataKey="count" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.15} name="Total Controls" />
                  <Area type="monotone" dataKey="failures" stroke="#ef4444" fill="#ef4444" fillOpacity={0.15} name="Failures" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Recent Results */}
          <div className="card">
            <div className="px-5 py-4 border-b border-surface-border flex items-center justify-between">
              <h3 className="text-sm font-semibold text-slate-300">Recent Evaluations</h3>
              <span className="text-xs text-slate-500">{results.length} results</span>
            </div>
            <div className="divide-y divide-surface-border">
              {results.slice(0, 8).map(r => (
                <div key={r.evaluation_id} className="px-5 py-3 flex items-center gap-4 hover:bg-slate-800/50 transition-colors">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-mono text-slate-500">{r.control_code}</span>
                      <span className="text-sm font-medium text-slate-200 truncate">{r.control_title}</span>
                    </div>
                  </div>
                  <span className={`badge ${r.ai_verdict === 'PASS' ? 'badge-pass' : r.ai_verdict === 'FAIL' ? 'badge-fail' : 'badge-insufficient'}`}>
                    {r.ai_verdict === 'PASS' ? <CheckCircle className="w-3 h-3 mr-1" /> : <XCircle className="w-3 h-3 mr-1" />}
                    {r.ai_verdict}
                  </span>
                  <span className="text-xs text-slate-500 w-12 text-right">{Math.round(r.ai_confidence * 100)}%</span>
                  <span className={`badge border text-xs ${r.risk_level === 'HIGH' ? 'risk-high' : r.risk_level === 'MEDIUM' ? 'risk-medium' : 'risk-low'}`}>
                    {r.risk_level}
                  </span>
                </div>
              ))}
              {results.length === 0 && (
                <div className="px-5 py-8 text-center text-sm text-slate-500">
                  No evaluations yet. Upload controls and evidence, then run the pipeline.
                </div>
              )}
            </div>
          </div>
        </>
      )}

      {/* New Audit Modal */}
      <Modal open={showNewAudit} onClose={() => setShowNewAudit(false)} title="Create New Audit">
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Audit Name</label>
            <input
              className="input-field"
              placeholder="Q1 2026 SOX Review"
              value={form.audit_name}
              onChange={e => setForm(f => ({ ...f, audit_name: e.target.value }))}
            />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">Framework</label>
              <select
                className="input-field"
                value={form.framework}
                onChange={e => setForm(f => ({ ...f, framework: e.target.value }))}
              >
                <option>SOX</option>
                <option>SOC2</option>
                <option>ISO27001</option>
                <option>PCI-DSS</option>
                <option>HIPAA</option>
                <option>GDPR</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1.5">Client Name</label>
              <input
                className="input-field"
                placeholder="Acme Corporation"
                value={form.client_name}
                onChange={e => setForm(f => ({ ...f, client_name: e.target.value }))}
              />
            </div>
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Description</label>
            <textarea
              className="input-field"
              rows={3}
              placeholder="Describe the audit scope..."
              value={form.description}
              onChange={e => setForm(f => ({ ...f, description: e.target.value }))}
            />
          </div>
          <div className="flex gap-3 pt-2">
            <button onClick={handleCreate} className="btn-primary flex-1" disabled={!form.audit_name}>
              Create Audit
            </button>
            <button onClick={() => setShowNewAudit(false)} className="btn-secondary">
              Cancel
            </button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
