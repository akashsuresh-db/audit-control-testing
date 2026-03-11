import { useState } from 'react'
import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import EmptyState from '../components/shared/EmptyState'
import { Dice5, Play, Download, Info } from 'lucide-react'
import axios from 'axios'

interface SampleResult {
  population_size: number
  sample_size: number
  method: string
  method_description: string
  confidence_level: number
  margin_of_error: number
  sample_items: number[]
  strata_count: number
}

export default function SamplingPage() {
  const { currentAudit } = useStore()
  const [populationSize, setPopulationSize] = useState(500)
  const [sampleSize, setSampleSize] = useState(25)
  const [method, setMethod] = useState('random')
  const [result, setResult] = useState<SampleResult | null>(null)
  const [loading, setLoading] = useState(false)

  const runSampling = async () => {
    if (!currentAudit) return
    setLoading(true)
    try {
      const resp = await axios.post(`/api/audits/${currentAudit.audit_id}/sampling`, {
        population_size: populationSize,
        sample_size: sampleSize,
        method,
      })
      setResult(resp.data)
    } catch (e) { console.error(e) }
    setLoading(false)
  }

  const exportSample = () => {
    if (!result) return
    const text = `AUDIT SAMPLING WORKPAPER
Audit: ${currentAudit?.audit_name}
Date: ${new Date().toISOString().split('T')[0]}

SAMPLING PARAMETERS
Population Size: ${result.population_size}
Sample Size: ${result.sample_size}
Method: ${result.method_description}
Confidence Level: ${result.confidence_level}%
Margin of Error: ±${result.margin_of_error}%

SELECTED SAMPLE ITEMS
${result.sample_items.map((item, i) => `${String(i + 1).padStart(3)}. Item #${item}`).join('\n')}

AUDITOR NOTES
[Add testing notes for each sample item here]
`
    const blob = new Blob([text], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `sampling_workpaper_${method}_${sampleSize}.txt`
    a.click()
    URL.revokeObjectURL(url)
  }

  if (!currentAudit) {
    return <div className="p-6"><EmptyState icon={Dice5} title="No Audit Selected" description="Select an audit to use sampling." /></div>
  }

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <PageHeader title="Sampling Engine" subtitle="Generate statistically valid test samples" />

      {/* Configuration */}
      <div className="card p-6 mb-6">
        <h3 className="text-sm font-semibold text-slate-300 mb-4">Sampling Parameters</h3>
        <div className="grid grid-cols-3 gap-4 mb-4">
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1.5">Population Size</label>
            <input type="number" className="input-field" value={populationSize} onChange={e => setPopulationSize(parseInt(e.target.value) || 0)} min={1} />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1.5">Sample Size</label>
            <input type="number" className="input-field" value={sampleSize} onChange={e => setSampleSize(parseInt(e.target.value) || 0)} min={1} max={populationSize} />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-400 mb-1.5">Sampling Method</label>
            <select className="input-field" value={method} onChange={e => setMethod(e.target.value)}>
              <option value="random">Random Sampling</option>
              <option value="risk_based">Risk-Based Sampling</option>
              <option value="stratified">Stratified Sampling</option>
            </select>
          </div>
        </div>

        {/* Method descriptions */}
        <div className="bg-surface-dark rounded-lg p-3 mb-4 flex gap-2">
          <Info className="w-4 h-4 text-brand-400 flex-shrink-0 mt-0.5" />
          <p className="text-xs text-slate-400">
            {method === 'random' && 'Simple random sampling selects items with equal probability. Best for homogeneous populations.'}
            {method === 'risk_based' && 'Risk-based sampling weights selection toward higher-risk items. Best when risk varies across the population.'}
            {method === 'stratified' && 'Stratified sampling divides the population into subgroups and samples proportionally from each. Best for heterogeneous populations.'}
          </p>
        </div>

        <button onClick={runSampling} className="btn-primary" disabled={loading || sampleSize > populationSize}>
          <Play className="w-4 h-4" /> {loading ? 'Generating...' : 'Generate Sample'}
        </button>
      </div>

      {/* Results */}
      {result && (
        <div className="space-y-4">
          {/* Summary */}
          <div className="card p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-semibold text-slate-300">Sample Results</h3>
              <button onClick={exportSample} className="btn-secondary text-xs">
                <Download className="w-3.5 h-3.5" /> Export
              </button>
            </div>

            <div className="grid grid-cols-4 gap-4 mb-4">
              <div className="text-center p-3 bg-surface-dark rounded-lg">
                <p className="text-2xl font-bold text-white">{result.sample_size}</p>
                <p className="text-xs text-slate-500">Sample Size</p>
              </div>
              <div className="text-center p-3 bg-surface-dark rounded-lg">
                <p className="text-2xl font-bold text-white">{result.population_size}</p>
                <p className="text-xs text-slate-500">Population</p>
              </div>
              <div className="text-center p-3 bg-surface-dark rounded-lg">
                <p className="text-2xl font-bold text-verdict-pass">{result.confidence_level}%</p>
                <p className="text-xs text-slate-500">Confidence</p>
              </div>
              <div className="text-center p-3 bg-surface-dark rounded-lg">
                <p className="text-2xl font-bold text-verdict-insufficient">±{result.margin_of_error}%</p>
                <p className="text-xs text-slate-500">Margin of Error</p>
              </div>
            </div>

            <p className="text-xs text-slate-400 mb-4">{result.method_description}</p>

            {/* Sample items grid */}
            <div className="grid grid-cols-5 gap-2">
              {result.sample_items.map((item, i) => (
                <div key={i} className="text-center px-2 py-1.5 rounded bg-surface-dark text-xs">
                  <span className="text-slate-500">#{i + 1}</span>
                  <span className="text-slate-300 ml-1 font-mono">Item {item}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Testing worksheet */}
          <div className="card overflow-hidden">
            <div className="px-5 py-3 border-b border-surface-border">
              <h3 className="text-sm font-semibold text-slate-300">Testing Worksheet</h3>
            </div>
            <table className="w-full">
              <thead className="bg-surface-elevated">
                <tr>
                  <th className="table-header w-16">#</th>
                  <th className="table-header">Sample Item</th>
                  <th className="table-header w-28">Test Result</th>
                  <th className="table-header w-32">Exception?</th>
                  <th className="table-header">Notes</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-surface-border">
                {result.sample_items.slice(0, 15).map((item, i) => (
                  <tr key={i} className="hover:bg-slate-800/30">
                    <td className="table-cell text-xs text-slate-500">{i + 1}</td>
                    <td className="table-cell text-sm text-slate-300">Item #{item}</td>
                    <td className="table-cell"><span className="badge-pending text-xs">Pending</span></td>
                    <td className="table-cell text-xs text-slate-500">—</td>
                    <td className="table-cell text-xs text-slate-500 italic">Not yet tested</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {result.sample_items.length > 15 && (
              <div className="px-5 py-2 text-xs text-slate-500 border-t border-surface-border">
                + {result.sample_items.length - 15} more items (export for full list)
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
