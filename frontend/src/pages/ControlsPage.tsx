import { useState, useRef } from 'react'
import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import RiskBadge from '../components/shared/RiskBadge'
import EmptyState from '../components/shared/EmptyState'
import { Shield, Upload, Search, Filter } from 'lucide-react'
import * as api from '../api/client'

export default function ControlsPage() {
  const { currentAudit, controls, loadControls } = useStore()
  const [search, setSearch] = useState('')
  const [filterRisk, setFilterRisk] = useState<string>('ALL')
  const [filterCategory, setFilterCategory] = useState<string>('ALL')
  const [uploading, setUploading] = useState(false)
  const fileRef = useRef<HTMLInputElement>(null)

  const categories = [...new Set(controls.map(c => c.control_category).filter(Boolean))]

  const filtered = controls.filter(c => {
    const matchSearch = !search ||
      c.control_code.toLowerCase().includes(search.toLowerCase()) ||
      c.control_title.toLowerCase().includes(search.toLowerCase()) ||
      c.control_description.toLowerCase().includes(search.toLowerCase())
    const matchRisk = filterRisk === 'ALL' || c.risk_level === filterRisk
    const matchCat = filterCategory === 'ALL' || c.control_category === filterCategory
    return matchSearch && matchRisk && matchCat
  })

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !currentAudit) return
    setUploading(true)
    try {
      await api.uploadControls(currentAudit.audit_id, file)
      await loadControls(currentAudit.audit_id)
    } catch (err) {
      console.error(err)
    }
    setUploading(false)
    if (fileRef.current) fileRef.current.value = ''
  }

  if (!currentAudit) {
    return (
      <div className="p-6">
        <EmptyState icon={Shield} title="No Audit Selected" description="Select an audit from the sidebar to view controls." />
      </div>
    )
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <PageHeader
        title="Control Catalog"
        subtitle={`${controls.length} controls loaded for ${currentAudit.framework}`}
        actions={
          <div>
            <input ref={fileRef} type="file" accept=".csv" className="hidden" onChange={handleUpload} />
            <button onClick={() => fileRef.current?.click()} className="btn-primary" disabled={uploading}>
              <Upload className="w-4 h-4" />
              {uploading ? 'Uploading...' : 'Upload Controls CSV'}
            </button>
          </div>
        }
      />

      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-4">
        <div className="relative flex-1 min-w-[200px] max-w-md">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
          <input
            className="input-field pl-9"
            placeholder="Search controls..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>
        <div className="flex items-center gap-2">
          <Filter className="w-4 h-4 text-slate-500" />
          <select className="input-field w-auto" value={filterRisk} onChange={e => setFilterRisk(e.target.value)}>
            <option value="ALL">All Risk Levels</option>
            <option value="HIGH">High</option>
            <option value="MEDIUM">Medium</option>
            <option value="LOW">Low</option>
          </select>
          <select className="input-field w-auto" value={filterCategory} onChange={e => setFilterCategory(e.target.value)}>
            <option value="ALL">All Categories</option>
            {categories.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      </div>

      {/* Table */}
      {filtered.length > 0 ? (
        <div className="card overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-surface-elevated">
                <tr>
                  <th className="table-header w-28">Code</th>
                  <th className="table-header">Title</th>
                  <th className="table-header w-40">Category</th>
                  <th className="table-header w-24">Risk</th>
                  <th className="table-header w-28">Frequency</th>
                  <th className="table-header w-36">Owner</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-surface-border">
                {filtered.map(c => (
                  <tr key={c.control_id} className="hover:bg-slate-800/50 transition-colors">
                    <td className="table-cell font-mono text-brand-400 text-xs">{c.control_code}</td>
                    <td className="table-cell">
                      <div className="font-medium text-slate-200">{c.control_title}</div>
                      <div className="text-xs text-slate-500 mt-0.5 line-clamp-1">{c.control_description}</div>
                    </td>
                    <td className="table-cell text-xs text-slate-400">{c.control_category}</td>
                    <td className="table-cell"><RiskBadge level={c.risk_level} /></td>
                    <td className="table-cell text-xs text-slate-400">{c.frequency}</td>
                    <td className="table-cell text-xs text-slate-400">{c.control_owner}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : controls.length === 0 ? (
        <EmptyState
          icon={Shield}
          title="No Controls Yet"
          description="Upload a CSV file with your control definitions to get started."
          action={{ label: 'Upload Controls', onClick: () => fileRef.current?.click() }}
        />
      ) : (
        <EmptyState
          icon={Search}
          title="No Matching Controls"
          description="Try adjusting your search or filters."
        />
      )}
    </div>
  )
}
