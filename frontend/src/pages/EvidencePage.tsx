import { useState, useRef, useCallback } from 'react'
import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import EmptyState from '../components/shared/EmptyState'
import { FileText, Upload, File, Image, FileType, Trash2, CheckCircle, AlertCircle, Clock, Search } from 'lucide-react'
import * as api from '../api/client'

const FILE_ICONS: Record<string, typeof FileText> = {
  pdf: File,
  png: Image,
  jpg: Image,
  jpeg: Image,
  txt: FileType,
  docx: FileText,
  csv: FileText,
}

function formatBytes(bytes: number) {
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB'
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB'
}

function ParseStatusBadge({ status }: { status: string }) {
  switch (status) {
    case 'COMPLETED':
      return <span className="badge-pass"><CheckCircle className="w-3 h-3 mr-1" />Parsed</span>
    case 'FAILED':
    case 'NO_CONTENT':
      return <span className="badge-fail"><AlertCircle className="w-3 h-3 mr-1" />{status}</span>
    default:
      return <span className="badge-pending"><Clock className="w-3 h-3 mr-1" />{status}</span>
  }
}

export default function EvidencePage() {
  const { currentAudit, evidence, loadEvidence } = useStore()
  const [dragOver, setDragOver] = useState(false)
  const [pendingFiles, setPendingFiles] = useState<File[]>([])
  const [uploading, setUploading] = useState(false)
  const [search, setSearch] = useState('')
  const fileRef = useRef<HTMLInputElement>(null)

  const filtered = evidence.filter(e =>
    !search || e.original_filename.toLowerCase().includes(search.toLowerCase())
  )

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setDragOver(false)
    const files = Array.from(e.dataTransfer.files)
    setPendingFiles(prev => [...prev, ...files])
  }, [])

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || [])
    setPendingFiles(prev => [...prev, ...files])
    if (fileRef.current) fileRef.current.value = ''
  }

  const handleUpload = async () => {
    if (!currentAudit || pendingFiles.length === 0) return
    setUploading(true)
    try {
      await api.uploadEvidence(currentAudit.audit_id, pendingFiles)
      await loadEvidence(currentAudit.audit_id)
      setPendingFiles([])
    } catch (err) {
      console.error(err)
    }
    setUploading(false)
  }

  if (!currentAudit) {
    return (
      <div className="p-6">
        <EmptyState icon={FileText} title="No Audit Selected" description="Select an audit from the sidebar to manage evidence." />
      </div>
    )
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <PageHeader
        title="Evidence Library"
        subtitle={`${evidence.length} documents uploaded`}
      />

      {/* Upload Zone */}
      <div
        onDragOver={e => { e.preventDefault(); setDragOver(true) }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
        className={`mb-6 border-2 border-dashed rounded-xl p-8 text-center transition-all duration-200 ${
          dragOver ? 'border-brand-500 bg-brand-500/5' : 'border-surface-border hover:border-slate-500'
        }`}
      >
        <Upload className="w-10 h-10 text-slate-500 mx-auto mb-3" />
        <p className="text-sm text-slate-300 mb-1">Drag & drop evidence files here</p>
        <p className="text-xs text-slate-500 mb-3">PDF, DOCX, TXT, images — or click to browse</p>
        <input ref={fileRef} type="file" multiple className="hidden" onChange={handleFileSelect} />
        <button onClick={() => fileRef.current?.click()} className="btn-secondary text-xs">
          Browse Files
        </button>
      </div>

      {/* Pending Files */}
      {pendingFiles.length > 0 && (
        <div className="card mb-6 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-slate-300">{pendingFiles.length} files ready to upload</h3>
            <div className="flex gap-2">
              <button onClick={() => setPendingFiles([])} className="btn-secondary text-xs py-1.5">Clear</button>
              <button onClick={handleUpload} className="btn-primary text-xs py-1.5" disabled={uploading}>
                {uploading ? 'Uploading...' : 'Upload All'}
              </button>
            </div>
          </div>
          <div className="space-y-1.5">
            {pendingFiles.map((f, i) => (
              <div key={i} className="flex items-center gap-3 px-3 py-2 rounded-lg bg-surface-dark text-sm">
                <FileText className="w-4 h-4 text-slate-500" />
                <span className="flex-1 truncate text-slate-300">{f.name}</span>
                <span className="text-xs text-slate-500">{formatBytes(f.size)}</span>
                <button onClick={() => setPendingFiles(prev => prev.filter((_, j) => j !== i))} className="text-slate-500 hover:text-verdict-fail">
                  <Trash2 className="w-3.5 h-3.5" />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Search */}
      <div className="relative max-w-md mb-4">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
        <input
          className="input-field pl-9"
          placeholder="Search evidence..."
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
      </div>

      {/* Evidence Table */}
      {filtered.length > 0 ? (
        <div className="card overflow-hidden">
          <table className="w-full">
            <thead className="bg-surface-elevated">
              <tr>
                <th className="table-header">Filename</th>
                <th className="table-header w-20">Type</th>
                <th className="table-header w-24">Size</th>
                <th className="table-header w-28">Status</th>
                <th className="table-header w-36">Uploaded</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {filtered.map(e => {
                const Icon = FILE_ICONS[e.file_type] || FileText
                return (
                  <tr key={e.document_id} className="hover:bg-slate-800/50 transition-colors">
                    <td className="table-cell">
                      <div className="flex items-center gap-2">
                        <Icon className="w-4 h-4 text-slate-400" />
                        <span className="text-slate-200">{e.original_filename}</span>
                      </div>
                    </td>
                    <td className="table-cell text-xs text-slate-400 uppercase">{e.file_type}</td>
                    <td className="table-cell text-xs text-slate-400">{formatBytes(e.file_size_bytes)}</td>
                    <td className="table-cell"><ParseStatusBadge status={e.parse_status} /></td>
                    <td className="table-cell text-xs text-slate-500">
                      {new Date(e.uploaded_at).toLocaleDateString()}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      ) : evidence.length === 0 ? (
        <EmptyState
          icon={FileText}
          title="No Evidence Uploaded"
          description="Upload evidence documents to begin your audit analysis."
        />
      ) : (
        <EmptyState icon={Search} title="No Matching Evidence" description="Try adjusting your search." />
      )}
    </div>
  )
}
