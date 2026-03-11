import { useEffect, useState } from 'react'
import { useStore } from '../store/useStore'
import PageHeader from '../components/shared/PageHeader'
import EmptyState from '../components/shared/EmptyState'
import { ClipboardList, Upload, Search as SearchIcon, CheckCircle, Play, Edit, FileText } from 'lucide-react'
import * as api from '../api/client'
import type { AuditLog } from '../types'

const ACTION_ICONS: Record<string, typeof Upload> = {
  UPLOAD: Upload,
  PARSE: FileText,
  EVALUATE: Play,
  REVIEW: CheckCircle,
  OVERRIDE: Edit,
}

export default function AuditLogPage() {
  const { currentAudit } = useStore()
  const [logs, setLogs] = useState<AuditLog[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!currentAudit) return
    setLoading(true)
    api.getAuditLog(currentAudit.audit_id).then(setLogs).catch(() => {}).finally(() => setLoading(false))
  }, [currentAudit])

  if (!currentAudit) {
    return (
      <div className="p-6">
        <EmptyState icon={ClipboardList} title="No Audit Selected" description="Select an audit to view the trail." />
      </div>
    )
  }

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <PageHeader title="Audit Trail" subtitle={`Activity log for ${currentAudit.audit_name}`} />

      {loading ? (
        <div className="text-center py-12 text-slate-500">Loading...</div>
      ) : logs.length > 0 ? (
        <div className="relative">
          <div className="absolute left-6 top-0 bottom-0 w-px bg-surface-border" />
          <div className="space-y-0">
            {logs.map(log => {
              const Icon = ACTION_ICONS[log.action] || ClipboardList
              return (
                <div key={log.log_id} className="relative pl-14 py-3">
                  <div className="absolute left-4 w-5 h-5 rounded-full bg-surface-card border border-surface-border flex items-center justify-center">
                    <Icon className="w-3 h-3 text-slate-400" />
                  </div>
                  <div className="card p-3">
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-2">
                        <span className="badge bg-brand-500/15 text-brand-400 text-xs">{log.action}</span>
                        <span className="text-xs text-slate-500">{log.entity_type}</span>
                      </div>
                      <span className="text-xs text-slate-500">
                        {new Date(log.timestamp).toLocaleString()}
                      </span>
                    </div>
                    <p className="text-sm text-slate-300">{log.details}</p>
                    <p className="text-xs text-slate-500 mt-1">by {log.user_id}</p>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      ) : (
        <EmptyState icon={ClipboardList} title="No Activity" description="No audit trail entries yet." />
      )}
    </div>
  )
}
