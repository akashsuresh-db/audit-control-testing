import { useLocation, useNavigate } from 'react-router-dom'
import { useStore } from '../../store/useStore'
import {
  LayoutDashboard,
  Shield,
  FileText,
  Search,
  AlertTriangle,
  BarChart3,
  ClipboardList,
  ChevronLeft,
  ChevronRight,
  Plus,
  Building2,
} from 'lucide-react'
import { useState } from 'react'

const navItems = [
  { path: '/dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { path: '/controls', label: 'Controls', icon: Shield },
  { path: '/evidence', label: 'Evidence Library', icon: FileText },
  { path: '/investigation', label: 'Investigation', icon: Search },
  { path: '/findings', label: 'Findings', icon: AlertTriangle },
  { path: '/reports', label: 'Reports', icon: BarChart3 },
  { path: '/audit-log', label: 'Audit Trail', icon: ClipboardList },
]

export default function Sidebar() {
  const location = useLocation()
  const navigate = useNavigate()
  const { audits, currentAudit, selectAudit, sidebarCollapsed, toggleSidebar } = useStore()
  const [showAuditPicker, setShowAuditPicker] = useState(false)

  return (
    <aside
      className={`fixed left-0 top-0 h-screen bg-slate-900 border-r border-surface-border flex flex-col z-40 transition-all duration-300 ${
        sidebarCollapsed ? 'w-16' : 'w-64'
      }`}
    >
      {/* Logo */}
      <div className="flex items-center gap-3 px-4 py-5 border-b border-surface-border">
        <div className="w-8 h-8 rounded-lg bg-brand-600 flex items-center justify-center flex-shrink-0">
          <Shield className="w-4 h-4 text-white" />
        </div>
        {!sidebarCollapsed && (
          <div className="overflow-hidden">
            <h1 className="text-base font-bold text-white tracking-tight">AuditLens</h1>
            <p className="text-[10px] text-slate-500 uppercase tracking-widest">Enterprise Audit Platform</p>
          </div>
        )}
      </div>

      {/* Audit Selector */}
      {!sidebarCollapsed && (
        <div className="px-3 py-3 border-b border-surface-border">
          <button
            onClick={() => setShowAuditPicker(!showAuditPicker)}
            className="w-full flex items-center gap-2 px-3 py-2 rounded-lg bg-surface-card border border-surface-border text-sm text-slate-300 hover:border-brand-500/50 transition-colors"
          >
            <Building2 className="w-4 h-4 text-slate-400" />
            <span className="truncate flex-1 text-left">
              {currentAudit?.audit_name || 'Select Audit'}
            </span>
          </button>
          {showAuditPicker && (
            <div className="mt-2 rounded-lg bg-surface-elevated border border-surface-border overflow-hidden">
              {audits.map(a => (
                <button
                  key={a.audit_id}
                  onClick={() => {
                    selectAudit(a.audit_id)
                    setShowAuditPicker(false)
                  }}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-slate-700 transition-colors ${
                    currentAudit?.audit_id === a.audit_id ? 'text-brand-400 bg-brand-500/10' : 'text-slate-300'
                  }`}
                >
                  <div className="font-medium truncate">{a.audit_name}</div>
                  <div className="text-xs text-slate-500">{a.framework} — {a.client_name}</div>
                </button>
              ))}
              <button
                onClick={() => navigate('/dashboard')}
                className="w-full text-left px-3 py-2 text-sm text-brand-400 hover:bg-slate-700 flex items-center gap-2 border-t border-surface-border"
              >
                <Plus className="w-3 h-3" /> New Audit
              </button>
            </div>
          )}
        </div>
      )}

      {/* Navigation */}
      <nav className="flex-1 px-2 py-3 space-y-0.5 overflow-y-auto">
        {navItems.map(item => {
          const isActive = location.pathname === item.path
          const Icon = item.icon
          return (
            <button
              key={item.path}
              onClick={() => navigate(item.path)}
              className={`sidebar-item w-full ${isActive ? 'active' : ''}`}
              title={sidebarCollapsed ? item.label : undefined}
            >
              <Icon className="w-5 h-5 flex-shrink-0" />
              {!sidebarCollapsed && <span>{item.label}</span>}
            </button>
          )
        })}
      </nav>

      {/* Collapse toggle */}
      <div className="px-2 py-3 border-t border-surface-border">
        <button
          onClick={toggleSidebar}
          className="sidebar-item w-full justify-center"
        >
          {sidebarCollapsed ? (
            <ChevronRight className="w-5 h-5" />
          ) : (
            <>
              <ChevronLeft className="w-5 h-5" />
              <span>Collapse</span>
            </>
          )}
        </button>
      </div>

      {/* Footer */}
      {!sidebarCollapsed && (
        <div className="px-4 py-3 border-t border-surface-border">
          <p className="text-[10px] text-slate-600 text-center">
            Powered by Databricks Lakebase
          </p>
        </div>
      )}
    </aside>
  )
}
