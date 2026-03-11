import { Routes, Route, Navigate } from 'react-router-dom'
import { useEffect } from 'react'
import { useStore } from './store/useStore'
import Sidebar from './components/layout/Sidebar'
import DashboardPage from './pages/DashboardPage'
import ControlsPage from './pages/ControlsPage'
import EvidencePage from './pages/EvidencePage'
import InvestigationPage from './pages/InvestigationPage'
import FindingsPage from './pages/FindingsPage'
import WorkpapersPage from './pages/WorkpapersPage'
import SamplingPage from './pages/SamplingPage'
import ReportsPage from './pages/ReportsPage'
import AuditLogPage from './pages/AuditLogPage'
import MonitoringPage from './pages/MonitoringPage'

export default function App() {
  const { loadAudits, sidebarCollapsed } = useStore()

  useEffect(() => {
    loadAudits()
  }, [loadAudits])

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar />
      <main
        className={`flex-1 overflow-auto transition-all duration-300 ${
          sidebarCollapsed ? 'ml-16' : 'ml-64'
        }`}
      >
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/controls" element={<ControlsPage />} />
          <Route path="/evidence" element={<EvidencePage />} />
          <Route path="/investigation" element={<InvestigationPage />} />
          <Route path="/findings" element={<FindingsPage />} />
          <Route path="/workpapers" element={<WorkpapersPage />} />
          <Route path="/sampling" element={<SamplingPage />} />
          <Route path="/reports" element={<ReportsPage />} />
          <Route path="/monitoring" element={<MonitoringPage />} />
          <Route path="/audit-log" element={<AuditLogPage />} />
        </Routes>
      </main>
    </div>
  )
}
