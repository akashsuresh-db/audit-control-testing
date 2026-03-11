import { create } from 'zustand'
import type {
  Audit,
  Control,
  EvidenceDocument,
  EvaluationResult,
  EvidenceMatch,
  Annotation,
  Finding,
  DashboardStats,
} from '../types'
import * as api from '../api/client'

interface AppState {
  // Data
  audits: Audit[]
  currentAudit: Audit | null
  controls: Control[]
  evidence: EvidenceDocument[]
  results: EvaluationResult[]
  evidenceMatches: EvidenceMatch[]
  annotations: Annotation[]
  findings: Finding[]
  dashboardStats: DashboardStats | null

  // UI State
  selectedControlId: string | null
  selectedDocumentId: string | null
  activeAnnotationId: string | null
  sidebarCollapsed: boolean
  isLoading: boolean
  error: string | null

  // Actions
  loadAudits: () => Promise<void>
  selectAudit: (auditId: string) => Promise<void>
  createAudit: (data: Partial<Audit>) => Promise<Audit>
  loadControls: (auditId: string) => Promise<void>
  loadEvidence: (auditId: string) => Promise<void>
  loadResults: (auditId: string) => Promise<void>
  loadAnnotations: (auditId: string, documentId?: string) => Promise<void>
  loadFindings: (auditId: string) => Promise<void>
  loadDashboardStats: (auditId: string) => Promise<void>
  selectControl: (controlId: string | null) => void
  selectDocument: (documentId: string | null) => void
  setActiveAnnotation: (annotationId: string | null) => void
  toggleSidebar: () => void
  clearError: () => void
}

export const useStore = create<AppState>((set, get) => ({
  audits: [],
  currentAudit: null,
  controls: [],
  evidence: [],
  results: [],
  evidenceMatches: [],
  annotations: [],
  findings: [],
  dashboardStats: null,
  selectedControlId: null,
  selectedDocumentId: null,
  activeAnnotationId: null,
  sidebarCollapsed: false,
  isLoading: false,
  error: null,

  loadAudits: async () => {
    try {
      set({ isLoading: true, error: null })
      const audits = await api.getAudits()
      set({ audits, isLoading: false })
    } catch (e: any) {
      set({ error: e.message, isLoading: false })
    }
  },

  selectAudit: async (auditId: string) => {
    try {
      set({ isLoading: true, error: null })
      const audit = await api.getAudit(auditId)
      set({ currentAudit: audit, isLoading: false })
      const { loadControls, loadEvidence, loadResults, loadDashboardStats } = get()
      await Promise.all([
        loadControls(auditId),
        loadEvidence(auditId),
        loadResults(auditId),
        loadDashboardStats(auditId),
      ])
    } catch (e: any) {
      set({ error: e.message, isLoading: false })
    }
  },

  createAudit: async (data) => {
    const audit = await api.createAudit(data)
    set(s => ({ audits: [...s.audits, audit] }))
    return audit
  },

  loadControls: async (auditId) => {
    try {
      const controls = await api.getControls(auditId)
      set({ controls })
    } catch { /* empty */ }
  },

  loadEvidence: async (auditId) => {
    try {
      const evidence = await api.getEvidence(auditId)
      set({ evidence })
    } catch { /* empty */ }
  },

  loadResults: async (auditId) => {
    try {
      const [results, evidenceMatches] = await Promise.all([
        api.getResults(auditId),
        api.getEvidenceMatches(auditId),
      ])
      set({ results, evidenceMatches })
    } catch { /* empty */ }
  },

  loadAnnotations: async (auditId, documentId?) => {
    try {
      const annotations = await api.getAnnotations(auditId, documentId)
      set({ annotations })
    } catch { /* empty */ }
  },

  loadFindings: async (auditId) => {
    try {
      const findings = await api.getFindings(auditId)
      set({ findings })
    } catch { /* empty */ }
  },

  loadDashboardStats: async (auditId) => {
    try {
      const dashboardStats = await api.getDashboardStats(auditId)
      set({ dashboardStats })
    } catch { /* empty */ }
  },

  selectControl: (controlId) => set({ selectedControlId: controlId }),
  selectDocument: (documentId) => set({ selectedDocumentId: documentId }),
  setActiveAnnotation: (annotationId) => set({ activeAnnotationId: annotationId }),
  toggleSidebar: () => set(s => ({ sidebarCollapsed: !s.sidebarCollapsed })),
  clearError: () => set({ error: null }),
}))
