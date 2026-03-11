import axios from 'axios'
import type {
  Audit,
  Control,
  EvidenceDocument,
  EvaluationResult,
  EvidenceMatch,
  Annotation,
  Finding,
  AuditLog,
  DashboardStats,
  PipelineStatus,
} from '../types'

const api = axios.create({
  baseURL: '/api',
  headers: { 'Content-Type': 'application/json' },
})

// Audits
export const getAudits = () => api.get<Audit[]>('/audits').then(r => r.data)
export const getAudit = (id: string) => api.get<Audit>(`/audits/${id}`).then(r => r.data)
export const createAudit = (data: Partial<Audit>) => api.post<Audit>('/audits', data).then(r => r.data)

// Controls
export const getControls = (auditId: string) =>
  api.get<Control[]>(`/audits/${auditId}/controls`).then(r => r.data)
export const uploadControls = (auditId: string, file: File) => {
  const fd = new FormData()
  fd.append('file', file)
  return api.post(`/audits/${auditId}/controls`, fd, {
    headers: { 'Content-Type': 'multipart/form-data' },
  }).then(r => r.data)
}

// Evidence
export const getEvidence = (auditId: string) =>
  api.get<EvidenceDocument[]>(`/audits/${auditId}/evidence`).then(r => r.data)
export const uploadEvidence = (auditId: string, files: File[]) => {
  const fd = new FormData()
  files.forEach(f => fd.append('files', f))
  return api.post(`/audits/${auditId}/evidence`, fd, {
    headers: { 'Content-Type': 'multipart/form-data' },
  }).then(r => r.data)
}
export const getEvidenceContent = (documentId: string) =>
  api.get<{ document_id: string; content: string; file_type: string }>(`/evidence/${documentId}/content`).then(r => r.data)

// Pipeline
export const triggerPipeline = (auditId: string) =>
  api.post<PipelineStatus>(`/audits/${auditId}/evaluate`).then(r => r.data)
export const getPipelineStatus = (auditId: string) =>
  api.get<PipelineStatus>(`/audits/${auditId}/status`).then(r => r.data)

// Results
export const getResults = (auditId: string) =>
  api.get<EvaluationResult[]>(`/audits/${auditId}/results`).then(r => r.data)
export const getResultsSummary = (auditId: string) =>
  api.get<DashboardStats>(`/audits/${auditId}/results/summary`).then(r => r.data)
export const getEvidenceMatches = (auditId: string) =>
  api.get<EvidenceMatch[]>(`/audits/${auditId}/evidence-matches`).then(r => r.data)
export const submitReview = (evalId: string, data: { verdict: string; notes: string; auditor_id: string }) =>
  api.put(`/results/${evalId}/review`, data).then(r => r.data)

// Annotations
export const getAnnotations = (auditId: string, documentId?: string) => {
  const params = documentId ? { document_id: documentId } : {}
  return api.get<Annotation[]>(`/audits/${auditId}/annotations`, { params }).then(r => r.data)
}
export const getAnnotationsForControl = (auditId: string, controlId: string) =>
  api.get<Annotation[]>(`/audits/${auditId}/annotations`, { params: { control_id: controlId } }).then(r => r.data)

// Findings
export const getFindings = (auditId: string) =>
  api.get<Finding[]>(`/audits/${auditId}/findings`).then(r => r.data)
export const updateFinding = (findingId: string, data: Partial<Finding>) =>
  api.put(`/findings/${findingId}`, data).then(r => r.data)

// Audit Log
export const getAuditLog = (auditId: string, limit = 100) =>
  api.get<AuditLog[]>(`/audits/${auditId}/audit-log`, { params: { limit } }).then(r => r.data)

// Dashboard
export const getDashboardStats = (auditId: string) =>
  api.get<DashboardStats>(`/audits/${auditId}/dashboard`).then(r => r.data)

// Export
export const exportResults = (auditId: string) =>
  api.get(`/audits/${auditId}/export`).then(r => r.data)
