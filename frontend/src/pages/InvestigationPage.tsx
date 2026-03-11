import { useState, useEffect, useCallback } from 'react'
import { useStore } from '../store/useStore'
import VerdictBadge from '../components/shared/VerdictBadge'
import RiskBadge from '../components/shared/RiskBadge'
import ConfidenceBar from '../components/shared/ConfidenceBar'
import EmptyState from '../components/shared/EmptyState'
import Modal from '../components/shared/Modal'
import {
  Search, Eye, CheckCircle, XCircle,
  MessageSquare, AlertTriangle, FileText, Sparkles,
  ZoomIn, ZoomOut, ArrowDown,
} from 'lucide-react'
import * as api from '../api/client'
import type { EvaluationResult, EvidenceMatch, Annotation } from '../types'

export default function InvestigationPage() {
  const {
    currentAudit, controls, results, evidenceMatches, evidence,
    selectedControlId, selectControl, loadResults,
  } = useStore()

  const [selectedResult, setSelectedResult] = useState<EvaluationResult | null>(null)
  const [controlMatches, setControlMatches] = useState<EvidenceMatch[]>([])
  const [controlAnnotations, setControlAnnotations] = useState<Annotation[]>([])
  const [documentContent, setDocumentContent] = useState<string>('')
  const [selectedDocId, setSelectedDocId] = useState<string | null>(null)
  const [zoom, setZoom] = useState(100)
  const [showReview, setShowReview] = useState(false)
  const [reviewVerdict, setReviewVerdict] = useState('')
  const [reviewNotes, setReviewNotes] = useState('')
  const [activeHighlightId, setActiveHighlightId] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState('')

  // When a control is selected, load its result, matches, and annotations
  useEffect(() => {
    if (!selectedControlId || !results.length) return
    const result = results.find(r => r.control_id === selectedControlId)
    setSelectedResult(result || null)

    const matches = evidenceMatches.filter(m => m.control_id === selectedControlId)
    setControlMatches(matches)

    // Select first document that has matches
    if (matches.length > 0) {
      const docId = matches[0].document_id
      setSelectedDocId(docId)
      loadDocContent(docId)
    } else {
      setSelectedDocId(null)
      setDocumentContent('')
    }

    // Load annotations for highlighting
    if (currentAudit) {
      api.getAnnotationsForControl(currentAudit.audit_id, selectedControlId)
        .then(anns => setControlAnnotations(anns))
        .catch(() => setControlAnnotations([]))
    }

    setActiveHighlightId(null)
  }, [selectedControlId, results, evidenceMatches, currentAudit])

  const loadDocContent = async (docId: string) => {
    try {
      const data = await api.getEvidenceContent(docId)
      setDocumentContent(data.content || '')
    } catch {
      const doc = evidence.find(e => e.document_id === docId)
      setDocumentContent(doc?.extracted_text || 'Content not available.')
    }
  }

  const handleDocSwitch = (docId: string) => {
    setSelectedDocId(docId)
    loadDocContent(docId)
    setActiveHighlightId(null)
  }

  const handleSubmitReview = async () => {
    if (!selectedResult) return
    try {
      await api.submitReview(selectedResult.evaluation_id, {
        verdict: reviewVerdict, notes: reviewNotes, auditor_id: 'auditor@firm.com',
      })
      setShowReview(false)
      setReviewVerdict('')
      setReviewNotes('')
      if (currentAudit) loadResults(currentAudit.audit_id)
    } catch (e) { console.error(e) }
  }

  // Click evidence match → switch to its document, highlight it, scroll to it
  const handleEvidenceClick = (match: EvidenceMatch) => {
    if (match.document_id !== selectedDocId) {
      setSelectedDocId(match.document_id)
      loadDocContent(match.document_id)
    }
    // Use chunk_id as the highlight identifier
    setActiveHighlightId(match.chunk_id)
    // Scroll after a short delay to let render complete
    setTimeout(() => {
      const el = document.getElementById(`hl-${match.chunk_id}`)
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' })
      }
    }, 150)
  }

  // Click annotation from the highlights panel → scroll to it
  const handleAnnotationClick = (ann: Annotation) => {
    if (ann.document_id !== selectedDocId) {
      setSelectedDocId(ann.document_id)
      loadDocContent(ann.document_id)
    }
    setActiveHighlightId(ann.chunk_id || ann.annotation_id)
    setTimeout(() => {
      const el = document.getElementById(`hl-${ann.chunk_id || ann.annotation_id}`)
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' })
    }, 150)
  }

  // Build highlighted document content
  // Highlights come from BOTH annotations and evidence matches for the current doc
  const renderHighlightedContent = useCallback(() => {
    if (!documentContent) return null

    // Merge all highlight regions: from annotations + from evidence matches
    type HighlightRegion = {
      id: string
      start: number
      end: number
      score: number
      label: string
      controlCode: string
      controlTitle: string
    }

    const regions: HighlightRegion[] = []

    // From annotations for this document
    controlAnnotations
      .filter(a => a.document_id === selectedDocId && a.start_char != null && a.end_char != null)
      .forEach(a => {
        regions.push({
          id: a.chunk_id || a.annotation_id,
          start: a.start_char,
          end: Math.min(a.end_char, documentContent.length),
          score: a.similarity_score,
          label: a.explanation_text,
          controlCode: a.control_code,
          controlTitle: a.control_title,
        })
      })

    // If no annotations, fall back to evidence match char ranges
    if (regions.length === 0) {
      controlMatches
        .filter(m => m.document_id === selectedDocId && m.start_char != null && m.end_char != null)
        .forEach(m => {
          regions.push({
            id: m.chunk_id,
            start: m.start_char,
            end: Math.min(m.end_char, documentContent.length),
            score: m.similarity_score,
            label: `${m.control_code}: Matched evidence from ${m.original_filename}`,
            controlCode: m.control_code,
            controlTitle: m.control_title || '',
          })
        })
    }

    if (regions.length === 0) {
      return (
        <pre className="whitespace-pre-wrap text-sm text-slate-300 font-sans leading-relaxed">
          {documentContent}
        </pre>
      )
    }

    // Sort by start position, deduplicate overlapping regions
    const sorted = [...regions].sort((a, b) => a.start - b.start)
    const merged: HighlightRegion[] = []
    for (const r of sorted) {
      const last = merged[merged.length - 1]
      if (last && r.start < last.end) {
        // Overlapping — extend the previous region
        last.end = Math.max(last.end, r.end)
        last.score = Math.max(last.score, r.score)
      } else {
        merged.push({ ...r })
      }
    }

    // Build interleaved parts
    const parts: React.ReactNode[] = []
    let cursor = 0

    merged.forEach((region, i) => {
      const start = Math.max(region.start, cursor)
      const end = Math.min(region.end, documentContent.length)
      if (start < 0 || end <= start) return

      // Text before this highlight
      if (start > cursor) {
        parts.push(<span key={`t-${i}`}>{documentContent.slice(cursor, start)}</span>)
      }

      const isActive = activeHighlightId === region.id
      parts.push(
        <span
          key={`hl-${i}`}
          id={`hl-${region.id}`}
          className={`evidence-highlight ${isActive ? 'active' : ''}`}
          onClick={() => setActiveHighlightId(region.id)}
        >
          {documentContent.slice(start, end)}
          {/* Floating tooltip */}
          {isActive && (
            <span className="tooltip-content -top-28 left-0 w-80 pointer-events-none z-50">
              <div className="text-xs font-bold text-neon mb-1">
                Control: {region.controlCode}
              </div>
              {region.controlTitle && (
                <div className="text-xs text-slate-200 mb-1">{region.controlTitle}</div>
              )}
              <div className="text-xs text-slate-400 mb-1">{region.label}</div>
              <div className="text-xs text-slate-500">
                Similarity: {(region.score * 100).toFixed(1)}%
              </div>
            </span>
          )}
        </span>
      )
      cursor = end
    })

    // Remaining text
    if (cursor < documentContent.length) {
      parts.push(<span key="tail">{documentContent.slice(cursor)}</span>)
    }

    return (
      <pre className="whitespace-pre-wrap text-sm text-slate-300 font-sans leading-relaxed">
        {parts}
      </pre>
    )
  }, [documentContent, controlAnnotations, controlMatches, selectedDocId, activeHighlightId])

  // Get unique documents for this control's matches
  const matchDocs = [...new Set(controlMatches.map(m => m.document_id))]
  const currentDocMatches = controlMatches.filter(m => m.document_id === selectedDocId)

  const filteredControls = controls.filter(c =>
    !searchTerm ||
    c.control_code.toLowerCase().includes(searchTerm.toLowerCase()) ||
    c.control_title.toLowerCase().includes(searchTerm.toLowerCase())
  )

  if (!currentAudit) {
    return (
      <div className="p-6">
        <EmptyState icon={Search} title="No Audit Selected" description="Select an audit to begin investigation." />
      </div>
    )
  }

  return (
    <div className="flex h-screen overflow-hidden">
      {/* ===== LEFT PANEL — Control List ===== */}
      <div className="w-80 border-r border-surface-border flex flex-col bg-slate-900/50 flex-shrink-0">
        <div className="p-3 border-b border-surface-border">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
            <input
              className="input-field pl-9 text-xs"
              placeholder="Search controls..."
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
            />
          </div>
        </div>
        <div className="flex-1 overflow-y-auto">
          {filteredControls.map(c => {
            const result = results.find(r => r.control_id === c.control_id)
            const isSelected = selectedControlId === c.control_id
            const matchCount = evidenceMatches.filter(m => m.control_id === c.control_id).length
            return (
              <button
                key={c.control_id}
                onClick={() => selectControl(c.control_id)}
                className={`w-full text-left px-3 py-3 border-b border-surface-border/50 transition-colors ${
                  isSelected ? 'bg-brand-500/10 border-l-2 border-l-brand-400' : 'hover:bg-slate-800/50'
                }`}
              >
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs font-mono text-slate-500">{c.control_code}</span>
                  <VerdictBadge verdict={result?.ai_verdict || null} size="sm" />
                </div>
                <div className="text-sm font-medium text-slate-200 line-clamp-1">{c.control_title}</div>
                <div className="flex items-center gap-2 mt-1">
                  <RiskBadge level={c.risk_level} />
                  {result && <span className="text-xs text-slate-500">{Math.round(result.ai_confidence * 100)}%</span>}
                  {matchCount > 0 && (
                    <span className="text-xs text-slate-600">{matchCount} matches</span>
                  )}
                </div>
              </button>
            )
          })}
        </div>
      </div>

      {/* ===== CENTER PANEL — Document Viewer with Highlights ===== */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Toolbar */}
        <div className="flex items-center gap-3 px-4 py-2 border-b border-surface-border bg-slate-900/80 flex-shrink-0">
          {matchDocs.length > 0 && (
            <select
              className="input-field text-xs w-auto max-w-[250px]"
              value={selectedDocId || ''}
              onChange={e => handleDocSwitch(e.target.value)}
            >
              {matchDocs.map(docId => {
                const doc = evidence.find(e => e.document_id === docId)
                const docMatchCount = controlMatches.filter(m => m.document_id === docId).length
                return (
                  <option key={docId} value={docId}>
                    {doc?.original_filename || docId} ({docMatchCount} matches)
                  </option>
                )
              })}
            </select>
          )}
          <div className="flex-1" />
          <div className="flex items-center gap-1">
            <button onClick={() => setZoom(z => Math.max(50, z - 10))} className="p-1.5 rounded hover:bg-slate-700">
              <ZoomOut className="w-4 h-4 text-slate-400" />
            </button>
            <span className="text-xs text-slate-500 w-10 text-center">{zoom}%</span>
            <button onClick={() => setZoom(z => Math.min(200, z + 10))} className="p-1.5 rounded hover:bg-slate-700">
              <ZoomIn className="w-4 h-4 text-slate-400" />
            </button>
          </div>
          {currentDocMatches.length > 0 && (
            <span className="badge bg-neon/10 text-neon text-xs">
              <Eye className="w-3 h-3 mr-1" />
              {currentDocMatches.length} evidence regions
            </span>
          )}
        </div>

        {/* Document Content */}
        <div className="flex-1 overflow-y-auto p-6 bg-surface-dark">
          {selectedControlId ? (
            documentContent ? (
              <div className="max-w-4xl mx-auto card p-8" style={{ fontSize: `${zoom}%` }}>
                {renderHighlightedContent()}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-slate-500 text-sm">
                {controlMatches.length === 0 ? 'No evidence matches found for this control.' : 'Loading document...'}
              </div>
            )
          ) : (
            <EmptyState
              icon={Eye}
              title="Select a Control"
              description="Choose a control from the left panel to view matched evidence with highlights."
            />
          )}
        </div>
      </div>

      {/* ===== RIGHT PANEL — Analysis & Evidence ===== */}
      <div className="w-[420px] border-l border-surface-border flex flex-col bg-slate-900/50 overflow-y-auto flex-shrink-0">
        {selectedResult ? (
          <div className="p-4 space-y-4">
            {/* Control Info */}
            <div className="card p-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-mono text-brand-400">{selectedResult.control_code}</span>
                <VerdictBadge verdict={selectedResult.ai_verdict} />
              </div>
              <h3 className="text-sm font-semibold text-white mb-1">{selectedResult.control_title}</h3>
              <p className="text-xs text-slate-400 leading-relaxed">{selectedResult.control_description}</p>
              <div className="flex items-center gap-3 mt-3">
                <RiskBadge level={selectedResult.risk_level} />
                <span className="text-xs text-slate-500">{selectedResult.framework}</span>
              </div>
            </div>

            {/* AI Confidence */}
            <div className="card p-4">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">AI Confidence</h4>
              <ConfidenceBar value={selectedResult.ai_confidence} />
              <p className="text-xs text-slate-500 mt-2">Model: {selectedResult.model_used}</p>
            </div>

            {/* AI Reasoning */}
            <div className="card p-4">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
                <Sparkles className="w-3.5 h-3.5 inline mr-1 text-brand-400" />
                AI Analysis
              </h4>
              <p className="text-sm text-slate-300 leading-relaxed">{selectedResult.ai_reasoning}</p>
              {selectedResult.evidence_summary && (
                <div className="mt-3 pt-3 border-t border-surface-border">
                  <p className="text-xs text-slate-400 font-semibold mb-1">Evidence Summary</p>
                  <p className="text-xs text-slate-400">{selectedResult.evidence_summary}</p>
                </div>
              )}
            </div>

            {/* ===== MATCHED EVIDENCE — Contextual Paragraphs ===== */}
            <div className="card p-4">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
                <FileText className="w-3.5 h-3.5 inline mr-1" />
                Matched Evidence ({controlMatches.length})
              </h4>
              <div className="space-y-3">
                {controlMatches.slice(0, 8).map((m, i) => (
                  <button
                    key={m.match_id || `m-${i}`}
                    onClick={() => handleEvidenceClick(m)}
                    className={`w-full text-left p-3 rounded-lg border transition-all ${
                      activeHighlightId === m.chunk_id
                        ? 'border-neon/50 bg-neon/5 shadow-lg shadow-neon/5'
                        : 'border-surface-border hover:border-slate-500 bg-surface-dark'
                    }`}
                  >
                    {/* Header: filename + score */}
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-1.5">
                        <FileText className="w-3 h-3 text-slate-500" />
                        <span className="text-xs font-medium text-slate-300 truncate max-w-[180px]">
                          {m.original_filename}
                        </span>
                      </div>
                      <span className={`text-xs font-bold ${
                        m.similarity_score >= 0.8 ? 'text-verdict-pass' :
                        m.similarity_score >= 0.6 ? 'text-brand-400' : 'text-verdict-insufficient'
                      }`}>
                        {(m.similarity_score * 100).toFixed(0)}%
                      </span>
                    </div>

                    {/* Context paragraph — the key improvement */}
                    <div className="text-xs text-slate-400 leading-relaxed mb-2 line-clamp-4">
                      {m.context_text || m.chunk_text}
                    </div>

                    {/* Control attribution line */}
                    <div className="flex items-center gap-1.5 text-xs">
                      <ArrowDown className="w-3 h-3 text-neon" />
                      <span className="text-neon font-medium">Click to view in document</span>
                    </div>
                  </button>
                ))}
                {controlMatches.length === 0 && (
                  <p className="text-xs text-slate-500 text-center py-4">No evidence matches found.</p>
                )}
              </div>
            </div>

            {/* Highlight Navigation (from annotations) */}
            {controlAnnotations.length > 0 && (
              <div className="card p-4">
                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
                  <AlertTriangle className="w-3.5 h-3.5 inline mr-1 text-neon" />
                  Evidence Highlights ({controlAnnotations.filter(a => a.document_id === selectedDocId).length})
                </h4>
                <div className="space-y-2">
                  {controlAnnotations
                    .filter(a => a.document_id === selectedDocId)
                    .map((ann, i) => (
                    <button
                      key={ann.annotation_id}
                      onClick={() => handleAnnotationClick(ann)}
                      className={`w-full text-left px-3 py-2 rounded-lg border transition-all text-xs ${
                        activeHighlightId === (ann.chunk_id || ann.annotation_id)
                          ? 'border-neon/50 bg-neon/5'
                          : 'border-surface-border hover:border-slate-500'
                      }`}
                    >
                      <div className="flex items-center justify-between mb-1">
                        <span className="font-semibold text-neon">#{i + 1}</span>
                        <span className="text-slate-500">{(ann.similarity_score * 100).toFixed(0)}% match</span>
                      </div>
                      <p className="text-slate-400 line-clamp-2">{ann.explanation_text}</p>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Auditor Review */}
            <div className="card p-4">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">Auditor Review</h4>
              {selectedResult.auditor_verdict ? (
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <VerdictBadge verdict={selectedResult.auditor_verdict} size="sm" />
                    <span className="text-xs text-slate-500">by {selectedResult.auditor_id}</span>
                  </div>
                  {selectedResult.auditor_notes && (
                    <p className="text-xs text-slate-400">{selectedResult.auditor_notes}</p>
                  )}
                </div>
              ) : (
                <div className="flex gap-2">
                  <button onClick={() => { setReviewVerdict('PASS'); setShowReview(true) }} className="btn-success text-xs flex-1">
                    <CheckCircle className="w-3.5 h-3.5" /> Confirm
                  </button>
                  <button onClick={() => { setReviewVerdict('FAIL'); setShowReview(true) }} className="btn-danger text-xs flex-1">
                    <XCircle className="w-3.5 h-3.5" /> Override
                  </button>
                  <button onClick={() => { setReviewVerdict('FALSE_POSITIVE'); setShowReview(true) }} className="btn-secondary text-xs">
                    <MessageSquare className="w-3.5 h-3.5" />
                  </button>
                </div>
              )}
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-center h-full">
            <EmptyState icon={Search} title="No Control Selected" description="Select a control to view the analysis." />
          </div>
        )}
      </div>

      {/* Review Modal */}
      <Modal open={showReview} onClose={() => setShowReview(false)} title="Submit Auditor Review">
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Verdict</label>
            <select className="input-field" value={reviewVerdict} onChange={e => setReviewVerdict(e.target.value)}>
              <option value="PASS">PASS — Control is effective</option>
              <option value="FAIL">FAIL — Control violation confirmed</option>
              <option value="INSUFFICIENT_EVIDENCE">INSUFFICIENT — Need more evidence</option>
              <option value="FALSE_POSITIVE">FALSE POSITIVE — AI finding incorrect</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Notes</label>
            <textarea className="input-field" rows={4} placeholder="Add review comments..." value={reviewNotes} onChange={e => setReviewNotes(e.target.value)} />
          </div>
          <div className="flex gap-3 pt-2">
            <button onClick={handleSubmitReview} className="btn-primary flex-1">Submit Review</button>
            <button onClick={() => setShowReview(false)} className="btn-secondary">Cancel</button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
