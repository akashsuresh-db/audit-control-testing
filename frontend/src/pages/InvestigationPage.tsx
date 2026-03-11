import { useState, useEffect, useRef, useCallback } from 'react'
import { useStore } from '../store/useStore'
import VerdictBadge from '../components/shared/VerdictBadge'
import RiskBadge from '../components/shared/RiskBadge'
import ConfidenceBar from '../components/shared/ConfidenceBar'
import EmptyState from '../components/shared/EmptyState'
import Modal from '../components/shared/Modal'
import {
  Search, Eye, ChevronDown, ChevronRight, CheckCircle, XCircle,
  MessageSquare, AlertTriangle, FileText, Sparkles, ArrowRight,
  ZoomIn, ZoomOut, ChevronUp,
} from 'lucide-react'
import * as api from '../api/client'
import type { EvaluationResult, EvidenceMatch, Annotation } from '../types'

export default function InvestigationPage() {
  const {
    currentAudit, controls, results, evidenceMatches, evidence, annotations,
    selectedControlId, selectControl, loadAnnotations, loadResults,
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
  const [activeHighlight, setActiveHighlight] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState('')
  const contentRef = useRef<HTMLDivElement>(null)

  // When a control is selected, load its result and matches
  useEffect(() => {
    if (!selectedControlId || !results.length) return
    const result = results.find(r => r.control_id === selectedControlId)
    setSelectedResult(result || null)
    const matches = evidenceMatches.filter(m => m.control_id === selectedControlId)
    setControlMatches(matches)
    if (matches.length > 0) {
      const docId = matches[0].document_id
      setSelectedDocId(docId)
      loadDocContent(docId)
    }
    if (currentAudit) {
      api.getAnnotationsForControl(currentAudit.audit_id, selectedControlId).then(setControlAnnotations).catch(() => {})
    }
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

  const handleSubmitReview = async () => {
    if (!selectedResult) return
    try {
      await api.submitReview(selectedResult.evaluation_id, {
        verdict: reviewVerdict,
        notes: reviewNotes,
        auditor_id: 'auditor@firm.com',
      })
      setShowReview(false)
      setReviewVerdict('')
      setReviewNotes('')
      if (currentAudit) loadResults(currentAudit.audit_id)
    } catch (e) { console.error(e) }
  }

  // Render document content with highlights
  const renderHighlightedContent = useCallback(() => {
    if (!documentContent) return null
    const docAnnotations = controlAnnotations.filter(a => a.document_id === selectedDocId)
    if (docAnnotations.length === 0) {
      return <pre className="whitespace-pre-wrap text-sm text-slate-300 font-sans leading-relaxed">{documentContent}</pre>
    }

    // Sort annotations by start position
    const sorted = [...docAnnotations].sort((a, b) => a.start_char - b.start_char)
    const parts: React.ReactNode[] = []
    let lastEnd = 0

    sorted.forEach((ann, i) => {
      const start = Math.max(ann.start_char, lastEnd)
      const end = Math.min(ann.end_char, documentContent.length)
      if (start > lastEnd) {
        parts.push(<span key={`text-${i}`}>{documentContent.slice(lastEnd, start)}</span>)
      }
      parts.push(
        <span
          key={`hl-${i}`}
          id={`annotation-${ann.annotation_id}`}
          className={`evidence-highlight ${activeHighlight === ann.annotation_id ? 'active' : ''}`}
          onClick={() => setActiveHighlight(ann.annotation_id)}
          title={`${ann.control_code}: ${ann.explanation_text} (Score: ${(ann.similarity_score * 100).toFixed(0)}%)`}
        >
          {documentContent.slice(start, end)}
          {/* Tooltip */}
          {activeHighlight === ann.annotation_id && (
            <span className="tooltip-content -top-24 left-0 w-80 pointer-events-none">
              <div className="text-xs font-semibold text-neon mb-1">
                Control: {ann.control_code} — {ann.control_title}
              </div>
              <div className="text-xs text-slate-300 mb-1">{ann.explanation_text}</div>
              <div className="text-xs text-slate-500">
                Similarity Score: {(ann.similarity_score * 100).toFixed(1)}%
              </div>
            </span>
          )}
        </span>
      )
      lastEnd = end
    })

    if (lastEnd < documentContent.length) {
      parts.push(<span key="text-end">{documentContent.slice(lastEnd)}</span>)
    }

    return <pre className="whitespace-pre-wrap text-sm text-slate-300 font-sans leading-relaxed">{parts}</pre>
  }, [documentContent, controlAnnotations, selectedDocId, activeHighlight])

  const scrollToAnnotation = (annotationId: string) => {
    setActiveHighlight(annotationId)
    const el = document.getElementById(`annotation-${annotationId}`)
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'center' })
    }
  }

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
      {/* LEFT PANEL — Control List */}
      <div className="w-80 border-r border-surface-border flex flex-col bg-slate-900/50">
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
                  {result && (
                    <span className="text-xs text-slate-500">{Math.round(result.ai_confidence * 100)}% confidence</span>
                  )}
                </div>
              </button>
            )
          })}
        </div>
      </div>

      {/* CENTER PANEL — Evidence Viewer */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Document toolbar */}
        <div className="flex items-center gap-3 px-4 py-2 border-b border-surface-border bg-slate-900/80">
          <div className="flex items-center gap-1">
            {controlMatches.length > 0 && (
              <select
                className="input-field text-xs w-auto"
                value={selectedDocId || ''}
                onChange={e => { setSelectedDocId(e.target.value); loadDocContent(e.target.value) }}
              >
                {[...new Set(controlMatches.map(m => m.document_id))].map(docId => {
                  const doc = evidence.find(e => e.document_id === docId)
                  return <option key={docId} value={docId}>{doc?.original_filename || docId}</option>
                })}
              </select>
            )}
          </div>
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
          {controlAnnotations.length > 0 && (
            <span className="badge bg-neon/10 text-neon text-xs">
              <Eye className="w-3 h-3 mr-1" />
              {controlAnnotations.filter(a => a.document_id === selectedDocId).length} highlights
            </span>
          )}
        </div>

        {/* Document content with highlights */}
        <div ref={contentRef} className="flex-1 overflow-y-auto p-6 bg-surface-dark">
          {selectedControlId ? (
            documentContent ? (
              <div className="max-w-4xl mx-auto card p-8" style={{ fontSize: `${zoom}%` }}>
                {renderHighlightedContent()}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-slate-500 text-sm">
                Loading document content...
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

      {/* RIGHT PANEL — Analysis */}
      <div className="w-96 border-l border-surface-border flex flex-col bg-slate-900/50 overflow-y-auto">
        {selectedResult ? (
          <div className="p-4 space-y-4">
            {/* Control Info */}
            <div className="card p-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-mono text-brand-400">{selectedResult.control_code}</span>
                <VerdictBadge verdict={selectedResult.ai_verdict} />
              </div>
              <h3 className="text-sm font-semibold text-white mb-1">{selectedResult.control_title}</h3>
              <p className="text-xs text-slate-400">{selectedResult.control_description}</p>
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

            {/* Evidence Highlights Navigation */}
            {controlAnnotations.length > 0 && (
              <div className="card p-4">
                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
                  <AlertTriangle className="w-3.5 h-3.5 inline mr-1 text-neon" />
                  Evidence Highlights
                </h4>
                <div className="space-y-2">
                  {controlAnnotations.map((ann, i) => (
                    <button
                      key={ann.annotation_id}
                      onClick={() => scrollToAnnotation(ann.annotation_id)}
                      className={`w-full text-left px-3 py-2 rounded-lg border transition-all text-xs ${
                        activeHighlight === ann.annotation_id
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

            {/* Matched Evidence Chunks */}
            <div className="card p-4">
              <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">
                <FileText className="w-3.5 h-3.5 inline mr-1" />
                Matched Evidence ({controlMatches.length})
              </h4>
              <div className="space-y-2">
                {controlMatches.slice(0, 5).map(m => (
                  <div key={m.match_id} className="p-2 rounded-lg bg-surface-dark text-xs">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-slate-400">#{m.match_rank} — {m.original_filename}</span>
                      <span className="text-brand-400">{(m.similarity_score * 100).toFixed(0)}%</span>
                    </div>
                    <p className="text-slate-500 line-clamp-3">{m.chunk_text}</p>
                  </div>
                ))}
              </div>
            </div>

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
                  <button
                    onClick={() => { setReviewVerdict('PASS'); setShowReview(true) }}
                    className="btn-success text-xs flex-1"
                  >
                    <CheckCircle className="w-3.5 h-3.5" /> Confirm
                  </button>
                  <button
                    onClick={() => { setReviewVerdict('FAIL'); setShowReview(true) }}
                    className="btn-danger text-xs flex-1"
                  >
                    <XCircle className="w-3.5 h-3.5" /> Override
                  </button>
                  <button
                    onClick={() => { setReviewVerdict('FALSE_POSITIVE'); setShowReview(true) }}
                    className="btn-secondary text-xs"
                  >
                    <MessageSquare className="w-3.5 h-3.5" />
                  </button>
                </div>
              )}
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-center h-full">
            <EmptyState
              icon={Search}
              title="No Control Selected"
              description="Select a control to view the analysis."
            />
          </div>
        )}
      </div>

      {/* Review Modal */}
      <Modal open={showReview} onClose={() => setShowReview(false)} title="Submit Auditor Review">
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Verdict</label>
            <select
              className="input-field"
              value={reviewVerdict}
              onChange={e => setReviewVerdict(e.target.value)}
            >
              <option value="PASS">PASS — Control is effective</option>
              <option value="FAIL">FAIL — Control violation confirmed</option>
              <option value="INSUFFICIENT_EVIDENCE">INSUFFICIENT — Need more evidence</option>
              <option value="FALSE_POSITIVE">FALSE POSITIVE — AI finding incorrect</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1.5">Notes</label>
            <textarea
              className="input-field"
              rows={4}
              placeholder="Add review comments..."
              value={reviewNotes}
              onChange={e => setReviewNotes(e.target.value)}
            />
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
