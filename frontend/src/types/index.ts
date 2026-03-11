export interface Audit {
  audit_id: string
  audit_name: string
  framework: string
  client_name: string
  description: string
  status: 'CREATED' | 'IN_PROGRESS' | 'COMPLETED' | 'ARCHIVED'
  created_by: string
  created_at: string
  updated_at: string
}

export interface Control {
  control_id: string
  audit_id: string
  control_code: string
  framework: string
  control_title: string
  control_description: string
  control_category: string
  risk_level: 'HIGH' | 'MEDIUM' | 'LOW'
  frequency: string
  control_owner: string
  uploaded_by: string
  uploaded_at: string
  source_file: string
}

export interface EvidenceDocument {
  document_id: string
  audit_id: string
  original_filename: string
  file_type: string
  file_path: string
  file_size_bytes: number
  page_count: number | null
  extracted_text: string | null
  parse_status: 'PENDING' | 'PENDING_AI_PARSE' | 'COMPLETED' | 'NO_CONTENT' | 'FAILED'
  parse_error: string | null
  uploaded_by: string
  uploaded_at: string
}

export interface DocumentChunk {
  chunk_id: string
  document_id: string
  audit_id: string
  chunk_index: number
  chunk_text: string
  start_char: number
  end_char: number
  token_count: number
  page_numbers: number[]
}

export interface EvidenceMatch {
  match_id: string
  control_id: string
  control_code: string
  control_title: string
  chunk_id: string
  chunk_text: string
  chunk_index: number
  document_id: string
  original_filename: string
  similarity_score: number
  match_rank: number
  start_char: number
  end_char: number
  context_text: string
  context_start: number
  context_end: number
}

export interface Annotation {
  annotation_id: string
  control_id: string
  document_id: string
  chunk_id: string
  start_char: number
  end_char: number
  similarity_score: number
  explanation_text: string
  control_code: string
  control_title: string
  violation_type: string
  created_at: string
}

export interface EvaluationResult {
  evaluation_id: string
  control_id: string
  audit_id: string
  ai_verdict: 'PASS' | 'FAIL' | 'INSUFFICIENT_EVIDENCE'
  ai_confidence: number
  ai_reasoning: string
  evidence_summary: string
  matched_document_ids: string[]
  matched_chunk_ids: string[]
  auditor_verdict: string | null
  auditor_notes: string | null
  auditor_id: string | null
  reviewed_at: string | null
  model_used: string
  prompt_version: string
  evaluated_at: string
  control_code: string
  control_title: string
  control_description: string
  control_category: string
  risk_level: 'HIGH' | 'MEDIUM' | 'LOW'
  framework: string
}

export interface Finding {
  finding_id: string
  audit_id: string
  control_id: string
  control_code: string
  control_title: string
  severity: 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW'
  status: 'OPEN' | 'IN_REVIEW' | 'CONFIRMED' | 'REMEDIATED' | 'FALSE_POSITIVE'
  title: string
  description: string
  evidence_refs: string[]
  annotations: Annotation[]
  risk_score: number
  remediation_plan: string | null
  assigned_to: string | null
  created_at: string
  updated_at: string
}

export interface AuditLog {
  log_id: string
  audit_id: string
  user_id: string
  action: string
  entity_type: string
  entity_id: string
  details: string
  timestamp: string
}

export interface DashboardStats {
  total_controls: number
  controls_tested: number
  pass_count: number
  fail_count: number
  insufficient_count: number
  pending_count: number
  avg_confidence: number
  total_evidence: number
  evidence_processed: number
  total_findings: number
  critical_findings: number
  compliance_rate: number
}

export interface PipelineStatus {
  status: string
  run_id: number | null
  progress: number
  current_step: string
  message: string
}
