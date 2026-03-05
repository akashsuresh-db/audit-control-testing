-- ============================================================
-- Audit Control Testing Application - Infrastructure Setup
-- ============================================================

-- 1. Create Catalog
CREATE CATALOG IF NOT EXISTS audit_catalog
COMMENT 'AI-Powered Internal Control Testing Application';

USE CATALOG audit_catalog;

-- 2. Create Schema
CREATE SCHEMA IF NOT EXISTS audit_schema
COMMENT 'Schema for audit control testing tables and volumes';

USE SCHEMA audit_schema;

-- 3. Create Volumes for raw file storage
CREATE VOLUME IF NOT EXISTS controls_raw
COMMENT 'Raw control files uploaded by auditors (CSV, Excel, PDF)';

CREATE VOLUME IF NOT EXISTS evidence_raw
COMMENT 'Raw evidence documents uploaded by auditors (PDFs, images, logs)';

CREATE VOLUME IF NOT EXISTS checkpoints
COMMENT 'Auto Loader checkpoint directories';

-- 4. Create Tables

-- 4a. Controls table
CREATE TABLE IF NOT EXISTS controls (
  control_id          STRING        NOT NULL COMMENT 'Unique control identifier (UUID)',
  audit_id            STRING        NOT NULL COMMENT 'Groups controls for one audit engagement',
  control_code        STRING                 COMMENT 'Control code e.g. SOX-IT-001, SOC2-CC6.1',
  framework           STRING                 COMMENT 'Framework: SOX, SOC2, ISO27001, COSO',
  control_title       STRING                 COMMENT 'Short title of the control',
  control_description STRING                 COMMENT 'Full text of the control requirement',
  control_category    STRING                 COMMENT 'Category: Access Control, Change Mgmt, etc.',
  risk_level          STRING                 COMMENT 'HIGH, MEDIUM, LOW',
  frequency           STRING                 COMMENT 'Annual, Quarterly, Monthly, Continuous',
  control_owner       STRING                 COMMENT 'Person responsible for the control',
  embedding           ARRAY<FLOAT>           COMMENT '1024-dim embedding vector',
  uploaded_by         STRING                 COMMENT 'User who uploaded the control',
  uploaded_at         TIMESTAMP              COMMENT 'Upload timestamp',
  source_file         STRING                 COMMENT 'Original filename',
  _ingested_at        TIMESTAMP              COMMENT 'Ingestion timestamp'
)
USING DELTA
PARTITIONED BY (audit_id)
COMMENT 'Audit controls parsed from uploaded files'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- 4b. Evidence documents table
CREATE TABLE IF NOT EXISTS evidence_documents (
  document_id         STRING        NOT NULL COMMENT 'Unique document identifier (UUID)',
  audit_id            STRING        NOT NULL COMMENT 'Audit engagement ID',
  original_filename   STRING                 COMMENT 'Original uploaded filename',
  file_type           STRING                 COMMENT 'File extension: pdf, png, jpg, csv, eml, log',
  file_path           STRING                 COMMENT 'Unity Catalog Volumes path',
  file_size_bytes     BIGINT                 COMMENT 'File size in bytes',
  page_count          INT                    COMMENT 'Number of pages (for PDFs)',
  extracted_text      STRING                 COMMENT 'Full extracted text from document',
  parse_status        STRING                 COMMENT 'PENDING, PARSED, FAILED',
  parse_error         STRING                 COMMENT 'Error message if parsing failed',
  ocr_applied         BOOLEAN     DEFAULT FALSE COMMENT 'Whether OCR was used',
  uploaded_by         STRING                 COMMENT 'User who uploaded',
  uploaded_at         TIMESTAMP              COMMENT 'Upload timestamp',
  _ingested_at        TIMESTAMP              COMMENT 'Ingestion timestamp'
)
USING DELTA
PARTITIONED BY (audit_id)
COMMENT 'Evidence documents uploaded by auditors'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- 4c. Document chunks table
CREATE TABLE IF NOT EXISTS document_chunks (
  chunk_id            STRING        NOT NULL COMMENT 'Unique chunk identifier (UUID)',
  document_id         STRING        NOT NULL COMMENT 'Parent document ID',
  audit_id            STRING        NOT NULL COMMENT 'Audit engagement ID',
  chunk_index         INT                    COMMENT 'Sequential position in document',
  chunk_text          STRING                 COMMENT 'The actual chunk content',
  token_count         INT                    COMMENT 'Number of tokens in chunk',
  page_numbers        ARRAY<INT>             COMMENT 'Pages this chunk spans',
  embedding           ARRAY<FLOAT>           COMMENT '1024-dim embedding vector',
  _created_at         TIMESTAMP              COMMENT 'Creation timestamp'
)
USING DELTA
PARTITIONED BY (audit_id)
COMMENT 'Chunked text from evidence documents for embedding and retrieval'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- 4d. Control-evidence matches table
CREATE TABLE IF NOT EXISTS control_evidence_matches (
  match_id            STRING        NOT NULL COMMENT 'Unique match identifier',
  control_id          STRING        NOT NULL COMMENT 'Matched control ID',
  chunk_id            STRING        NOT NULL COMMENT 'Matched chunk ID',
  document_id         STRING        NOT NULL COMMENT 'Source document ID',
  audit_id            STRING        NOT NULL COMMENT 'Audit engagement ID',
  similarity_score    DOUBLE                 COMMENT 'Cosine similarity score',
  match_rank          INT                    COMMENT 'Rank (1 = best match)',
  _matched_at         TIMESTAMP              COMMENT 'Match timestamp'
)
USING DELTA
PARTITIONED BY (audit_id)
COMMENT 'Vector similarity matches between controls and evidence chunks';

-- 4e. Evaluation results table
CREATE TABLE IF NOT EXISTS evaluation_results (
  evaluation_id       STRING        NOT NULL COMMENT 'Unique evaluation identifier',
  control_id          STRING        NOT NULL COMMENT 'Control being evaluated',
  audit_id            STRING        NOT NULL COMMENT 'Audit engagement ID',
  ai_verdict          STRING                 COMMENT 'PASS, FAIL, INSUFFICIENT_EVIDENCE',
  ai_confidence       DOUBLE                 COMMENT 'Confidence score 0.0 to 1.0',
  ai_reasoning        STRING                 COMMENT 'LLM-generated explanation',
  evidence_summary    STRING                 COMMENT 'Summary of matched evidence',
  matched_document_ids ARRAY<STRING>          COMMENT 'Document IDs used in evaluation',
  matched_chunk_ids   ARRAY<STRING>           COMMENT 'Chunk IDs used in evaluation',
  auditor_verdict     STRING                 COMMENT 'Final human decision',
  auditor_notes       STRING                 COMMENT 'Auditor comments',
  auditor_id          STRING                 COMMENT 'Reviewing auditor',
  reviewed_at         TIMESTAMP              COMMENT 'Review timestamp',
  model_used          STRING                 COMMENT 'LLM model identifier',
  prompt_version      STRING                 COMMENT 'Prompt version used',
  evaluated_at        TIMESTAMP              COMMENT 'Evaluation timestamp',
  _created_at         TIMESTAMP              COMMENT 'Record creation timestamp'
)
USING DELTA
PARTITIONED BY (audit_id)
COMMENT 'AI evaluation results with auditor review fields'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.logRetentionDuration' = 'interval 365 days'
);

-- 4f. Audit log table
CREATE TABLE IF NOT EXISTS audit_log (
  log_id              STRING        NOT NULL COMMENT 'Unique log entry identifier',
  audit_id            STRING                 COMMENT 'Related audit engagement',
  user_id             STRING                 COMMENT 'User who performed the action',
  action              STRING                 COMMENT 'UPLOAD, PARSE, EVALUATE, REVIEW, OVERRIDE',
  entity_type         STRING                 COMMENT 'CONTROL, EVIDENCE, EVALUATION',
  entity_id           STRING                 COMMENT 'ID of the affected entity',
  details             STRING                 COMMENT 'JSON blob with action-specific details',
  timestamp           TIMESTAMP              COMMENT 'When the action occurred'
)
USING DELTA
PARTITIONED BY (audit_id)
COMMENT 'Complete audit trail for compliance and traceability'
TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 365 days');

-- 4g. Framework mappings table
CREATE TABLE IF NOT EXISTS framework_mappings (
  mapping_id          STRING        NOT NULL COMMENT 'Unique mapping identifier',
  source_framework    STRING                 COMMENT 'Source framework e.g. SOX',
  source_control      STRING                 COMMENT 'Source control code',
  target_framework    STRING                 COMMENT 'Target framework e.g. SOC2',
  target_control      STRING                 COMMENT 'Target control code',
  mapping_type        STRING                 COMMENT 'EXACT, PARTIAL, RELATED',
  similarity_score    DOUBLE                 COMMENT 'Embedding similarity score',
  _created_at         TIMESTAMP              COMMENT 'Creation timestamp'
)
USING DELTA
COMMENT 'Cross-framework control mappings';

-- 4h. Audit engagements table
CREATE TABLE IF NOT EXISTS audit_engagements (
  audit_id            STRING        NOT NULL COMMENT 'Unique audit engagement ID',
  audit_name          STRING                 COMMENT 'Name of the audit engagement',
  framework           STRING                 COMMENT 'Primary framework: SOX, SOC2, ISO27001',
  client_name         STRING                 COMMENT 'Organization being audited',
  description         STRING                 COMMENT 'Engagement description',
  status              STRING                 COMMENT 'CREATED, IN_PROGRESS, COMPLETED, ARCHIVED',
  created_by          STRING                 COMMENT 'Auditor who created the engagement',
  created_at          TIMESTAMP              COMMENT 'Creation timestamp',
  updated_at          TIMESTAMP              COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Audit engagement metadata';
