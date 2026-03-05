# AI-Powered Internal Control Testing Application

![Built on Databricks](https://img.shields.io/badge/Built%20on-Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Status: Active](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)
![Python 3.11+](https://img.shields.io/badge/Python-3.11+-blue?style=flat-square&logo=python&logoColor=white)

An enterprise-grade application that automates audit control testing using AI. Built entirely on the Databricks platform, it leverages `ai_parse_document` for intelligent document extraction, Foundation Model APIs for embeddings and evaluation, Vector Search for semantic matching, and Databricks Apps for a self-contained auditor interface — transforming weeks of manual evidence review into minutes of AI-assisted evaluation with full explainability.

---

## Table of Contents

- [Business Problem](#business-problem)
- [Solution Overview](#solution-overview)
- [Technical Architecture](#technical-architecture)
- [Process Flow](#process-flow)
- [Data Model](#data-model)
- [Pipeline Deep Dive](#pipeline-deep-dive)
- [AI Evaluation & Explainability](#ai-evaluation--explainability)
- [Frontend Experience](#frontend-experience)
- [Impact and ROI](#impact-and-roi)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)

---

## Business Problem

### What Are We Solving?

Internal audit teams are overwhelmed. Every quarter, auditors must verify that hundreds of internal controls — access policies, change management procedures, financial close checklists, vulnerability remediation — are operating effectively. This requires matching each control against stacks of evidence documents (PDFs, screenshots, policy documents, scan reports) and making a professional judgment call.

This process is fundamentally broken at scale:

| Pain Point | Impact |
|---|---|
| **Manual Document Review** | Auditors spend 60-80% of their time reading documents and matching evidence to controls — repetitive work that doesn't leverage their expertise |
| **Inconsistent Judgments** | Two auditors reviewing the same evidence may reach different conclusions, creating quality and defensibility risks |
| **Bottlenecked Audit Cycles** | Audit periods routinely extend past deadlines because document review cannot be parallelized effectively |
| **Scaling Crisis** | Every new regulation (SOX, SOC2, ISO 27001, PCI-DSS) adds controls without adding capacity. Headcount scales linearly; compliance requirements scale exponentially |
| **No Evidence Traceability** | Manual reviews rarely capture which specific passages in which documents led to a verdict — making it difficult to defend findings to regulators |

### The Core Challenge

An auditor testing a single control like *"Multi-factor authentication must be enforced for all remote access to financial systems"* must:

1. Search through dozens of uploaded documents for relevant evidence
2. Read and parse PDFs, images, and policy documents
3. Identify which passages are relevant to this specific control
4. Synthesize findings across multiple sources
5. Make a judgment: PASS, FAIL, or insufficient evidence
6. Document their reasoning for the audit trail

Multiply this by hundreds of controls per engagement, and it becomes clear why audit teams are perpetually behind.

---

## Solution Overview

### How Does the Solution Solve the Problem?

This application replaces the manual document-to-control matching and evaluation workflow with an AI-powered pipeline that:

1. **Intelligently parses documents** — Uses Databricks `ai_parse_document` to extract structured text from PDFs, images, Word documents, and scanned files. No more manual copy-pasting from PDFs.

2. **Understands meaning, not just keywords** — Embeds both controls and evidence into a shared 1024-dimensional vector space using `databricks-bge-large-en`. A control about "periodic access reviews" will match evidence about "quarterly user entitlement audits" even without shared keywords.

3. **Matches evidence to controls automatically** — Uses Databricks Vector Search to find the most semantically relevant evidence chunks for each control, ranked by similarity score.

4. **Evaluates with chain-of-thought reasoning** — A 70B-parameter LLM (`databricks-meta-llama-3-3-70b-instruct`) evaluates each control against its matched evidence using a structured, step-by-step prompt that mirrors how a senior auditor thinks.

5. **Shows the complete picture** — The auditor UI displays the control, the AI verdict with confidence and reasoning, AND the specific evidence passages that were retrieved — with similarity scores and source filenames — so the auditor can verify the AI's work.

6. **Maintains a full audit trail** — Every action (upload, parse, evaluate, review, override) is logged with timestamps and user IDs for regulatory compliance.

### Key Principle: Augment, Don't Replace

The AI does not make final decisions. It presents structured recommendations with citations. The auditor reviews, accepts, or overrides each verdict with their professional judgment. The system handles the high-volume document review; the auditor handles the risk assessment.

---

## Technical Architecture

### Component Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Frontend** | Static HTML/CSS/JS SPA | Single-page auditor interface with Tailwind CSS styling |
| **Backend** | FastAPI on Databricks Apps | REST API with parameterized SQL queries |
| **File Storage** | Unity Catalog Volumes | Raw evidence files stored in `/Volumes/main/audit_schema/evidence_raw/` |
| **Document Parsing** | `ai_parse_document` via SQL Statement API | Structured text extraction from PDFs, images, DOCX, PPTX |
| **Embeddings** | Foundation Model API (`databricks-bge-large-en`) | 1024-dimensional dense vector embeddings via `ai_query()` |
| **Vector Search** | Databricks Vector Search (Delta Sync Index) | Cosine similarity matching between control and evidence embeddings |
| **LLM Evaluation** | Foundation Model API (`databricks-meta-llama-3-3-70b-instruct`) | Chain-of-thought evaluation with structured JSON output via `ai_query()` |
| **Pipeline** | Databricks Serverless Notebook Job | Single-notebook pipeline with 6 steps |
| **Data Store** | Delta Lake (Unity Catalog) | 8 tables in `main.audit_schema` with change data feed |
| **Auth** | Databricks Apps OAuth / Service Principal | Workspace-integrated authentication |

### Architecture Diagram

```
                    +-------------------------------------+
                    |     Auditor Browser Interface        |
                    |  (Static HTML SPA + Tailwind CSS)    |
                    +-----------------+-------------------+
                                      |
                                      | REST API (JSON)
                                      v
                    +-------------------------------------+
                    |      FastAPI Backend (Databricks App)|
                    |  Parameterized SQL | File Uploads    |
                    +---+--------+--------+--------+------+
                        |        |        |        |
           +------------+   +----+   +----+   +----+-----------+
           |                |        |                          |
           v                v        v                          v
  +------------------+ +----+--------+-----+     +-------------+---------+
  | UC Volumes       | | Databricks SQL    |     | Serverless Notebook   |
  | /evidence_raw/   | | Warehouse        |     | Job (Pipeline)        |
  | /controls_raw/   | | (1b1d59e180e4ac26)|     | (Job: 675224616522707)|
  +------------------+ +-------+----------+     +---+---+---+---+------+
                               |                     |   |   |   |
                               |          +----------+   |   |   +--------+
                               |          |              |   |            |
                               v          v              v   v            v
                  +---------------------+ +-----------+ +--------+ +----------+
                  | ai_parse_document   | | ai_query  | | Vector | | ai_query |
                  | (via SQL Statement  | | BGE-Large | | Search | | Llama 3.3|
                  |  REST API)          | | Embeddings| | REST   | | 70B Eval |
                  +----------+----------+ +-----+-----+ | API    | +----+-----+
                             |                  |        +---+----+      |
                             v                  v            |           v
                  +----------------------------------------------------------+
                  |            Unity Catalog - Delta Lake                     |
                  |                main.audit_schema                          |
                  |                                                          |
                  |  audit_engagements | controls         | document_chunks  |
                  |  evidence_documents| control_evidence_matches             |
                  |  evaluation_results| audit_log        | framework_mappings|
                  +------------------------------+---------------------------+
                                                 |
                                                 v
                              +----------------------------------+
                              | Databricks Vector Search         |
                              | Delta Sync Index                 |
                              | main.audit_schema.evidence_chunk |
                              | _index                           |
                              +----------------------------------+
```

### Key Architectural Decisions

**Why SQL Statement API for `ai_parse_document`?**
`ai_parse_document` is a SQL-native function that runs on the SQL Warehouse, not the serverless notebook Spark runtime. The pipeline notebook calls it via the `/api/2.0/sql/statements` REST API, targeting the SQL Warehouse by ID. This is the only way to use `ai_parse_document` from a serverless job.

**Why `ai_query()` for embeddings and LLM?**
`ai_query()` is a built-in SQL function that routes through the Foundation Model API serving endpoints. Using it inside `MERGE INTO` and `SELECT` statements allows embedding generation and LLM evaluation to happen directly in Spark SQL — no Python SDK calls needed for these steps.

**Why Vector Search REST API?**
The Databricks Vector Search Python SDK is not pre-installed on serverless notebook runtimes. The REST API (`/api/2.0/vector-search/indexes/{index}/query`) provides the same functionality with OAuth authentication from the `WorkspaceClient`.

**Why Static HTML SPA instead of React?**
A single `index.html` file with inline Tailwind CSS eliminates build tooling, node_modules, and deployment complexity. The entire frontend ships as one file in the FastAPI static directory.

---

## Process Flow

### End-to-End Workflow

```
  AUDITOR                        APPLICATION                         DATABRICKS SERVICES
  -------                        -----------                         -------------------

  1. Create Audit   ---------->  POST /api/audits
     (name, framework,           Insert into audit_engagements
      client)                                                        Delta Lake

  2. Upload Controls ----------> POST /api/audits/{id}/controls
     (sox_controls.csv)          Parse CSV, insert rows              controls table
                                 into controls table

  3. Upload Evidence ----------> POST /api/audits/{id}/evidence
     (PDFs, images,              Store files to UC Volume  --------> /Volumes/.../evidence_raw/
      text files)                Register in evidence_documents       evidence_documents table
                                 Mark PDFs as PENDING_AI_PARSE

  4. Run Pipeline   ---------->  POST /api/audits/{id}/evaluate
                                 Trigger Job 675224616522707 ------> Serverless Notebook Job
                                                                         |
                                                                         v
                                 Step 1: ai_parse_document  -------> SQL Warehouse
                                   Extract text from PDFs/images      (SQL Statement API)
                                                                         |
                                 Step 2: Chunk documents                 |
                                   Paragraph-aware splitting             |
                                   (1500 chars, 200 overlap)            |
                                                                         |
                                 Step 3: Generate embeddings  -------> ai_query(BGE-Large-EN)
                                   1024-dim vectors for                  Foundation Model API
                                   controls + chunks                     |
                                                                         |
                                 Step 4: Vector Search matching -----> Vector Search REST API
                                   Top-15 chunks per control             Cosine similarity
                                   Sync index, query each control        |
                                                                         |
                                 Step 5: LLM evaluation  -----------> ai_query(Llama 3.3 70B)
                                   Chain-of-thought prompt               Foundation Model API
                                   Structured JSON output                |
                                                                         |
                                 Step 6: Save results                    |
                                   Write to evaluation_results           Delta Lake

  5. Review Results  ----------> GET /api/audits/{id}/results
     View AI verdicts,           GET /api/audits/{id}/evidence-matches
     evidence passages,          Display control + evidence + verdict
     similarity scores           with expandable evidence sections

  6. Accept/Override ----------> PUT /api/results/{id}/review
     Final audit judgment        Update auditor_verdict               evaluation_results
                                 Log action                           audit_log
```

### Document Parsing Flow (Detail)

```
  PDF/DOCX/Image uploaded
         |
         v
  Stored in UC Volume: /Volumes/main/audit_schema/evidence_raw/{audit_id}/{doc_id}.pdf
         |
         v
  Registered in evidence_documents with parse_status = 'PENDING_AI_PARSE'
         |
         v  (Pipeline Step 1)
  SQL Statement API call to SQL Warehouse:
    SELECT ai_parse_document(content, map('contentType', 'application/pdf')) AS parsed
    FROM read_files('/Volumes/.../doc_id.pdf', format => 'BINARYFILE')
         |
         v
  Parse JSON response -> extract document.elements[].content
         |
         v
  UPDATE evidence_documents SET extracted_text = '...', parse_status = 'COMPLETED'
         |
         v  (Pipeline Step 2)
  paragraph_aware_chunk(text, max_size=1500, overlap=200)
    - Split by paragraph boundaries (\n\n, numbered lists, bullet points)
    - Combine paragraphs into chunks up to 1500 chars
    - Carry 200 chars overlap between chunks
    - Fallback to fixed-size if single large paragraph
         |
         v
  INSERT INTO document_chunks (chunk_id, document_id, audit_id, chunk_index, chunk_text)
```

---

## Data Model

All tables reside in **`main.audit_schema`** using Delta Lake format.

### Tables

| Table | Description | Key Columns |
|---|---|---|
| `audit_engagements` | Audit metadata | `audit_id`, `audit_name`, `framework`, `client_name`, `status` |
| `controls` | Control definitions with embeddings | `control_id`, `control_code`, `control_description`, `risk_level`, `embedding ARRAY<FLOAT>` |
| `evidence_documents` | Uploaded evidence files | `document_id`, `file_path`, `extracted_text`, `parse_status`, `file_type` |
| `document_chunks` | Chunked text with embeddings | `chunk_id`, `document_id`, `chunk_text`, `embedding ARRAY<FLOAT>` |
| `control_evidence_matches` | Vector similarity results | `control_id`, `chunk_id`, `similarity_score`, `match_rank` |
| `evaluation_results` | AI verdicts + auditor reviews | `ai_verdict`, `ai_confidence`, `ai_reasoning`, `auditor_verdict`, `auditor_notes` |
| `audit_log` | Complete audit trail | `user_id`, `action`, `entity_type`, `entity_id`, `details`, `timestamp` |
| `framework_mappings` | Cross-framework control mappings | `source_control_id`, `target_control_id`, `similarity_score` |

### Storage Volumes

| Volume | Purpose |
|---|---|
| `evidence_raw` | Raw uploaded files (PDFs, images, text) organized by `audit_id` |
| `controls_raw` | Raw control definition files |
| `checkpoints` | Auto Loader checkpoints for incremental ingestion |

### Key Properties

- **Change Data Feed** enabled on `controls`, `evidence_documents`, `document_chunks`, `evaluation_results` — powers Delta Sync vector index
- **Embedding columns** are `ARRAY<FLOAT>` with 1024 dimensions
- All tables partitioned by `audit_id` for multi-engagement isolation
- `evaluation_results` and `audit_log` retain logs for 365 days

---

## Pipeline Deep Dive

The pipeline runs as a single Databricks serverless notebook (`notebooks/00_run_full_pipeline.py`) triggered as Job `675224616522707`.

### Step 1: AI Parse Documents

Extracts text from PDFs, images, and Office documents using `ai_parse_document`.

- **Technology:** SQL Statement REST API (`/api/2.0/sql/statements`) targeting the SQL Warehouse
- **Why not `spark.sql()`?** — `ai_parse_document` is only available on SQL Warehouse, not the serverless notebook Spark runtime
- **Auth:** `WorkspaceClient().config.authenticate()` — handles both dict and callable return types for cross-environment compatibility
- **Supported formats:** PDF, PNG, JPG, JPEG, DOC, DOCX, PPT, PPTX
- **File reading:** `read_files(path, format => 'BINARYFILE')` reads files directly from UC Volumes

### Step 2: Chunk Documents

Splits extracted text into semantically meaningful chunks.

- **Strategy:** Paragraph-aware chunking (not fixed-token)
- **Max chunk size:** 1500 characters
- **Overlap:** 200 characters
- **Paragraph detection:** Splits on `\n\n`, numbered lists (`1.`, `A)`), and bullet points
- **Fallback:** Fixed-size splitting if text has no paragraph boundaries
- **Schema enforcement:** Explicit `StructType` with `IntegerType()` for `chunk_index` (avoids Delta schema merge errors with Python bigint)

### Step 3: Generate Embeddings

Creates vector representations for semantic matching.

- **Model:** `databricks-bge-large-en` via `ai_query()` SQL function
- **Dimensions:** 1024
- **Applied to:** Both `controls.control_description` and `document_chunks.chunk_text`
- **Method:** `MERGE INTO` with subquery — only embeds rows where `embedding IS NULL`

### Step 4: Vector Search Matching

Finds the most semantically relevant evidence for each control.

- **Index:** `main.audit_schema.evidence_chunk_index` (Delta Sync)
- **Endpoint:** `mas-ea793b7b-endpoint`
- **Pre-query:** Triggers index sync and waits for ready state
- **Top-K:** 15 results per control
- **Filters:** `audit_id` filter ensures cross-engagement isolation
- **Threshold:** Only matches with `similarity_score >= 0.4` and `match_rank <= 8` are used for evaluation
- **Auth:** Vector Search REST API with OAuth headers from `WorkspaceClient`

### Step 5: LLM Evaluation (Chain-of-Thought)

Evaluates each control against its matched evidence using a structured prompt.

- **Model:** `databricks-meta-llama-3-3-70b-instruct` via `ai_query()` SQL function
- **Prompt version:** `v4.0-cot`
- **Prompt structure:**
  1. Role: Senior internal auditor with SOX/PCI-DSS/ISO 27001 expertise
  2. Instruction: Use ONLY the provided evidence — no assumptions
  3. Control details: Code, description, risk level
  4. Evidence: Concatenated chunks with source filenames and similarity scores
  5. Step-by-step evaluation instructions (identify criteria, check evidence, note gaps, determine verdict)
  6. Verdict criteria: PASS / FAIL / INSUFFICIENT_EVIDENCE with clear definitions
  7. Output format: Structured JSON with verdict, confidence, reasoning, key_findings, gaps_identified, evidence_summary

### Step 6: Save Results

Parses LLM JSON output and writes to `evaluation_results`.

- **JSON parsing:** `from_json()` with explicit schema
- **Fallback:** If JSON parse fails, defaults to `INSUFFICIENT_EVIDENCE` with raw LLM response as reasoning
- **Cleanup:** Deletes previous results for the same `audit_id` before inserting (idempotent reruns)

---

## AI Evaluation & Explainability

### Verdicts

| Verdict | Meaning |
|---|---|
| **`PASS`** | Evidence directly demonstrates ALL key aspects of the control are operating effectively |
| **`FAIL`** | Evidence shows non-compliance or the control was not executed as designed |
| **`INSUFFICIENT_EVIDENCE`** | Evidence is missing, partial, or does not clearly address the control requirements |

### Anti-Hallucination Safeguards

- **Grounded generation:** The prompt explicitly instructs: *"Use ONLY the evidence provided — do not assume or infer anything not explicitly stated"*
- **Evidence attribution:** Each evidence chunk in the prompt includes its source filename and similarity score, so the LLM cites specific sources
- **Structured output:** JSON schema constrains the response — no free-form text that could contain unsupported claims
- **Chain-of-thought:** The 4-step evaluation process (identify criteria → check evidence → note gaps → determine verdict) forces systematic reasoning rather than snap judgments
- **Traceability:** Every verdict is linked to specific `matched_document_ids` and `matched_chunk_ids` — an auditor can trace any decision back to exact evidence passages

### Evidence Display in UI

Each result card in the auditor interface shows:

1. **Control details** — code, title, description, risk level, category
2. **AI verdict** — color-coded badge with confidence percentage
3. **AI reasoning** — the LLM's step-by-step evaluation narrative
4. **Retrieved Evidence** — expandable section showing each matched evidence chunk with:
   - Rank badge (1st, 2nd, 3rd...)
   - Source filename
   - Similarity score as a percentage
   - Full chunk text (expandable with "Show more")

This gives the auditor the complete picture: what the AI decided, why, and exactly what evidence it based its decision on.

---

## Frontend Experience

The frontend is a single-page application (`backend/app/static/index.html`) built with vanilla JavaScript and Tailwind CSS.

### Screens

1. **Dashboard** — List of audit engagements with status indicators
2. **Audit Detail** — Tabs for Controls, Evidence, and Results
3. **Controls Tab** — Upload CSV, view imported controls with code/title/risk/category
4. **Evidence Tab** — Drag-and-drop multi-file upload, parse status tracking (PENDING_AI_PARSE → COMPLETED)
5. **Results Tab** — Summary cards (PASS/FAIL/INSUFFICIENT counts with avg confidence), individual result cards with expandable evidence sections, accept/override workflow

### Key UI Features

- **Real-time pipeline status** polling after triggering evaluation
- **Evidence-aware result cards** with similarity scores and source attribution
- **Auditor review workflow** — accept AI verdict or override with notes
- **Export capability** for generating audit reports

---

## Impact and ROI

| Metric | Before (Manual) | After (AI-Assisted) |
|---|---|---|
| Document review time | 60-80% of audit cycle | Automated — seconds per control |
| Evidence-to-control matching | Manual keyword search | Semantic vector search |
| Evaluation consistency | Variable across auditors | Standardized AI baseline |
| Evidence traceability | Partial, undocumented | 100% — every chunk cited with scores |
| Audit cycle duration | Weeks | Hours to days |
| Document formats supported | Manual reading only | PDF, DOCX, PPTX, images (OCR), text |
| Scale | Hundreds of documents | Thousands+ with same latency |

### Business Value & Cost Savings

| Metric | Estimate |
|---|---|
| **Manual effort reduction** | A typical SOX audit with 200+ controls requires 3-4 senior auditors spending 4-6 weeks on evidence review. This solution compresses that to hours of pipeline execution + 1-2 days of review on flagged items — a **70-80% reduction in audit cycle time** |
| **Labor cost savings** | Senior IT auditors bill at $150-250/hr. At ~120 hours per engagement on document review, that's $72K-$120K per audit in review labor. Automating the bulk saves **$50K-$90K per engagement** |
| **Consistency & rework avoidance** | Manual reviews have ~15-20% inter-auditor disagreement on borderline controls, leading to rework and resampling. A standardized AI baseline reduces **rework costs by 30-40%** |
| **Expertise democratization** | Chain-of-thought reasoning guides junior auditors through evaluation logic — firms can **staff engagements with fewer senior resources** while maintaining quality |
| **Scale without headcount** | The same audit team can handle **2-3x the engagement volume** since the bottleneck (document review) is automated — growth without proportional hiring |
| **Regulatory defensibility** | Every verdict traces to specific evidence passages with similarity scores and prompt versions — reducing **audit quality findings and regulatory pushback** ($25K-$100K per remediation cycle avoided) |

**Bottom line:** For a mid-size audit practice running 20-30 SOX/SOC2 engagements annually, this solution could deliver **$1M-$2M in annual savings** through reduced labor, faster cycles, and fewer quality defects.

### Where Auditor Time Is Redirected

- **Professional judgment** on borderline cases flagged by AI
- **Risk assessment** and scoping decisions
- **Client communication** and remediation guidance
- **Quality review** of AI evaluations rather than document review
- **Strategic audit planning** rather than operational evidence gathering

---

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (serverless or pro)
- Vector Search endpoint provisioned
- Databricks CLI configured with workspace profile

### Workspace Resources

| Resource | Value |
|---|---|
| **Workspace** | `https://fe-sandbox-akash-finance-app.cloud.databricks.com` |
| **Catalog / Schema** | `main.audit_schema` |
| **SQL Warehouse ID** | `1b1d59e180e4ac26` |
| **Pipeline Job ID** | `675224616522707` |
| **Vector Search Endpoint** | `mas-ea793b7b-endpoint` |
| **Vector Search Index** | `main.audit_schema.evidence_chunk_index` |
| **App Service Principal** | `caaef299-c070-4021-a317-8ba8267c6e0b` |

### Setup

1. **Create infrastructure** (tables, volumes, vector search index):

   ```bash
   # Run the DDL in a SQL Warehouse
   databricks sql execute --warehouse-id 1b1d59e180e4ac26 < setup/01_create_infrastructure.sql
   ```

2. **Deploy the Databricks App**:

   ```bash
   databricks apps deploy audit-control-evaluator --source-code-path backend/
   ```

3. **Upload the pipeline notebook**:

   ```bash
   databricks workspace import-dir notebooks/ /Workspace/Users/<email>/audit-control-app/notebooks/ --overwrite
   ```

4. **Create the pipeline job** pointing to `notebooks/00_run_full_pipeline.py` with serverless compute

5. **Access the app** and:
   - Create an audit engagement
   - Upload controls CSV (`sample_test_data/sox_controls.csv`)
   - Upload evidence files (`sample_test_data/pdf/*.pdf`)
   - Click "Run Evaluation" to trigger the pipeline

### Sample Test Data

| File | Description |
|---|---|
| `sample_test_data/sox_controls.csv` | 12 SOX-aligned controls (access control, change management, financial reporting, vulnerability management, business continuity, network security) |
| `sample_test_data/pdf/access_control_policy.pdf` | 4-page RBAC, MFA, and PAM policy |
| `sample_test_data/pdf/quarterly_access_review.pdf` | 3-page Q4 2025 access review report |
| `sample_test_data/pdf/change_management_procedures.pdf` | 4-page change workflow with CAB minutes |
| `sample_test_data/pdf/vulnerability_scan_report.pdf` | 3-page Q4 2025 vulnerability assessment |
| `sample_test_data/pdf/financial_close_checklist.pdf` | 4-page month-end close process |
| `sample_test_data/pdf/business_continuity_plan.pdf` | 4-page BCP/DR documentation |

---

## Project Structure

```
audit-control-app/
|
|-- backend/                               # Databricks App (FastAPI)
|   |-- app/
|   |   |-- main.py                       # FastAPI routes, file upload, pipeline trigger
|   |   |-- db.py                         # Databricks SQL connector with parameterized queries
|   |   |-- static/
|   |       |-- index.html                # Single-page frontend (HTML + Tailwind CSS + JS)
|   |-- app.yaml                          # Databricks App config (warehouse ID, uvicorn command)
|   |-- requirements.txt                  # Python dependencies
|
|-- notebooks/                             # Databricks notebooks
|   |-- 00_run_full_pipeline.py           # Main pipeline: parse -> chunk -> embed -> match -> eval -> save
|
|-- setup/                                 # Infrastructure
|   |-- 01_create_infrastructure.sql      # Full DDL: 8 tables, 3 volumes, catalog, schema
|   |-- create_all.py                     # Automated setup script
|   |-- load_evidence.py                  # Load synthetic evidence
|   |-- load_chunks_and_results.py        # Load chunks, matches, evaluations
|
|-- sample_test_data/                      # Demo data
|   |-- sox_controls.csv                  # 12 SOX controls (CSV)
|   |-- pdf/                              # 6 professional PDF evidence documents
|   |   |-- generate_pdfs.py             # Script to regenerate PDFs (fpdf2)
|   |-- evidence_*.txt                    # Text-format evidence samples
|
|-- resources/
|   |-- workflow.json                     # Databricks Workflow definition
|
|-- README.md                              # This file
```

---

## Technical Reference

### Models

| Model | Purpose | Details |
|---|---|---|
| `databricks-bge-large-en` | Embeddings | 1024-dim vectors, called via `ai_query()` in SQL |
| `databricks-meta-llama-3-3-70b-instruct` | Evaluation | Chain-of-thought prompting, structured JSON output |

### Chunking Configuration

| Parameter | Value |
|---|---|
| Strategy | Paragraph-aware with fallback to fixed-size |
| Max chunk size | 1500 characters |
| Overlap | 200 characters |
| Paragraph detection | `\n\n`, numbered lists, bullet points |

### Vector Search Configuration

| Parameter | Value |
|---|---|
| Index type | Delta Sync (automatic incremental updates) |
| Similarity metric | Cosine similarity |
| Top-K per control | 15 retrieved, top 8 used for evaluation |
| Minimum similarity | 0.4 threshold |

### LLM Evaluation Configuration

| Parameter | Value |
|---|---|
| Prompt version | `v4.0-cot` (chain-of-thought) |
| Output format | Structured JSON (verdict, confidence, reasoning, key_findings, gaps, summary) |
| Grounding | Evidence-only — no external knowledge |

### Backend API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/audits` | List all audit engagements |
| `POST` | `/api/audits` | Create new audit |
| `GET` | `/api/audits/{id}` | Get audit details |
| `POST` | `/api/audits/{id}/controls` | Upload controls CSV |
| `GET` | `/api/audits/{id}/controls` | List controls |
| `POST` | `/api/audits/{id}/evidence` | Upload evidence files |
| `GET` | `/api/audits/{id}/evidence` | List evidence documents |
| `POST` | `/api/audits/{id}/evaluate` | Trigger pipeline job |
| `GET` | `/api/audits/{id}/status` | Pipeline status |
| `GET` | `/api/audits/{id}/results` | Evaluation results with control details |
| `GET` | `/api/audits/{id}/results/summary` | Verdict counts and avg confidence |
| `GET` | `/api/audits/{id}/evidence-matches` | All evidence matches by control |
| `GET` | `/api/results/{id}` | Single evaluation with matched evidence chunks |
| `PUT` | `/api/results/{id}/review` | Submit auditor review (accept/override) |
| `GET` | `/api/audits/{id}/export` | Export results for reporting |
| `GET` | `/api/audits/{id}/audit-log` | Audit trail |

---

## License

This application is proprietary and intended for internal demonstration purposes.
