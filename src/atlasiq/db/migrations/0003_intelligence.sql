CREATE VIEW public.intelligence_candidates AS
    SELECT 'STARTUP'::text AS kind, id AS record_id, raw_document_id, source_id, source_url,
           entity_name AS title, NULL::text AS evidence_text
    FROM public.startups
    UNION ALL
    SELECT 'PRODUCT', id, raw_document_id, source_id, source_url, product_name, NULL::text FROM public.products
    UNION ALL
    SELECT 'RESEARCH_PAPER', id, raw_document_id, source_id, source_url, title, abstract FROM public.research_papers
    UNION ALL
    SELECT 'JOB', id, raw_document_id, source_id, source_url, job_title, full_text FROM public.jobs
    UNION ALL
    SELECT 'NEWS', id, raw_document_id, source_id, source_url, title, full_text FROM public.news;

CREATE TABLE public.intelligence_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_document_id uuid NOT NULL REFERENCES public.raw_documents (id),
    task_id uuid NOT NULL UNIQUE REFERENCES public.ingestion_tasks (id),
    record_type text NOT NULL CHECK (record_type IN ('STARTUP','PRODUCT','RESEARCH_PAPER','JOB','NEWS')),
    canonical_record_id uuid NOT NULL,
    extractor_version text NOT NULL,
    source_url text NOT NULL,
    intelligence jsonb NOT NULL CHECK (jsonb_typeof(intelligence) = 'object'),
    chunk_count integer NOT NULL CHECK (chunk_count > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (record_type, canonical_record_id, raw_document_id, extractor_version)
);
CREATE INDEX intelligence_records_record_history
    ON public.intelligence_records (canonical_record_id, created_at DESC);

CREATE VIEW public.intelligence_current AS
    SELECT result.* FROM public.intelligence_records AS result
    JOIN public.intelligence_candidates AS candidate
      ON candidate.kind = result.record_type AND candidate.record_id = result.canonical_record_id
     AND candidate.raw_document_id = result.raw_document_id;

ALTER TABLE public.extraction_attempts
    ADD COLUMN task_id uuid REFERENCES public.ingestion_tasks (id),
    ADD COLUMN input_tokens integer CHECK (input_tokens >= 0),
    ADD COLUMN output_tokens integer CHECK (output_tokens >= 0);

CREATE INDEX extraction_attempts_task_time ON public.extraction_attempts (task_id, created_at DESC)
    WHERE task_id IS NOT NULL;
CREATE INDEX extraction_attempts_recent ON public.extraction_attempts (created_at DESC, id DESC);
CREATE INDEX ingestion_tasks_recent ON public.ingestion_tasks (updated_at DESC, id DESC);
CREATE INDEX crawl_runs_last_success ON public.crawl_runs (source_id, finished_at DESC)
    WHERE accepted_count > 0 AND status IN ('COMPLETED','PARTIAL');
