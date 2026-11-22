ALTER TABLE public.crawl_runs
    ADD COLUMN requested_limit integer CHECK (requested_limit > 0),
    ADD COLUMN discovery_done boolean NOT NULL DEFAULT false,
    ADD COLUMN duplicate_discoveries bigint NOT NULL DEFAULT 0 CHECK (duplicate_discoveries >= 0),
    ADD COLUMN discovery_retry_at timestamptz;

CREATE UNIQUE INDEX crawl_runs_active_ingestion ON public.crawl_runs (source_id)
    WHERE requested_limit IS NOT NULL AND status = 'RUNNING';

ALTER TABLE public.ingestion_tasks
    ADD COLUMN processing_result text
        CHECK (processing_result IN ('INSERTED', 'UPDATED', 'REJECTED')),
    ADD CONSTRAINT ingestion_tasks_result_requires_completion
        CHECK (processing_result IS NULL OR status = 'COMPLETED');

ALTER TABLE public.raw_documents ADD COLUMN processing_evidence jsonb;

ALTER TABLE public.research_papers
    ADD COLUMN abstract text,
    ADD COLUMN collected_at timestamptz;

CREATE TABLE public.crawl_run_items (
    crawl_run_id uuid NOT NULL REFERENCES public.crawl_runs (id),
    discovery_key text NOT NULL,
    task_id uuid NOT NULL REFERENCES public.ingestion_tasks (id),
    reused boolean NOT NULL DEFAULT false,
    PRIMARY KEY (crawl_run_id, discovery_key)
);
CREATE INDEX crawl_run_items_task_run ON public.crawl_run_items (task_id, crawl_run_id);
