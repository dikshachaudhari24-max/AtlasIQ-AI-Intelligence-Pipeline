CREATE TABLE public.sources (
    id text PRIMARY KEY CHECK (id ~ '^[a-z0-9]([a-z0-9_-]{0,62}[a-z0-9])?$'),
    name text NOT NULL CHECK (btrim(name) <> ''),
    category text NOT NULL CHECK (category IN ('STARTUP', 'PRODUCT', 'RESEARCH_PAPER', 'JOB', 'NEWS')),
    base_url text NOT NULL,
    adapter_name text NOT NULL CHECK (btrim(adapter_name) <> ''),
    adapter_config jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(adapter_config) = 'object'),
    schedule_config jsonb CHECK (jsonb_typeof(schedule_config) = 'object'),
    rate_limit_config jsonb NOT NULL CHECK (jsonb_typeof(rate_limit_config) = 'object'),
    enabled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX sources_category_name_unique ON public.sources (category, lower(name));

CREATE TABLE public.crawl_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id text NOT NULL REFERENCES public.sources (id),
    started_at timestamptz,
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'PARTIAL')),
    cursor jsonb,
    discovered_count bigint NOT NULL DEFAULT 0 CHECK (discovered_count >= 0),
    accepted_count bigint NOT NULL DEFAULT 0 CHECK (accepted_count >= 0),
    rejected_count bigint NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    failure_summary text,
    UNIQUE (id, source_id),
    CHECK (status = 'PENDING' OR started_at IS NOT NULL),
    CHECK ((finished_at IS NOT NULL) = (status IN ('COMPLETED', 'FAILED', 'PARTIAL'))),
    CHECK (finished_at >= started_at)
);
CREATE INDEX crawl_runs_source_started ON public.crawl_runs (source_id, started_at DESC);

CREATE TABLE public.raw_documents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id text NOT NULL REFERENCES public.sources (id),
    crawl_run_id uuid NOT NULL,
    upstream_id text CHECK (btrim(upstream_id) <> ''),
    source_url text NOT NULL,
    canonical_url text NOT NULL,
    discovery_key text GENERATED ALWAYS AS (
        CASE WHEN upstream_id IS NULL THEN 'url:' || canonical_url ELSE 'id:' || upstream_id END
    ) STORED,
    raw_content text,
    storage_reference text,
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    fetched_at timestamptz NOT NULL,
    original_timestamp_evidence jsonb CHECK (jsonb_typeof(original_timestamp_evidence) = 'object'),
    status text NOT NULL DEFAULT 'DISCOVERED'
        CHECK (status IN ('DISCOVERED', 'ACCEPTED', 'QUARANTINED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (crawl_run_id, source_id) REFERENCES public.crawl_runs (id, source_id),
    UNIQUE (id, source_id),
    UNIQUE (source_id, discovery_key, content_hash),
    CHECK ((raw_content IS NOT NULL) <> (storage_reference IS NOT NULL)),
    CHECK (storage_reference IS NULL OR btrim(storage_reference) <> '')
);
CREATE INDEX raw_documents_crawl_run ON public.raw_documents (crawl_run_id);

CREATE TABLE public.ingestion_tasks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_document_id uuid NOT NULL REFERENCES public.raw_documents (id),
    stage text NOT NULL CHECK (btrim(stage) <> ''),
    input_version text NOT NULL CHECK (btrim(input_version) <> ''),
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'CLAIMED', 'COMPLETED', 'RETRY', 'FAILED')),
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    next_retry_at timestamptz DEFAULT clock_timestamp(),
    lease_owner text,
    lease_expires_at timestamptz,
    claim_token uuid,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (raw_document_id, stage, input_version),
    CHECK (attempts BETWEEN 0 AND max_attempts),
    CHECK ((status = 'CLAIMED') = (lease_owner IS NOT NULL)),
    CHECK ((status = 'CLAIMED') = (lease_expires_at IS NOT NULL)),
    CHECK ((status IN ('CLAIMED', 'COMPLETED')) = (claim_token IS NOT NULL)),
    CHECK ((status IN ('PENDING', 'RETRY')) = (next_retry_at IS NOT NULL))
);
CREATE INDEX ingestion_tasks_ready ON public.ingestion_tasks (stage, next_retry_at, id)
    WHERE status IN ('PENDING', 'RETRY');
CREATE INDEX ingestion_tasks_ready_all ON public.ingestion_tasks (next_retry_at, id)
    WHERE status IN ('PENDING', 'RETRY');
CREATE INDEX ingestion_tasks_expiring ON public.ingestion_tasks (lease_expires_at, id)
    WHERE status = 'CLAIMED';
CREATE INDEX ingestion_tasks_status ON public.ingestion_tasks (status);

CREATE TABLE public.extraction_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_document_id uuid NOT NULL REFERENCES public.raw_documents (id),
    provider text NOT NULL,
    model_name text NOT NULL,
    chunk_index integer NOT NULL CHECK (chunk_index >= 0),
    latency_ms integer CHECK (latency_ms >= 0),
    response_status integer CHECK (response_status BETWEEN 100 AND 599),
    error_code text,
    retry_number integer NOT NULL DEFAULT 0 CHECK (retry_number >= 0),
    outcome text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX extraction_attempts_document ON public.extraction_attempts (raw_document_id);

CREATE TABLE public.startups (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_key text NOT NULL UNIQUE CHECK (btrim(canonical_key) <> ''),
    entity_name text NOT NULL CHECK (btrim(entity_name) <> ''),
    employee_count bigint CHECK (employee_count >= 0),
    source_id text NOT NULL REFERENCES public.sources (id),
    source_url text NOT NULL,
    raw_document_id uuid NOT NULL,
    schema_version text NOT NULL,
    collected_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (raw_document_id, source_id) REFERENCES public.raw_documents (id, source_id)
);
CREATE INDEX startups_source ON public.startups (source_id);
CREATE INDEX startups_name ON public.startups (lower(entity_name));

CREATE TABLE public.products (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_key text NOT NULL UNIQUE CHECK (btrim(canonical_key) <> ''),
    product_name text NOT NULL CHECK (btrim(product_name) <> ''),
    startup_id uuid REFERENCES public.startups (id),
    startup_name text NOT NULL CHECK (btrim(startup_name) <> ''),
    pricing_model text NOT NULL CHECK (pricing_model IN ('FREE', 'FREEMIUM', 'PAID', 'ENTERPRISE')),
    source_id text NOT NULL REFERENCES public.sources (id),
    source_url text NOT NULL,
    raw_document_id uuid NOT NULL,
    schema_version text NOT NULL,
    collected_at timestamptz NOT NULL,
    FOREIGN KEY (raw_document_id, source_id) REFERENCES public.raw_documents (id, source_id)
);
CREATE INDEX products_source ON public.products (source_id);
CREATE INDEX products_name ON public.products (lower(product_name));
CREATE INDEX products_startup ON public.products (startup_id) WHERE startup_id IS NOT NULL;

CREATE TABLE public.research_papers (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_key text NOT NULL UNIQUE CHECK (btrim(canonical_key) <> ''),
    arxiv_id text CHECK (btrim(arxiv_id) <> ''),
    doi text CHECK (btrim(doi) <> ''),
    title text NOT NULL CHECK (btrim(title) <> ''),
    authors jsonb NOT NULL CHECK (jsonb_typeof(authors) = 'array'),
    paper_url text NOT NULL,
    published_date timestamptz NOT NULL,
    source_id text NOT NULL REFERENCES public.sources (id),
    source_url text NOT NULL,
    raw_document_id uuid NOT NULL,
    schema_version text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (raw_document_id, source_id) REFERENCES public.raw_documents (id, source_id)
);
CREATE UNIQUE INDEX research_papers_arxiv_unique ON public.research_papers (lower(arxiv_id))
    WHERE arxiv_id IS NOT NULL;
CREATE UNIQUE INDEX research_papers_doi_unique ON public.research_papers (lower(doi))
    WHERE doi IS NOT NULL;
CREATE INDEX research_papers_source ON public.research_papers (source_id);
CREATE INDEX research_papers_publication ON public.research_papers (published_date DESC);

CREATE TABLE public.paper_github_metrics (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    paper_id uuid NOT NULL REFERENCES public.research_papers (id),
    github_repository_url text NOT NULL,
    repository_identity text NOT NULL CHECK (btrim(repository_identity) <> ''),
    association_evidence jsonb NOT NULL
        CHECK (jsonb_typeof(association_evidence) = 'object' AND association_evidence <> '{}'::jsonb),
    github_stars bigint CHECK (github_stars >= 0),
    observed_at timestamptz NOT NULL,
    UNIQUE (paper_id, repository_identity, observed_at)
);
CREATE INDEX paper_github_metrics_observation ON public.paper_github_metrics (repository_identity, observed_at DESC);

CREATE TABLE public.jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_key text NOT NULL UNIQUE CHECK (btrim(canonical_key) <> ''),
    source_url text NOT NULL,
    canonical_url text NOT NULL,
    upstream_id text,
    source_id text NOT NULL REFERENCES public.sources (id),
    raw_document_id uuid NOT NULL,
    company text NOT NULL CHECK (btrim(company) <> ''),
    job_title text NOT NULL CHECK (btrim(job_title) <> ''),
    role_family text NOT NULL CHECK (btrim(role_family) <> ''),
    location text,
    is_remote boolean,
    published_at timestamptz NOT NULL,
    publication_evidence jsonb NOT NULL
        CHECK (jsonb_typeof(publication_evidence) = 'object' AND publication_evidence <> '{}'::jsonb),
    full_text text NOT NULL CHECK (btrim(full_text) <> ''),
    schema_version text NOT NULL,
    collected_at timestamptz NOT NULL,
    FOREIGN KEY (raw_document_id, source_id) REFERENCES public.raw_documents (id, source_id)
);
CREATE INDEX jobs_source ON public.jobs (source_id);
CREATE INDEX jobs_publication ON public.jobs (published_at DESC);

CREATE TABLE public.news (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_key text NOT NULL UNIQUE CHECK (btrim(canonical_key) <> ''),
    source_url text NOT NULL,
    canonical_url text NOT NULL,
    upstream_id text,
    source_id text NOT NULL REFERENCES public.sources (id),
    raw_document_id uuid NOT NULL,
    title text NOT NULL CHECK (btrim(title) <> ''),
    full_text text NOT NULL CHECK (btrim(full_text) <> ''),
    published_at timestamptz NOT NULL,
    publication_evidence jsonb NOT NULL
        CHECK (jsonb_typeof(publication_evidence) = 'object' AND publication_evidence <> '{}'::jsonb),
    schema_version text NOT NULL,
    collected_at timestamptz NOT NULL,
    FOREIGN KEY (raw_document_id, source_id) REFERENCES public.raw_documents (id, source_id)
);
CREATE INDEX news_source ON public.news (source_id);
CREATE INDEX news_publication ON public.news (published_at DESC);

CREATE TABLE public.entity_mappings (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_name text NOT NULL CHECK (btrim(raw_name) <> ''),
    canonical_name text CHECK (btrim(canonical_name) <> ''),
    canonical_key text CHECK (btrim(canonical_key) <> ''),
    source_url text NOT NULL,
    match_method text NOT NULL CHECK (btrim(match_method) <> ''),
    confidence numeric CHECK (confidence BETWEEN 0 AND 1),
    review_status text NOT NULL CHECK (review_status IN ('PENDING', 'ACCEPTED', 'REJECTED')),
    resolved_at timestamptz,
    CHECK ((review_status = 'PENDING') = (resolved_at IS NULL)),
    CHECK (review_status <> 'ACCEPTED' OR (canonical_key IS NOT NULL AND canonical_name IS NOT NULL))
);
CREATE INDEX entity_mappings_canonical ON public.entity_mappings (canonical_key);

CREATE TABLE public.quarantine_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_document_id uuid NOT NULL REFERENCES public.raw_documents (id),
    record_type text NOT NULL CHECK (record_type IN ('STARTUP', 'PRODUCT', 'RESEARCH_PAPER', 'JOB', 'NEWS')),
    reason_code text NOT NULL CHECK (btrim(reason_code) <> ''),
    reason_detail text,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX quarantine_records_document ON public.quarantine_records (raw_document_id);
