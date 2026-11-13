# py-parser

Enterprise document parsing service. Single responsibility: parse a document
into structured chunks with metadata. Exposed as an async gRPC API
(`SubmitParse` → poll `GetResult`), backed by a Celery worker pool.

Document extraction is delegated to **MinerU** (running as a separate HTTP
service); this package owns the chunking logic and the gRPC contract.

## Structure (hexagonal architecture)

```
py_parse/
├── ports/                 # Interfaces (dependency inversion)
│   ├── source_reader.py     # SourceReader: storage abstraction
│   └── document_extractor.py  # DocumentExtractor: document bytes → content_list
├── adapters/              # Concrete implementations
│   ├── extractor/           # MinerU V1 Tasks API client
│   ├── source_reader/       # file:// / minio:// / s3:// + scheme registry
│   ├── db/                  # outbox/chunks models + Postgres task store
│   └── queue/               # Celery app, worker task, outbox dispatcher
├── domain/                # Core parsing logic (depends only on ports)
│   ├── models.py            # Chunk / ChunkMetadata / DocMeta / ParseOutput
│   ├── chunker.py           # content_list → chunks (heading grouping + packing)
│   └── parser.py            # orchestrator: read → extract → chunk
└── ...
proto/
└── parse.proto             # gRPC service contract
```

## MinerU service (prerequisite)

py-parser does **not** bundle MinerU; it talks to a MinerU HTTP service.

MinerU 4.1's `mineru-kit api-server` runs the GPU parsing engine and exposes
the asynchronous **V1 Tasks API**. `MineruClient` drives it directly
(`POST /v1/tasks` → upload → poll → download), so only one MinerU service is
needed; the optional `file-parse-server` wrapper is not used.

### Option 1: Docker

```bash
docker compose -f docker/compose.yaml up -d
curl http://localhost:8000/v1/health   # → MinerU engine health
```

`docker/compose.mineru.yaml` starts just the MinerU engine without the gRPC
server.

### Option 2: pip (bare metal)

```bash
# 1. Isolated env + GPU deps (see requirements-mineru.txt header for CUDA notes)
python -m venv ~/mineru-env && source ~/mineru-env/bin/activate
pip install torch --index-url https://download.pytorch.org/whl/cu118   # driver 535 → cu118
pip install -r requirements-mineru.txt

# 2. Confirm GPU is used (False = torch was installed as CPU build)
python -c "import torch; print(torch.cuda.is_available())"

# 3. Download models, then start the engine
export MINERU_MODEL_SOURCE=modelscope
mineru-kit models download --tier standard --small-backend torch --vlm-engine vllm
mineru-kit api-server --host 127.0.0.1 --port 8000
```

Point `MINERU_URL` / `MineruClient(base_url=...)` at the engine (port 8000).

### What the client does with MinerU's output

The `default` format returns a ZIP containing both `structured_content.json`
and `middle_json.json`; the adapter reads both.

- **Figure / chart captions** become their own `image` chunks. MinerU labels
  plots `chart`; both types are treated as figures, and an uncaptioned figure
  is skipped rather than emitted empty.
- **Tables** are re-rendered from `middle_json`'s HTML, which still carries
  `rowspan`/`colspan`. `structured_content`'s markdown has already flattened
  them, which merges multi-level headers into a broken row; the HTML path
  emits qualified column names such as `BLEU / EN-DE` and repeats rowspan
  group labels. A table's `heading` is re-attributed to the nearest section
  that references `Table N`, since floated tables land at the top of a page.

See `requirements-mineru.txt` and `docker/compose.yaml` for full details.

## Run the parser (end-to-end check)

```bash
python scripts/test_parse.py /path/to/doc.pdf \
  --doc-id sample \
  --mineru-url http://localhost:8000 \
  --output-dir out
```

This prints an extraction report (block-type counts, detected headings), a
chunking report (token distribution, samples), and persists chunks as JSONL to
`out/sample.jsonl`.

## Run the async parse stack (Docker)

`docker/compose.yaml` starts the full async stack. Shared Postgres and the
chunks-cache Redis come from the repo-root infra stack, so start it once first:

```bash
docker compose -f ../compose.infra.yaml up -d --wait   # postgres + cache
docker compose -f docker/compose.yaml up -d
```

Services: `mineru-api` (document extraction, GPU), `redis` (Celery broker +
result backend), `outbox-dispatcher` (publishes pending outbox rows),
`celery-worker` (parse jobs), and `py-parse` (gRPC control plane). Postgres
(outbox + chunks) and the Redis chunks cache are external infrastructure.

> **Schema change (one-time):** task rows now live in `outbox_tasks`, and
> `chunks` references it instead of the old `tasks` table. `create_all()` never
> alters existing tables, so on a database created by an older version drop the
> old schema once before starting: `DROP TABLE IF EXISTS chunks, tasks CASCADE;`
> (or recreate the Postgres volume). There is no data migration by design.
>
> **Schema change (this version):** `outbox_tasks` gains `claimed_at` (the
> claim lease the stale-RUNNING reaper measures) and `requeued_from` (lineage
> for retry tasks). `create_all()` creates a missing table but never alters an
> existing one, so on an older database run:
> `ALTER TABLE outbox_tasks ADD COLUMN IF NOT EXISTS claimed_at timestamp,
>  ADD COLUMN IF NOT EXISTS requeued_from varchar(64);`

Submit path: `SubmitParse` inserts a PENDING row into `outbox_tasks`; the
dispatcher publishes it to Celery; a worker claims it in Postgres
(`PENDING → RUNNING`, `rowcount == 1`, so duplicate publishes are no-ops),
parses, writes chunks to Postgres, caches them in Redis, and returns metadata
to Celery. Outbox status mirrors Celery state (`PENDING` → `STARTED` →
`SUCCESS`/`FAILURE`).

Each publish gets a fresh queue message id, so a duplicate publish keeps its
own Celery result record instead of overwriting the one of the publish that
actually parsed the document. The business `task_id` travels in the message
body and is what the worker claims on.

A worker killed between its claim and its terminal write never resolves the
row, and a redelivered message is only a no-op duplicate. The dispatcher
therefore fails any RUNNING row whose `claimed_at` is older than
`RUNNING_TIMEOUT_SECONDS` (default 3600s; it must exceed `MINERU_TIMEOUT` or a
slow document is failed under a live worker). To retry that task — or any
FAILED one — call `RequeueParse(task_id)`: it inserts a **new** PENDING task for
the same document and returns its id. The failed record is never mutated; the
new row links back via `requeued_from`, so the history of the original attempt
(including its error) is preserved.

- gRPC endpoint: `localhost:50051` (service `parse.v1.ParseService`)
- API: `SubmitParse` (returns `task_id`) → poll `GetResult(task_id)`;
  `RequeueParse(task_id)` retries a FAILED task as a new task;
  `ListTasks(sources?)` lists tasks of every status (newest first, one row per
  `source` — that source's most recent status) with task_id, doc_id, source,
  status, error and timestamps; the optional `sources` list narrows it to those
  URIs, which is how a caller decides per source whether to submit, skip,
  requeue or attach
- One task per source: `SubmitParse` returns `ALREADY_EXISTS` (with the
  existing `task_id`) when that `source` already has a task, whatever its
  status. A FAILED document is retried with `RequeueParse`, never resubmitted.
- Health probe: standard gRPC health service (`grpc-health-probe` compatible)

Run the gRPC server directly (no Docker) with `python -m py_parse`, an outbox
dispatcher with `python -m py_parse.adapters.queue.dispatcher`, and a worker
with:

```bash
celery -A py_parse.adapters.queue.celery_app worker --loglevel=info -c 2
```

See `py_parse/config.py` for env vars (`GRPC_PORT`, `REDIS_URL`,
`CACHE_REDIS_URL`, `POSTGRES_DSN`, `MINERU_URL`, outbox tuning, storage
credentials).

Test the async contract end-to-end:

```bash
python scripts/grpc_client.py file:///path/to/doc.pdf --doc-id sample
```

## Storage abstraction

- `source` is a uniform URI: `file:///...` / `minio://...` / `s3://...`; the
  scheme selects the storage backend.
- The request carries only location; connection config (endpoints,
  credentials) is injected at adapter construction time.
- Adding a backend = write an adapter + `registry.register(scheme, reader)`.

## Generate gRPC code

```bash
python -m grpc_tools.protoc \
  -I proto \
  --python_out=py_parse/generated \
  --grpc_python_out=py_parse/generated \
  proto/parse.proto
```

## Wiring example (for main)

```python
from py_parse.adapters import FileSourceReader, MineruClient, SourceReaderRegistry
from py_parse.domain.parser import DocumentParser

extractor = MineruClient(base_url="http://localhost:8000")
parser = DocumentParser(extractor)

registry = SourceReaderRegistry()
registry.register("file", FileSourceReader())
# ...

stream = registry.resolve("file:///data/docs/doc_001.pdf").read(...)
out = parser.parse(stream, doc_id="doc_001", filename="doc_001.pdf")
```
