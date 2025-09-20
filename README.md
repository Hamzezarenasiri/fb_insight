## fb_insight

An Express.js API to import and process Facebook Ads data. It fetches ad-level insights and Ad Library entries from Facebook Graph API, persists raw and processed data in MongoDB, optionally merges AWS Athena aggregates for allowlisted accounts, enriches assets, and exposes background job processing via BullMQ.

### Features
- **Facebook insights**: Graph API v22.0 insights with creatives (batched requests)
- **Ad Library**: Search by `search_page_ids` and date range
- **Storage**: MongoDB collections (`fb_insights`, `metrics`, `reports_data`, `assets`, etc.)
- **Optional Athena**: Query and merge aggregates for selected accounts
- **Enrichment**: Preview scraping, optional OpenAI, external tagging API
- **Validation**: Zod request schemas per endpoint
- **Jobs**: BullMQ queues + workers replacing `setTimeout`
- **Observability**: Structured JSON logs for every stage (fetch, mapping, metrics, report, enrichment, tagging)

### Architecture
- **Ingress**: Client → Nginx (TLS) → Express app
- **HTTP**: Routes → Controllers → Services → Repositories
- **Jobs**: Controllers enqueue jobs → BullMQ workers run domain tasks
- **Data sources**: Facebook Graph API, AWS Athena (optional)
- **Storage**: MongoDB

### Directory structure (key parts)
```text
src/
  app.js                # express app setup
  server.js             # process entrypoint
  routes/               # http routes (task, ad-library)
  controllers/          # request handling, enqueue jobs
  middlewares/          # auth, validate, error
  schemas/              # zod request schemas
  services/
    facebook/           # clients for insights/ad library, fields
    reporting/          # mapping, metrics, process, schema.defaults
    enrichment/         # preview/openai/tagging/product
    athena/             # runAthenaQuery
    status/             # saveFacebookImportStatus
  repositories/
    mongo/              # client + common CRUD helpers
  jobs/
    queues.js           # BullMQ queues
    workers/            # job workers (task/adLibrary)
```

## Getting started

### Prerequisites
- **Node.js** 18+
- **MongoDB** (Atlas or self-hosted)
- **Redis** for BullMQ (localhost ok)
- **Facebook Marketing API** access token(s)
- Optional: **AWS** credentials and Athena config

### Install
```bash
npm install
```

### Environment variables
Create `.env` in the project root.

- **Core**
  - `mongodb_uri` (required): MongoDB connection string
  - `PORT` (optional): Express port, default `3000`
  - `SENTRY_DSN` (optional): Sentry DSN
- **Auth/External**
  - `STATIC_TOKEN` (optional): Static token for `/run-ad-library`
  - `FLUX_STATIC_API_KEY` (optional): External tagging API key
  - `OPENAI_API_KEY` (optional): OpenAI API key
- **AWS Athena (optional)**
  - `AWS_REGION` (default `us-east-1`)
  - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
  - `ATHENA_DATABASE` (e.g. `marketing`)
  - `ATHENA_OUTPUT_LOCATION` (e.g. `s3://bucket/prefix/`)
- **Redis (BullMQ)**
  - `REDIS_HOST` (default `127.0.0.1`)
  - `REDIS_PORT` (default `6379`)
  - `REDIS_PASSWORD` (optional)

Example:
```bash
mongodb_uri=mongodb+srv://user:pass@cluster/db
PORT=3000
SENTRY_DSN=https://xxx@sentry.io/yyy
STATIC_TOKEN=replace-me
FLUX_STATIC_API_KEY=your-flux-api-key
OPENAI_API_KEY=sk-...
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
ATHENA_DATABASE=marketing
ATHENA_OUTPUT_LOCATION=s3://your-bucket/athena/results/
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
```

### Run locally
```bash
# 1) start API
npm start

# 2) start workers (separate terminals)
node src/jobs/workers/task.worker.js
node src/jobs/workers/adLibrary.worker.js
```

The server logs: `API listening on <PORT>`.

### API Docs (OpenAPI)

- Visit `/docs` for Swagger UI (served from `src/docs/openapi.yaml`).
- Update the YAML file to add/modify endpoints and schemas.
- Request/response validation enforced via Zod schemas.

### Nginx (recommended)

Express trusts the proxy, respecting `X-Forwarded-*` headers. Example config:
```nginx
upstream fb_insight_upstream { server 127.0.0.1:3000; }
server {
  listen 443 ssl http2;
  server_name example.com;
  location / {
    proxy_pass http://fb_insight_upstream;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header Authorization $http_authorization;
  }
}
```

Call via `https://example.com/run-task`.

## API reference

### POST `/run-task`
Triggers Facebook Ads insights import for the provided account and date range. Validated via Zod.

- **Headers**
  - `Authorization: Bearer <token>` (required)

- **Body (JSON)**
```json
{
  "fbAccessToken": "<facebook-graph-api-token>",
  "FBadAccountId": "act_123456789",
  "start_date": "2025-04-01",
  "end_date": "2025-04-30",
  "agencyId": "<mongoObjectId>",
  "clientId": "<mongoObjectId>",
  "userId": "<mongoObjectId>",
  "importListName": "My Import",
  "uuid": "<job-uuid>",
  "ad_objective_id": "leads_all",
  "ad_objective_field_expr": "actions.lead",
  "ai": "gemini"  
  // optional; may also be null
}
```

- **Responses**
  - `200 OK`: `{ "success": true, "message": "Task has been queued for processing" }`
  - `400 Bad Request`: Schema validation failed
  - `401 Unauthorized`: Missing header

- **Processing**
  - Enqueues a BullMQ job (`run-task`) handled by `task.worker.js`
  - Persists raw and processed data; optionally merges Athena; enriches assets

### POST `/run-ad-library`
Fetches Facebook Ad Library entries (optional endpoint).

- **Headers**
  - `Authorization: Bearer <STATIC_TOKEN>` (required if auth enabled)

- **Body (JSON)**
```json
{
  "fbAccessToken": "<facebook-graph-api-token>",
  "FBadAccountId": "act_123456789",
  "start_date": "2025-04-01",
  "end_date": "2025-04-30",
  "search_page_ids": "['98269389167']",
  "max_count": 50
}
```

- **Responses**
  - `200 OK`: `{ "success": true, "message": "Task has been queued for processing" }`
  - `400 Bad Request`: Schema validation failed
  - `401 Unauthorized`: Missing/invalid static token

- **Processing**
  - Enqueues a BullMQ job (`run-ad-library`) handled by `adLibrary.worker.js`

### Validation and OpenAPI
- **Validation**: Implemented via Zod schemas under `src/schemas/` and applied by `validate` middleware. Optional fields like `ai`, `ad_objective_id`, `ad_objective_field_expr` accept `null`.
- **OpenAPI**: Served at `/docs` using `swagger-ui-express` with `src/docs/openapi.yaml`.

### Facebook insights ingestion details

- Response normalization: Facebook returns many metrics as arrays of `{action_type, value}` or as numeric strings. The API normalizes these into plain objects and numbers. Example: `actions: [{action_type: 'link_click', value: '159'}]` becomes `actions.link_click = 159`. Dotted action types are flattened with `_` (e.g., `offsite_conversion.custom.123` → `offsite_conversion_custom_123`).
- Requested fields: Includes `impressions`, `reach`, `spend`, `inline_link_clicks`, `frequency`, `actions`, `action_values`, and video fields `video_thruplay_watched_actions`, `video_avg_time_watched_actions`, `video_p{25,50,75,95,100}_watched_actions`, plus `purchase_roas`.
- Mapping for metrics:
  - `link_clicks` is sourced from `inline_link_clicks` (fallback `actions.link_click`). Both `link_clicks` and legacy `link_click` are populated.
  - `vvr = actions.video_view / impressions`.
  - `hold = video_thruplay_watched_actions.video_view / impressions`.
  - `cpc/cpl/cpa` prefer `cost_per_action_type.{link_click|lead|purchase}`; otherwise derived.
  - `purchases` is mapped from `actions.purchase` for formulas like AOV/CPA.
  - `frequency` is requested; if missing but `impressions` and `reach` exist, a fallback `frequency = impressions / reach` is computed.
- Objective result: `result` is taken from `ad_objective_field_expr` (e.g., `actions.lead`). `cvr = result / link_clicks`, `cpr = spend / result`.

### Inclusion of ACTIVE ads with zero activity

- The system lists ads from the Ads edge and fetches per-ad insights for the selected range.
- If an ad is ACTIVE (by `effective_status` or `status`) and returns no insights rows for the range, the API synthesizes a zero-metric record (impressions=0, spend=0, link_clicks=0, vvr=0, hold=0, etc.).
- This ensures active ads are present in reports even when they had no measurable actions in the period, avoiding undercount/misinterpretation.

### Hybrid fetch to prevent spend gaps

- Primary listing: Insights index
  - We first list ads via `/{account_id}/insights?level=ad&fields=ad_id&time_range=...`.
  - This captures all ads that had in-range insights/spend, even if they are currently paused/archived.
- Secondary pass: Ads edge for zeros
  - We then query `/{account_id}/ads?fields=id,name,effective_status,status&ad_status=ACTIVE`.
  - For currently ACTIVE ads missing from the primary set, we add a synthesized zero-metric row so they remain visible.
- Result: Ads with spend in-range are always included; ACTIVE ads with no in-range activity get zero rows; non-active/no-activity ads are not included.

### Logging and observability

The API emits structured logs you can tail with PM2:

- Fetch progress
  - `fb.insights.list.start|page|page.done|list.done`
  - `fb.ads.edge.page` (Ads edge pagination and count of zeros to add)
- Batch-level diagnostics
  - `fb.batch.insights.error|missing_body|parse_error`
  - `fb.batch.detail.error|missing_body|parse_error`
- Synthetic rows
  - `fb.synthetic.zero` when a zero-metric ad record is created
- Ads pass fallback
  - `fb.ads.edge.skip` if Ads edge call is skipped due to errors

Monitor on server:
```bash
npx pm2 logs fb-task-worker --lines 200 --timestamp
```

## Data model (MongoDB)
- **fb_insights**: raw ad insights with creative, status, post_url
- **metrics**: processed records per import list and schema
- **imported_lists**: import session metadata + schema snapshot
- **reports_data**, **sub_reports**: reporting artifacts
- **assets**: per-ad assets, creative metadata, extracted links/messages
- **facebook_imports**: job status/progress log
- Optional: **athena_result**, **merged_results**, **fb_ad_libraries**

## Jobs (BullMQ)
- **Queues**: `run-task`, `run-ad-library` (`src/jobs/queues.js`)
- **Workers**: `src/jobs/workers/*.worker.js`
- **Config**: Redis via `REDIS_*` env vars
- **De-duplication**: Jobs are enqueued with `jobId = uuid` to prevent duplicates
- **Retries**: Disabled (`attempts: 1`) so failed jobs do not auto re-run
- **Concurrency**: Workers run with limited concurrency for stability

### Observability (logs)
- Logs are emitted as JSON for easier grep/parse. Common stages:
  - `worker.task.start|done|error` (job lifecycle)
  - `task.start`, `fetch.ads.start|done`, `fb_insights.inserted`
  - `athena.start`, `athena.query.start|poll|done`, `merge.athena`
  - `mapping.headers|match|included.keys|formData`
  - `records.validate.start|done`, `metrics.spend.stats`
  - `metrics.insert.result`, `report.create.done`, `sub_reports.cloned|created.default`
  - `enrichment.messages_links.start|done`, `enrichment.products.start|done`
  - `tagging.start|done|skipped`
  - HTTP client: `http.success|retry|catch.retry|error|fail` (redacted URLs)

## Security
- **Network**: Put behind Nginx with TLS
- **Auth**: Validate `Authorization` header; replace static token with proper auth (JWT/API keys)
- **Secrets**: Store DSN/API keys in env/secret manager
- **Limits**: JSON body limit set to `10mb`; add rate limiting if public

## Troubleshooting
- **Mongo**: Verify `mongodb_uri` and network access
- **Redis**: Ensure Redis is reachable by workers (BullMQ recommends Redis >= 6.2; warnings are informational)
- **Athena**: Check AWS creds, region, database, and S3 output location
- **Facebook API**: Validate access token scopes and date ranges; 429s will backoff and retry automatically
- **Swagger not found**: Install `swagger-ui-express` and `yamljs`, then restart
- **Port in use (EADDRINUSE)**: Stop old PM2 processes or change `PORT`
- **npm ci lock mismatch**: Run `npm install` locally, commit `package-lock.json`, deploy
- **$arrayToObject 40392**: Caused by malformed tag data. The enrichment step now filters invalid `{k,v}` pairs and falls back to JS-built objects, allowing the job to complete.

## Production
Use PM2 and Node 20+:
```bash
# install deps
npm ci --omit=dev

# start
npx pm2 start src/server.js --name fb-api --time
npx pm2 start src/jobs/workers/task.worker.js --name fb-task-worker --time
npx pm2 start src/jobs/workers/adLibrary.worker.js --name fb-adlib-worker --time

# logs
npx pm2 logs fb-api fb-task-worker fb-adlib-worker --lines 200
```

## Scripts
- **Start API**: `npm start` (runs `node src/server.js`)
- **Start workers**: `node src/jobs/workers/task.worker.js`, `node src/jobs/workers/adLibrary.worker.js`

## License
ISC


