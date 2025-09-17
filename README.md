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
- **Observability**: Sentry-ready, structured logs recommended

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
  - `Authorization: Bearer <token>` (required; presence-checked by default)

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
  - `Authorization: <STATIC_TOKEN>` (required if auth enabled)

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
- **Validation**: Implemented via Zod schemas under `src/schemas/` and applied by `validateBody` middleware.
- **OpenAPI**: You can generate OpenAPI from Zod using tools like `zod-to-openapi`. Consider serving docs at `/docs` with `swagger-ui-express`.

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
- **Retries**: Controllers enqueue with exponential backoff and `attempts: 3`

## Security
- **Network**: Put behind Nginx with TLS
- **Auth**: Validate `Authorization` header; replace static token with proper auth (JWT/API keys)
- **Secrets**: Store DSN/API keys in env/secret manager
- **Limits**: JSON body limit set to `10mb`; add rate limiting if public

## Troubleshooting
- **Mongo**: Verify `mongodb_uri` and network access
- **Redis**: Ensure Redis is reachable by workers
- **Athena**: Check AWS creds, region, database, and S3 output location
- **Facebook API**: Validate access token scopes and date ranges; handle 429 backoff

## Scripts
- **Start API**: `npm start` (runs `node src/server.js`)
- **Start workers**: `node src/jobs/workers/task.worker.js`, `node src/jobs/workers/adLibrary.worker.js`

## License
ISC


