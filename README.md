# fb_insight Service

A Node.js/Express service that imports Facebook Ads Insights and Ad Library data, enriches and normalizes metrics, optionally merges with AWS Athena results for specific accounts, persists to MongoDB, and triggers downstream tagging and reporting workflows.

### Features
- Import Facebook Ads Insights per-ad with batching and retry/backoff
- Import Facebook Ad Library with pagination and progress tracking
- Athena query execution with polling and result transformation
- Normalization and computation of metrics using formulas
- Asset enrichment (messages, product links, preview data)
- Product/tag generation via OpenAI and tag consolidation
- Background processing with status persistence to MongoDB
- Error tracking via Sentry

### Tech stack
- Node.js (ESM), Express
- MongoDB Node.js Driver v6
- Axios for HTTP
- AWS SDK v3 for Athena
- Sentry for error monitoring
- dotenv for configuration
- acorn for safe formula parsing/compilation

### Prerequisites
- Node.js 18+
- MongoDB instance and connection string
- AWS account with Athena configured (database and S3 output)
- Facebook Graph API access token(s)
- OpenAI API key (for product/tag extraction)
- Flux API key (for tagging)

### Environment variables
Provide these in a `.env` file (see `.env.example`).

- `mongodb_uri`: MongoDB connection string
- `PORT` (optional): Server port (default 3000)
- `AWS_REGION`: AWS region for Athena (default `us-east-1`)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `ATHENA_DATABASE`: Athena database name
- `ATHENA_OUTPUT_LOCATION`: S3 URI for Athena results (e.g., s3://bucket/path/)
- `FLUX_STATIC_API_KEY`: API key for Flux tagging service
- `OPENAI_API_KEY`: OpenAI API key

Note: Sentry DSN and the API auth token are hardcoded in `index.js`. See Security notes below.

### Install
```bash
npm install
```

### Run
```bash
node index.js
```

The server starts on `http://localhost:3000` (or `:${PORT}`).

### Authentication
All endpoints require an `Authorization` header that must match the static token defined in code. Default:

- Header: `Authorization: KV5NfjBPaN9JDWqbDXrjQGoyeMtQWyfG16nTHmUPXFw=`

Change `STATIC_TOKEN` in `index.js` for production use.

### API
- POST `/run-task`: Import Ads Insights, process, and store metrics
- POST `/run-ad-library`: Import Ad Library entries

See `docs/API.md` for full parameter lists and cURL examples.

### Data flow (high-level)
1. Request hits endpoint; simple static-token auth check
2. Background task starts and saves status to `facebook_imports`
3. For `/run-task`:
   - Fetch ad IDs, batch-fetch insights and ad creatives from Facebook
   - Optional: For specific accounts, fetch/aggregate Athena results and merge
   - Normalize/derive metrics and map headers to schema
   - Upsert/update `assets`, create `imported_lists`, `metrics`, `reports_data`, `sub_reports`
   - Enrich assets with messages/product links and generate products/tags
   - Optionally call Flux tagging API
4. For `/run-ad-library`:
   - Paginate Ad Library API and insert rows into `fb_ad_libraries`
5. Progress updates written to `facebook_imports`

### Collections (used)
- `facebook_imports`, `fb_insights`, `athena_result`, `merged_results`
- `imported_lists`, `metrics`, `reports_data`, `sub_reports`
- `assets`, `tags`, `tags_categories`, `products`
- `settings`, `fb_ad_libraries`

### Security notes and recommendations
- Static API token: Replace `STATIC_TOKEN` with a secret sourced from `process.env`.
- Sentry DSN: Move DSN to an env var and rotate the value.
- Formula execution: `parseFormulaOld` uses `new Function`. Prefer the safe acorn-based compiler already included; avoid evaluating untrusted formulas.
- Input validation: Validate and sanitize request body fields (dates, IDs) to reduce risk of injection and logic errors.
- Least privilege: Ensure AWS and MongoDB credentials follow least-privilege principles.

### Troubleshooting
- Athena polling never finishes: Verify `ATHENA_DATABASE` and `ATHENA_OUTPUT_LOCATION` (S3 permissions).
- Empty imports: Ensure date ranges and permissions on the Facebook token. Inspect `facebook_imports` collection for status and errors.
- Rate limiting: The client retries with backoff; consider longer backoff windows for high-volume imports.

### License
ISC (see `package.json`).