## fb_insight

An Express.js API for importing and processing Facebook Ads data. It fetches ad-level insights and Ad Library entries from the Facebook Graph API, stores and transforms data in MongoDB, optionally merges AWS Athena results for selected accounts, and provides hooks to external services for tagging and enrichment.

### Features
- Fetch Facebook Ads insights and creatives via Graph API v22.0
- Fetch Facebook Ad Library entries for specified pages and dates
- Persist raw and processed data in MongoDB (insights, metrics, reports, assets, etc.)
- Optional Athena query execution and result merge for allowlisted accounts
- Pluggable tagging/enrichment through an external API
- Sentry error monitoring

### Architecture (high level)
- Client calls Nginx over HTTPS → Nginx forwards to Node/Express
- Express exposes JSON endpoints (e.g., POST `/run-task`)
- Data sources: Facebook Graph API, AWS Athena (optional)
- Storage: MongoDB
- Observability: Sentry

## Getting started

### Prerequisites
- Node.js 18+
- MongoDB instance (Atlas or self-hosted)
- Facebook Marketing API access token(s)
- Optional: AWS credentials and Athena configuration

### Installation
```bash
npm install
```

### Environment variables
Create a `.env` file in the project root with the following keys:

- `mongodb_uri` (required): MongoDB connection string.
- `PORT` (optional): Port for the Express server. Defaults to `3000`.
- `FLUX_STATIC_API_KEY` (optional): API key for the external tagging service.
- `OPENAI_API_KEY` (optional): API key used for content extraction/tagging flows.
- `AWS_REGION` (optional): AWS region for Athena; defaults to `us-east-1` if not set.
- `AWS_ACCESS_KEY_ID` (optional): AWS access key for Athena queries.
- `AWS_SECRET_ACCESS_KEY` (optional): AWS secret key for Athena queries.
- `ATHENA_DATABASE` (optional): Athena database name.
- `ATHENA_OUTPUT_LOCATION` (optional): S3 path for Athena query results (e.g., `s3://bucket/prefix/`).

Example `.env`:
```bash
mongodb_uri=mongodb+srv://user:pass@cluster/dbname
PORT=3000
FLUX_STATIC_API_KEY=your-key
OPENAI_API_KEY=your-openai-key
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
ATHENA_DATABASE=marketing
ATHENA_OUTPUT_LOCATION=s3://your-bucket/athena/results/
```

### Run locally
```bash
npm start
```
The server logs: `API listening on <PORT>`.

## Running behind Nginx (recommended)

Express is configured to trust the proxy (`app.set('trust proxy', 1)`), so it respects `X-Forwarded-*` headers. A minimal Nginx config that terminates TLS and forwards the `Authorization` header:

```nginx
upstream fb_insight_upstream {
    server 127.0.0.1:3000; # or your configured PORT
}

server {
    listen 443 ssl http2;
    server_name flux.afarin.top; # replace with your domain

    # ssl_certificate /path/to/fullchain.pem;
    # ssl_certificate_key /path/to/privkey.pem;

    location / {
        proxy_pass http://fb_insight_upstream;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Authorization $http_authorization; # forward bearer token
    }
}
```

With Nginx in front, call the service via your HTTPS domain (no port needed), e.g. `https://flux.afarin.top/run-task`.

## API

### POST `/run-task`
Triggers import of Facebook Ads insights for the given date range and account. The request is acknowledged immediately and the work runs in the background.

Headers:
- `Authorization: Bearer <token>` (required). Note: the service currently checks only for presence; implement your own allowlist or validation as needed.

Body (JSON):
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
  "uuid": "<job-uuid>"
}
```

Responses:
- `200 OK`: `{ "success": true, "message": "Task has been queued for processing" }`
- `400 Bad Request`: Missing required parameters
- `401 Unauthorized`: Missing `Authorization` header

Notes:
- The service persists raw insights and processed results to MongoDB collections (e.g., `fb_insights`, `metrics`, `reports_data`, `imported_lists`, etc.).
- For selected accounts, Athena results may be queried and merged into additional collections (e.g., `athena_result`, `merged_results`).

### POST `/run-ad-library` (optional)
Fetches Facebook Ad Library entries for pages/date range. This route may still use an internal static token middleware. If enabled in your deployment, send the same `Authorization` header as above.

Example body:
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

## Security
- Put the service behind Nginx with TLS.
- Forward and validate an `Authorization` header (implement a proper allowlist/verification).
- Store credentials and tokens only in environment variables or secrets managers.

## Troubleshooting
- MongoDB connection issues: verify `mongodb_uri` and network access.
- Missing headers: ensure Nginx forwards `Authorization` and `X-Forwarded-*` headers.
- Athena failures: verify AWS creds, region, database, and S3 output location.
- Facebook API errors: confirm `fbAccessToken` permissions and date ranges.

## Scripts
- `npm start` — start the API (`node index.js`)

## License
ISC


