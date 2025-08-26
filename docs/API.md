# API Reference

All requests must include the `Authorization` header with the static token configured in `index.js`.

Header:
```
Authorization: KV5NfjBPaN9JDWqbDXrjQGoyeMtQWyfG16nTHmUPXFw=
Content-Type: application/json
```

### POST /run-task
Kicks off an Ads Insights import + processing job in the background.

Request body (JSON):
- `start_date` (string, YYYY-MM-DD) required
- `end_date` (string, YYYY-MM-DD) required
- `fbAccessToken` (string) required — Facebook Graph API token
- `FBadAccountId` (string) required — e.g., `act_1234567890`
- `agencyId` (string, Mongo ObjectId) required
- `clientId` (string, Mongo ObjectId) required
- `userId` (string, Mongo ObjectId) required
- `importListName` (string) optional
- `uuid` (string) required — correlation ID for status tracking
- `ad_objective_id` (string) required — name for the result field (e.g., `purchases`)
- `ad_objective_field_expr` (string) required — path expression like `actions.purchase`
- `ai` (boolean|string) optional — enable tagging via Flux API

Response:
- `200 OK` — `{ success: true, message: 'Task has been queued for processing' }`
- `400 Bad Request` — missing required parameters
- `401 Unauthorized` — invalid/missing Authorization header

Example:
```bash
curl -X POST http://localhost:3000/run-task \
  -H 'Authorization: KV5NfjBPaN9JDWqbDXrjQGoyeMtQWyfG16nTHmUPXFw=' \
  -H 'Content-Type: application/json' \
  -d '{
    "start_date": "2025-03-10",
    "end_date": "2025-04-10",
    "fbAccessToken": "FACEBOOK_GRAPH_ACCESS_TOKEN",
    "FBadAccountId": "act_70970029",
    "agencyId": "6656208cdb5d669b53cc98c5",
    "clientId": "67d306be742ef319388d07d1",
    "userId": "67d30...",
    "importListName": "FB Import Mar10-Apr10",
    "uuid": "1b2f3c4d-...",
    "ad_objective_id": "purchases",
    "ad_objective_field_expr": "actions.purchase",
    "ai": true
  }'
```

Progress and status:
- Check collection `facebook_imports` by `uuid` for `status`, `percentage`, and counts

---

### POST /run-ad-library
Imports Facebook Ad Library entries for given pages/date range in the background.

Request body (JSON):
- `start_date` (string, YYYY-MM-DD) required
- `end_date` (string, YYYY-MM-DD) required
- `fbAccessToken` (string) required
- `FBadAccountId` (string) required — used only for status/logging
- `agencyId` (string, Mongo ObjectId) required
- `clientId` (string, Mongo ObjectId) required
- `userId` (string, Mongo ObjectId) required
- `importListName` (string) optional
- `uuid` (string) required
- `search_page_ids` (string) optional — e.g., "['98269389167']"
- `max_count` (number) optional — default 50

Response:
- `200 OK` — `{ success: true, message: 'Task has been queued for processing' }`
- `400 Bad Request`, `401 Unauthorized` as above

Example:
```bash
curl -X POST http://localhost:3000/run-ad-library \
  -H 'Authorization: KV5NfjBPaN9JDWqbDXrjQGoyeMtQWyfG16nTHmUPXFw=' \
  -H 'Content-Type: application/json' \
  -d '{
    "start_date": "2025-03-10",
    "end_date": "2025-04-10",
    "fbAccessToken": "FACEBOOK_GRAPH_ACCESS_TOKEN",
    "FBadAccountId": "act_70970029",
    "agencyId": "6656208cdb5d669b53cc98c5",
    "clientId": "67d306be742ef319388d07d1",
    "userId": "67d30...",
    "uuid": "1b2f3c4d-...",
    "search_page_ids": "['98269389167']",
    "max_count": 100
  }'
```

---

### Status tracking
- `facebook_imports`: documents keyed by `uuid` with `status`, `percentage`, `insights_count`, `ads_count`, and `status_history`.

### Errors
- Sentry is enabled; errors are captured server-side.
- The final Express error handler returns a 500 with the Sentry event ID.