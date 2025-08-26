# Architecture

### Overview
Single-process Express app exposing two endpoints that start background jobs. Jobs fetch data from Facebook APIs (and optionally AWS Athena), transform and normalize the results into a canonical schema, and persist output to MongoDB. Additional enrichment pulls message/link metadata and generates products/tags via OpenAI and Flux services.

### Modules (within `index.js`)
- Sentry init and config
- Environment/config and constants (BASE_URL, Flux API, STATIC_TOKEN)
- MongoDB helpers: `connectToCollection`, `findDocuments`, `insertMany`, `updateOneDocument`, etc.
- HTTP helpers: `sendHttpRequest` with retries/backoff, `fetchAds`, `fetchBatchData`
- Athena: `runAthenaQuery` with status polling and `transformAthenaResult`
- Facebook import logic: `getAdsInsights`, `getAdsLibrary`
- Data normalization: `convertListsToDict`, `convertToObject`, `findNonEmptyKeys`, `transformObjects`
- Matching/mapping: Jaro-Winkler distance, `findMostSimilarKey`
- Metric computation: acorn-based safe `compileFormula` and fallbacks (`parseFormulaOld`)
- Post-processing: `processRow`, `processData`, normalization helpers
- Asset enrichment: `getFbAdPreview`, `getSource`, `getPropsOfSource`, `removeUTM`
- Product/tag generation: `generateProduct` (OpenAI + Mongo aggregation), `tagging` (Flux API)
- Status tracking: `saveFacebookImportStatus`
- Background orchestration: `mainTask` (insights), `adLibraryTask` (ad library)
- Express endpoints: `/run-task`, `/run-ad-library`

### Data flow: `/run-task`
1. Validate input; enqueue `mainTask`
2. Save initial status to `facebook_imports`
3. Fetch ad IDs and batch insights + ad details from Graph API
4. For specific accounts, aggregate Athena and merge by `ad_name` prefix code
5. Convert raw records to canonical objects (`convertToObject`) using `ad_objective_field_expr`
6. Infer header-to-schema mapping via similarity and build `formData`
7. Create `imported_lists` document capturing schema metadata
8. Compute metrics (safe acorn compiler preferred), normalize numbers/percentages
9. Upsert/update `assets`; insert computed `metrics`
10. Create `reports_data` and default `sub_reports` if none exist
11. Enrich assets with message/product links and preview data
12. Generate products and consolidate tags; optionally call Flux tagging
13. Update status to success/failure

### Data flow: `/run-ad-library`
1. Validate input; enqueue `adLibraryTask`
2. Save initial status to `facebook_imports`
3. Paginate Ad Library API until `max_count` or no next page
4. Insert rows into `fb_ad_libraries`
5. Update status

### Collections and key fields
- `facebook_imports`: `{ uuid, status, percentage, ... }`
- `fb_insights`: Raw insight rows (+ uuid)
- `athena_result`: Aggregated external metrics (+ uuid)
- `merged_results`: Merge of insights and Athena (+ uuid)
- `imported_lists`: Schema snapshot and human-readable field config
- `metrics`: Final normalized metrics per ad; links to `assets`
- `assets`: Creative metadata, message/product links, mapping to ad names
- `reports_data`, `sub_reports`: Report scaffolding
- `tags`, `tags_categories`, `products`: Tag vocabulary and product discovery
- `settings`: Holds prompts and configuration (e.g., `extractProductPrompt`)
- `fb_ad_libraries`: Imported Ad Library records

### Configuration
- Facebook Graph API version: `v22.0`
- Athena region and credentials from env; database and S3 output required
- Static auth token and Sentry DSN currently hardcoded in `index.js`

### Risks and improvements
- Replace hardcoded `STATIC_TOKEN` and Sentry DSN with env vars
- Validate body params (dates/object IDs) and limit ranges to protect upstream APIs
- Prefer the safe formula compiler; deprecate `parseFormulaOld` (`new Function`)
- Add request id logging with `uuid` for end-to-end traceability
- Add health and readiness endpoints; add a start script in `package.json`
- Add tests and schema validation for Mongo writes