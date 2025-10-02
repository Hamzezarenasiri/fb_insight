import { sendHttpRequest } from '../../utils/http.js';
import { logProgress } from '../../utils/logger.js';

const DEFAULT_BASE_URL = 'https://services.leadconnectorhq.com';
const DEFAULT_VERSION = '2021-07-28';
const SEARCH_PATH = '/opportunities/search';
const DEFAULT_LIMIT = 100;
const MAX_RETRIES = 5;
const BASE_BACKOFF_MS = 1500;

const PIPELINE_STAGE_GROUPS = {
  appts: new Set([
    '0b622041-6c40-4df1-9e9c-fe65aea86813',
    '36e41659-4363-4f73-92ed-8a962f7f70fc',
    'ede9d2c2-7860-48e4-91b0-584a415e0546',
    'fea4f73c-77d0-4c5b-8bcb-62d0f482c148',
    '31151b8d-5d8c-4dd6-a302-df90d13e7892',
    'da25f024-f84e-40f7-a914-0a1cdd348046',
    '8a16a399-e073-43fd-abef-4071ae15e246',
    '568f828a-7ca2-4244-a762-c446f2e95cce',
    'd4d1908e-2182-48c4-ac4d-2aeb1353254f',
    '35ba5a54-ac4a-4c2c-97ca-b01230ac27bf',
    'd849b36a-df3c-469d-9227-90df7d5bc1b5',
  ].map((id) => id.toLowerCase())),
  noresp: new Set([
    'f4957a29-d67b-4e79-87f9-42eb00bf5f59',
    '54c68766-b3fc-4927-811b-182fd1d4aec6',
  ].map((id) => id.toLowerCase())),
  show: new Set([
    '31151b8d-5d8c-4dd6-a302-df90d13e7892',
    'da25f024-f84e-40f7-a914-0a1cdd348046',
    '8a16a399-e073-43fd-abef-4071ae15e246',
    '568f828a-7ca2-4244-a762-c446f2e95cce',
    'd4d1908e-2182-48c4-ac4d-2aeb1353254f',
    '35ba5a54-ac4a-4c2c-97ca-b01230ac27bf',
    'd849b36a-df3c-469d-9227-90df7d5bc1b5',
  ].map((id) => id.toLowerCase())),
  noshow: new Set([
    'fea4f73c-77d0-4c5b-8bcb-62d0f482c148',
  ].map((id) => id.toLowerCase())),
  sched: new Set([
    '8a16a399-e073-43fd-abef-4071ae15e246',
    '568f828a-7ca2-4244-a762-c446f2e95cce',
    'd4d1908e-2182-48c4-ac4d-2aeb1353254f',
    '35ba5a54-ac4a-4c2c-97ca-b01230ac27bf',
    'd849b36a-df3c-469d-9227-90df7d5bc1b5',
  ].map((id) => id.toLowerCase())),
  surgcancel: new Set([
    'd4d1908e-2182-48c4-ac4d-2aeb1353254f',
  ].map((id) => id.toLowerCase())),
  sold: new Set([
    '35ba5a54-ac4a-4c2c-97ca-b01230ac27bf',
  ].map((id) => id.toLowerCase())),
  nosurgsold: new Set([
    'd849b36a-df3c-469d-9227-90df7d5bc1b5',
  ].map((id) => id.toLowerCase())),
};
const PIPELINE_STAGE_ENTRIES = Object.entries(PIPELINE_STAGE_GROUPS);

function normalizeAccountKey(accountId = '') {
  return accountId.toUpperCase().replace(/[^A-Z0-9]/g, '_');
}

function formatDateForGhl(isoDate) {
  if (!isoDate) return isoDate;
  const parsed = new Date(isoDate);
  if (Number.isNaN(parsed.valueOf())) return isoDate;
  return parsed.toISOString().slice(0, 10);
}

function normalizeStageId(value) {
  if (!value) return null;
  return String(value).trim().toLowerCase();
}

function extractPipelineStageId(opportunity) {
  return (
    normalizeStageId(opportunity?.pipelineStageId) ||
    normalizeStageId(opportunity?.pipeline_stage_id) ||
    normalizeStageId(opportunity?.stageId) ||
    normalizeStageId(opportunity?.stage_id)
  );
}

function createEmptyMetrics() {
  return {
    lead: 0,
    appts: 0,
    noresp: 0,
    show: 0,
    noshow: 0,
    sched: 0,
    surgcancel: 0,
    sold: 0,
    nosurgsold: 0,
  };
}

function getMetricsEntry(map, adId) {
  if (!map[adId]) {
    map[adId] = createEmptyMetrics();
  }
  return map[adId];
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function requestGhlPage({ url, headers, attempt = 1, ctx }) {
  try {
    return await sendHttpRequest({ url, method: 'GET', headers });
  } catch (error) {
    const status = error?.response?.status || error?.statusCode || error?.status;
    const retryable = status === 429 || (status >= 500 && status < 600);
    if (retryable && attempt < MAX_RETRIES) {
      const backoff = BASE_BACKOFF_MS * 2 ** (attempt - 1);
      logProgress('ghl.fetch.retry', { attempt, status, backoff }, ctx);
      await delay(backoff);
      return requestGhlPage({ url, headers, attempt: attempt + 1, ctx });
    }
    logProgress('ghl.fetch.error', { attempt, status, error: String(error?.message || error) }, ctx);
    throw error;
  }
}

export function resolveGhlCredentials(accountId) {
  const key = normalizeAccountKey(accountId);
  const token = process.env[`GHL_TOKEN_${key}`] || process.env.GHL_API_TOKEN || process.env.GHL_AUTH_TOKEN;
  const locationId = process.env[`GHL_LOCATION_${key}`] || process.env.GHL_LOCATION_ID;
  const baseUrl = process.env.GHL_API_BASE || DEFAULT_BASE_URL;
  const version = process.env.GHL_API_VERSION || DEFAULT_VERSION;
  const limit = Number(process.env.GHL_PAGE_LIMIT) || DEFAULT_LIMIT;

  if (!token || !locationId) return null;
  return { token, locationId, baseUrl, version, limit };
}

function extractAttributionAdIds(opportunity) {
  const attributions = Array.isArray(opportunity?.attributions) ? opportunity.attributions : [];
  const ids = new Set();
  for (const attribution of attributions) {
    const utmAdId = attribution?.utmAdId || attribution?.utm_ad_id || attribution?.ad_id || attribution?.utm_adid;
    if (utmAdId) ids.add(String(utmAdId));
  }
  return ids;
}

export async function fetchLeadCountsFromGhl({ accountId, startDate, endDate, ctx = {} }) {
  const credentials = resolveGhlCredentials(accountId);
  if (!credentials) {
    logProgress('ghl.missing.credentials', { accountId }, ctx);
    return null;
  }

  const { token, locationId, baseUrl, version, limit } = credentials;
  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const formattedStart = formatDateForGhl(startDate);
  const formattedEnd = formatDateForGhl(endDate);
  const paramsBase = {
    location_id: locationId,
    limit: String(limit),
    date: formattedStart,
    startDate: formattedStart,
    start_date: formattedStart,
    endDate: formattedEnd,
    end_date: formattedEnd,
  };

  let page = 1;
  let totalFetched = 0;
  const leadCounts = Object.create(null);

  logProgress('ghl.fetch.start', { accountId, startDate, endDate }, ctx);

  let expectedPages = null;
  const dedupeIds = new Set();

  while (true) {
    const params = new URLSearchParams({ ...paramsBase, page: String(page) });
    const url = `${normalizedBase}${SEARCH_PATH}?${params.toString()}`;
    const headers = {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json',
      Version: version,
    };
    const data = await requestGhlPage({ url, headers, ctx });

    const opportunities = Array.isArray(data?.opportunities) ? data.opportunities : [];
    logProgress('ghl.fetch.page', { page, fetched: opportunities.length }, ctx);
    totalFetched += opportunities.length;
    if (expectedPages === null) {
      const metaTotal = Number(data?.meta?.total ?? data?.meta?.count ?? data?.total ?? 0);
      if (Number.isFinite(metaTotal) && metaTotal > 0) {
        expectedPages = Math.ceil(metaTotal / limit) || null;
      }
    }

    for (const opportunity of opportunities) {
      const opportunityId = opportunity?.id || opportunity?._id;
      if (opportunityId) {
        const seenKey = String(opportunityId);
        if (dedupeIds.has(seenKey)) continue;
        dedupeIds.add(seenKey);
      }
      const ids = extractAttributionAdIds(opportunity);
      if (ids.size === 0) continue;
      const stageId = extractPipelineStageId(opportunity);
      ids.forEach((adId) => {
        const metrics = getMetricsEntry(leadCounts, adId);
        metrics.lead += 1;
        if (!stageId) return;
        for (const [metric, stages] of PIPELINE_STAGE_ENTRIES) {
          if (stages.has(stageId)) {
            metrics[metric] += 1;
          }
        }
      });
    }

    const processedPages = page;
    if ((opportunities.length < limit && expectedPages === null) || (expectedPages && processedPages >= expectedPages)) {
      break;
    }
    page += 1;
  }

  const totals = Object.values(leadCounts).reduce((acc, metrics) => {
    Object.entries(metrics).forEach(([key, value]) => {
      acc[key] = (acc[key] || 0) + value;
    });
    return acc;
  }, {});

  logProgress('ghl.fetch.complete', { pages: page, totalFetched, totals }, ctx);
  return leadCounts;
}

