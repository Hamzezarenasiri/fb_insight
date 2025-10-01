import { sendHttpRequest } from '../../utils/http.js';
import { logProgress } from '../../utils/logger.js';

const DEFAULT_BASE_URL = 'https://services.leadconnectorhq.com';
const DEFAULT_VERSION = '2021-07-28';
const SEARCH_PATH = '/opportunities/search';
const DEFAULT_LIMIT = 100;

function normalizeAccountKey(accountId = '') {
  return accountId.toUpperCase().replace(/[^A-Z0-9]/g, '_');
}

function formatDateForGhl(isoDate) {
  if (!isoDate) return isoDate;
  const [year, month, day] = isoDate.split('-');
  if (!year || !month || !day) return isoDate;
  return `${month}-${day}-${year}`;
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
  const paramsBase = {
    location_id: locationId,
    limit: String(limit),
    date: formatDateForGhl(startDate),
    endDate: formatDateForGhl(endDate),
  };

  let page = 1;
  let totalFetched = 0;
  const leadCounts = Object.create(null);

  logProgress('ghl.fetch.start', { accountId, startDate, endDate }, ctx);

  while (true) {
    const params = new URLSearchParams({ ...paramsBase, page: String(page) });
    const url = `${normalizedBase}${SEARCH_PATH}?${params.toString()}`;
    let data;
    try {
      data = await sendHttpRequest({
        url,
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: 'application/json',
          Version: version,
        },
      });
    } catch (error) {
      logProgress('ghl.fetch.error', { page, error: String(error?.message || error) }, ctx);
      throw error;
    }

    const opportunities = Array.isArray(data?.opportunities) ? data.opportunities : [];
    logProgress('ghl.fetch.page', { page, fetched: opportunities.length }, ctx);
    totalFetched += opportunities.length;

    for (const opportunity of opportunities) {
      const ids = extractAttributionAdIds(opportunity);
      if (ids.size === 0) continue;
      ids.forEach((adId) => {
        leadCounts[adId] = (leadCounts[adId] || 0) + 1;
      });
    }

    if (opportunities.length < limit) break;
    page += 1;
  }

  logProgress('ghl.fetch.complete', { pages: page, totalFetched }, ctx);
  return leadCounts;
}

