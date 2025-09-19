import { sendHttpRequest } from '../../utils/http.js';
import { saveFacebookImportStatus } from '../status/status.service.js';
import { logProgress } from '../../utils/logger.js';

function convertListsToDict(data) {
  if (typeof data !== 'object' || data === null) return data;
  for (const [key, value] of Object.entries(data)) {
    if (Array.isArray(value)) {
      if (value.every(item => typeof item === 'object' && item !== null && 'action_type' in item && 'value' in item)) {
        data[key] = value.reduce((acc, item) => {
          acc[item.action_type.replace('.', '_')] = Array.isArray(item.value) ? item.value : parseFloat(item.value);
          return acc;
        }, {});
      } else {
        data[key] = value.map(item => (typeof item === 'object' && item !== null ? convertListsToDict(item) : item));
      }
    } else if (typeof value === 'object' && value !== null) {
      data[key] = convertListsToDict(value);
    } else if (typeof value === 'string') {
      if (!key.includes('_id') && !isNaN(value)) {
        data[key] = value.includes('.') ? parseFloat(value) : parseInt(value, 10);
      } else {
        try {
          const parsedDate = new Date(value);
          if (!isNaN(parsedDate)) data[key] = parsedDate;
        } catch {}
      }
    }
  }
  return data;
}

const BASE_URL = 'https://graph.facebook.com/v22.0';

export async function fetchAds(url, fbAccessToken) {
  return sendHttpRequest({
    url,
    method: 'GET',
    headers: { Authorization: `Bearer ${fbAccessToken}` },
  });
}

export async function fetchBatchData(batchRequests, fbAccessToken) {
  return sendHttpRequest({
    url: `${BASE_URL}/`,
    method: 'POST',
    headers: { Authorization: `Bearer ${fbAccessToken}`, 'Content-Type': 'application/json' },
    body: { batch: batchRequests },
  });
}

export async function getAdsInsights(accountId, fbAccessToken, start_date, end_date, uuid, FIELDS) {
  // List ads first so we can include ACTIVE ads with zero activity in the date range
  const adsUrl = `${BASE_URL}/${accountId}/ads?fields=id&limit=50`;
  const insights = [];
  let nextPage = adsUrl;
  let page = 0;
  logProgress('fb.insights.list.start', { accountId, start_date, end_date }, { uuid });
  while (nextPage) {
    const adsResponse = await fetchAds(nextPage, fbAccessToken);
    if (!adsResponse) break;
    page += 1;
    const adIds = (adsResponse?.data || []).map((ad) => ad.id);
    logProgress('fb.insights.list.page', { page, ad_count: adIds.length }, { uuid });
    const insightsBatchRequests = adIds.map((adId) => ({
      method: 'GET',
      relative_url: `${adId}/insights?level=ad&fields=${FIELDS}&time_range={"since":"${start_date}","until":"${end_date}"}`,
    }));
    const adDetailBatchRequests = adIds.map((adId) => ({
      method: 'GET',
      relative_url: `${adId}?fields=status,effective_status,creative{id,name,video_id,object_id,product_data,product_set_id,object_story_id,effective_object_story_id,object_story_spec,object_store_url,object_type,thumbnail_id,destination_set_id,instagram_permalink_url,link_og_id,link_url,object_url},source_ad_id,name,preview_shareable_link`,
    }));
    const insightsBatchResponse = (await fetchBatchData(insightsBatchRequests, fbAccessToken)) || [];
    const adDetailBatchResponse = (await fetchBatchData(adDetailBatchRequests, fbAccessToken)) || [];
    const adDetailById = {};
    adDetailBatchResponse.forEach((item) => {
      if (item.body) {
        const bodyData = JSON.parse(item.body);
        adDetailById[bodyData.id] = bodyData;
      }
    });
    if (insightsBatchResponse && adDetailBatchResponse) {
      // Track which ads have insights; we'll backfill zero rows for those without
      const seen = Object.create(null);
      adIds.forEach((id) => { seen[id] = false; });
      insightsBatchResponse.forEach((result, idx) => {
        if (!result.body) return;
        const parsed = JSON.parse(result.body);
        const insightData = parsed?.data?.[0];
        if (insightData) {
          // Normalize arrays of { action_type, value } to dictionaries and coerce numeric strings/dates
          const normalized = convertListsToDict({ ...insightData });
          const adId = normalized.ad_id;
          const creativeData = adDetailById[adId]?.creative || {};
          const status = adDetailById[adId]?.status || {};
          const post_url = creativeData.effective_object_story_id ? `https://www.facebook.com/${creativeData.effective_object_story_id}` : null;
          insights.push({ ...normalized, creative: creativeData, status, post_url, format: creativeData?.object_type || null });
          if (adId in seen) seen[adId] = true;
        }
      });
      // For ads with no insights rows, synthesize a zero-metric record
      adIds.forEach((adId) => {
        if (!seen[adId]) {
          const det = adDetailById[adId] || {};
          const creativeData = det?.creative || {};
          const post_url = creativeData?.effective_object_story_id ? `https://www.facebook.com/${creativeData.effective_object_story_id}` : null;
          const synthetic = convertListsToDict({
            ad_id: adId,
            ad_name: det?.name || `ad_${adId}`,
            impressions: 0,
            reach: 0,
            spend: 0,
            ctr: 0,
            cpm: 0,
            inline_link_clicks: 0,
            actions: { link_click: 0, video_view: 0 },
            video_thruplay_watched_actions: { video_view: 0 },
          });
          insights.push({ ...synthetic, creative: creativeData, status: det?.status || {}, post_url, format: creativeData?.object_type || null });
        }
      });
      await saveFacebookImportStatus(uuid, { insights_count: insights.length });
      logProgress('fb.insights.page.done', { page, cumulative: insights.length }, { uuid });
    }
    nextPage = adsResponse.paging?.next;
  }
  logProgress('fb.insights.list.done', { total_insights: insights.length, pages: page }, { uuid });
  return insights;
}


