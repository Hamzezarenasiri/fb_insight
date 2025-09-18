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
  const adsUrl = `${BASE_URL}/${accountId}/insights?level=ad&fields=ad_id&limit=50&action_breakdowns=action_type&time_range={"since":"${start_date}","until":"${end_date}"}`;
  const insights = [];
  let nextPage = adsUrl;
  let page = 0;
  logProgress('fb.insights.list.start', { accountId, start_date, end_date }, { uuid });
  while (nextPage) {
    const adsResponse = await fetchAds(nextPage, fbAccessToken);
    if (!adsResponse) break;
    page += 1;
    const adIds = (adsResponse?.data || []).map((ad) => ad.ad_id);
    logProgress('fb.insights.list.page', { page, ad_count: adIds.length }, { uuid });
    const insightsBatchRequests = adIds.map((adId) => ({
      method: 'GET',
      relative_url: `${adId}/insights?level=ad&fields=${FIELDS}&time_range={"since":"${start_date}","until":"${end_date}"}`,
    }));
    const adDetailBatchRequests = adIds.map((adId) => ({
      method: 'GET',
      relative_url: `${adId}?fields=status,creative{id,name,video_id,object_id,product_data,product_set_id,object_story_id,effective_object_story_id,object_story_spec,object_store_url,object_type,thumbnail_id,destination_set_id,instagram_permalink_url,link_og_id,link_url,object_url},source_ad_id,name,preview_shareable_link`,
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
      insightsBatchResponse.forEach((result) => {
        if (!result.body) return;
        const insightData = JSON.parse(result.body)?.data?.[0];
        if (!insightData) return;
        // Normalize arrays of { action_type, value } to dictionaries and coerce numeric strings/dates
        const normalized = convertListsToDict({ ...insightData });
        const creativeData = adDetailById[insightData.ad_id]?.creative || {};
        const status = adDetailById[insightData.ad_id]?.status || {};
        const post_url = creativeData.effective_object_story_id ? `https://www.facebook.com/${creativeData.effective_object_story_id}` : null;
        insights.push({ ...normalized, creative: creativeData, status, post_url, format: creativeData?.object_type || null });
      });
      await saveFacebookImportStatus(uuid, { insights_count: insights.length });
      logProgress('fb.insights.page.done', { page, cumulative: insights.length }, { uuid });
    }
    nextPage = adsResponse.paging?.next;
  }
  logProgress('fb.insights.list.done', { total_insights: insights.length, pages: page }, { uuid });
  return insights;
}


