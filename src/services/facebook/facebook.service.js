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
  const insightsIndexUrl = `${BASE_URL}/${accountId}/insights?level=ad&fields=ad_id&limit=50&time_range={"since":"${start_date}","until":"${end_date}"}`;
  const insights = [];
  const seen = Object.create(null);
  let nextPage = insightsIndexUrl;
  let page = 0;
  logProgress('fb.insights.list.start', { accountId, start_date, end_date }, { uuid });
  while (nextPage) {
    const listResponse = await fetchAds(nextPage, fbAccessToken);
    if (!listResponse) break;
    page += 1;
    const adIds = (listResponse?.data || []).map((ad) => ad.ad_id);
    logProgress('fb.insights.list.page', { page, ad_count: adIds.length }, { uuid });
    if (adIds.length === 0) { nextPage = listResponse.paging?.next; continue; }
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
    adDetailBatchResponse.forEach((item, idx) => {
      if (item?.code && item.code >= 400) logProgress('fb.batch.detail.error', { code: item.code, idx }, { uuid });
      if (item?.body) {
        try { const bodyData = JSON.parse(item.body); adDetailById[bodyData.id] = bodyData; }
        catch (e) { logProgress('fb.batch.detail.parse_error', { idx, error: String(e?.message || e) }, { uuid }); }
      } else {
        logProgress('fb.batch.detail.missing_body', { idx }, { uuid });
      }
    });
    insightsBatchResponse.forEach((result, idx) => {
      if (result?.code && result.code >= 400) logProgress('fb.batch.insights.error', { code: result.code, idx }, { uuid });
      if (!result?.body) { logProgress('fb.batch.insights.missing_body', { idx }, { uuid }); return; }
      try {
        const parsed = JSON.parse(result.body);
        const insightData = parsed?.data?.[0];
        if (!insightData) return;
        const normalized = convertListsToDict({ ...insightData });
        const adId = normalized.ad_id;
        const creativeData = adDetailById[adId]?.creative || {};
        const status = adDetailById[adId]?.status || {};
        const post_url = creativeData.effective_object_story_id ? `https://www.facebook.com/${creativeData.effective_object_story_id}` : null;
        insights.push({ ...normalized, creative: creativeData, status, post_url, format: creativeData?.object_type || null });
        seen[adId] = true;
      } catch (e) {
        logProgress('fb.batch.insights.parse_error', { idx, error: String(e?.message || e) }, { uuid });
      }
    });
    await saveFacebookImportStatus(uuid, { insights_count: insights.length });
    logProgress('fb.insights.page.done', { page, cumulative: insights.length }, { uuid });
    nextPage = listResponse.paging?.next;
  }
  // Second pass: include ACTIVE ads with zero activity in-range (synthetic zeros)
  const adsUrl = `${BASE_URL}/${accountId}/ads?fields=id,name,effective_status,status&effective_status=ACTIVE&limit=50`;
  let nextAds = adsUrl; let adsPages = 0;
  while (nextAds) {
    const adsResponse = await fetchAds(nextAds, fbAccessToken);
    if (!adsResponse) break;
    adsPages += 1;
    const pageAds = (adsResponse?.data || []);
    const missingIds = pageAds.map(a => a.id).filter((id) => !seen[id]);
    logProgress('fb.ads.edge.page', { ads_page: adsPages, page_count: pageAds.length, missing_for_zero: missingIds.length }, { uuid });
    if (missingIds.length > 0) {
      const detailReqs = missingIds.map((adId) => ({
        method: 'GET',
        relative_url: `${adId}?fields=status,effective_status,creative{id,name,video_id,object_id,product_data,product_set_id,object_story_id,effective_object_story_id,object_story_spec,object_store_url,object_type,thumbnail_id,destination_set_id,instagram_permalink_url,link_og_id,link_url,object_url},source_ad_id,name,preview_shareable_link`,
      }));
      const details = (await fetchBatchData(detailReqs, fbAccessToken)) || [];
      details.forEach((item, idx) => {
        if (item?.code && item.code >= 400) logProgress('fb.batch.detail.error', { code: item.code, idx, phase: 'zeros' }, { uuid });
        if (!item?.body) { logProgress('fb.batch.detail.missing_body', { idx, phase: 'zeros' }, { uuid }); return; }
        try {
          const det = JSON.parse(item.body);
          const adId = det?.id || missingIds[idx];
          const creativeData = det?.creative || {};
          const post_url = creativeData?.effective_object_story_id ? `https://www.facebook.com/${creativeData.effective_object_story_id}` : null;
          const synthetic = convertListsToDict({ ad_id: adId, ad_name: det?.name || `ad_${adId}`, impressions: 0, reach: 0, spend: 0, ctr: 0, cpm: 0, inline_link_clicks: 0, actions: { link_click: 0, video_view: 0 }, video_thruplay_watched_actions: { video_view: 0 } });
          insights.push({ ...synthetic, creative: creativeData, status: det?.status || {}, post_url, format: creativeData?.object_type || null });
          logProgress('fb.synthetic.zero', { ad_id: adId }, { uuid });
          seen[adId] = true;
        } catch (e) {
          logProgress('fb.batch.detail.parse_error', { idx, phase: 'zeros', error: String(e?.message || e) }, { uuid });
        }
      });
    }
    nextAds = adsResponse.paging?.next;
  }
  logProgress('fb.insights.list.done', { total_insights: insights.length, pages: page, ads_pages: adsPages }, { uuid });
  return insights;
}


