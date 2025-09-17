import { sendHttpRequest } from '../../utils/http.js';
import { saveFacebookImportStatus } from '../status/status.service.js';

const BASE_URL = 'https://graph.facebook.com/v22.0';

export async function getAdsLibrary(accountId, fbAccessToken, start_date, end_date, uuid, search_page_ids = "['98269389167']", max_count = 50) {
  const adsUrl = `${BASE_URL}/ads_archive?access_token=${fbAccessToken}&ad_type=ALL&ad_reached_countries=['US']&search_page_ids=${search_page_ids}&ad_delivery_date_min=${start_date}&ad_delivery_date_max=${end_date}&ad_active_status=ALL&fields=id,page_name,ad_snapshot_url,ad_delivery_start_time,ad_delivery_stop_time`;
  let ads = [];
  let nextPage = adsUrl;
  while (nextPage && ads.length < max_count) {
    const adsResponse = await sendHttpRequest({ url: nextPage, method: 'GET' });
    if (!adsResponse) break;
    const adData = adsResponse?.data || [];
    ads.push(...adData);
    nextPage = adsResponse.paging?.next;
    await saveFacebookImportStatus(uuid, { adlibrary_count: ads.length });
  }
  return ads.slice(0, max_count);
}


