import axios from 'axios';

const BASE_URL = 'https://graph.facebook.com/v22.0';

export async function getFbAdPreview(adId, fbGraphToken) {
  const url = `${BASE_URL}/${adId}/previews?ad_format=MOBILE_FEED_STANDARD`;
  const headers = { Authorization: `Bearer ${fbGraphToken}`, 'Content-Type': 'application/json' };
  try {
    const response = await axios.get(url, { headers });
    const preview = response.data;
    if (preview?.data?.length > 0) {
      const body = preview.data[0].body || '';
      const match = body.match(/src="([^"]+)"/);
      if (match) return match[1].replace('amp;', '');
    }
  } catch (e) {
    console.error('Error fetching FB ad preview:', e);
  }
  return null;
}

export async function getSource(url, post = null) {
  const headers = {
    'sec-fetch-user': '?1', 'sec-ch-ua-mobile': '?0', 'sec-fetch-site': 'none', 'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate', 'cache-control': 'max-age=0', 'upgrade-insecure-requests': '1', 'accept-language': 'en-GB,en;q=0.9',
    'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
  };
  try {
    const response = post ? await axios.post(url, post, { headers }) : await axios.get(url, { headers });
    return response.data;
  } catch (e) {
    console.error('Request failed:', e);
    return null;
  }
}

function extractAndDecode(linkUrl) {
  const prefix = 'https://l.facebook.com/l.php?u=';
  if (linkUrl && linkUrl.startsWith(prefix)) {
    const remaining = linkUrl.slice(prefix.length);
    const endIndex = remaining.indexOf('&');
    const encodedUrl = endIndex !== -1 ? remaining.slice(0, endIndex) : remaining;
    return decodeURIComponent(encodedUrl);
  }
  return linkUrl;
}

export function removeUTM(url) {
  try {
    const urlObj = new URL(url);
    urlObj.protocol = 'https:';
    const params = new URLSearchParams(urlObj.search);
    const utmParams = ['ad_id','utm_term','fb_campaign_id','hsa_grp','hsa_ad','utm_medium','utm_source','utm_placement','msclkid','campaign_id','utm_campaign_group','placement','utm_marpipe_id','utm_social-type','tw_adid','utm_variant','utm_fbid','utm_campaign_id','hsa_mt','device','twclid','utm_device','gclid','utm_ad_id','hsa_net','hsa_src','utm_location','tw_source','utm_adset','utm_test','campaignid','utm_platform','hsa_cam','fb_ad_id','yclid','utm_camp_id','fbclid','utm_adset_id','utm_campaign','fb_action_types','utm_referrer','utm_source_platform','utm_content_id','fb_action_ids','fb_ref','fbadid','st-t','hsa_tgt','utm_creative_id','utm_feed','utm_creative','hsa_acc','dclid','utm_ad','hsa_kw','hsa_ver','ttclid','utm_content_type','utm_social','utm_creative_format','fb_source','fb_page_id','fb_adgroup_id','utm_content','adgroupid'];
    utmParams.forEach(p => params.delete(p));
    urlObj.search = params.toString();
    return urlObj.toString();
  } catch { return null; }
}

export async function getPropsOfSource(url) {
  const source = await getSource(url);
  if (!source) return { message: '', product_link: '', product_url: null, preview_data: {} };
  const pattern = /"props":\s*(.*?)\s*,\s*"placeholderElement":/s;
  const match = source.match(pattern);
  if (!match) return { message: '', product_link: '', product_url: null, preview_data: {} };
  let previewData; try { previewData = JSON.parse(match[1]); } catch { previewData = {}; }
  const product_link = previewData.attachmentsData?.[0]?.attachmentDataList?.[0]?.navigation?.link_url;
  const message = previewData.messageData?.message;
  const productLink = extractAndDecode(product_link) || '';
  return { preview_data: previewData || {}, product_link: productLink, product_url: removeUTM(productLink), message: message || '' };
}


