import {findDocuments, findOneDocument, updateOneDocument} from "./mongodb.js";
import {ObjectId} from "mongodb";
import {saveFacebookImportStatus} from "./common.js";
import axios from "axios";

const BASE_URL = "https://graph.facebook.com/v22.0";
const FIELDS = [
    "account_currency",
    "account_id",
    "account_name",
    "action_values",
    "actions",
    "ad_click_actions",
    "ad_id",
    "ad_impression_actions",
    "ad_name",
    "adset_id",
    "adset_name",
    "attribution_setting",
    "auction_bid",
    "auction_competitiveness",
    "auction_max_competitor_bid",
    "buying_type",
    "campaign_id",
    "campaign_name",
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    "catalog_segment_actions",
    "catalog_segment_value",
    "catalog_segment_value_mobile_purchase_roas",
    "catalog_segment_value_omni_purchase_roas",
    "catalog_segment_value_website_purchase_roas",
    "clicks",
    "conversion_values",
    "conversions",
    "converted_product_quantity",
    "converted_product_value",
    "cost_per_2_sec_continuous_video_view",
    "cost_per_15_sec_video_view",
    "cost_per_action_type",
    "cost_per_ad_click",
    "cost_per_conversion",
    "cost_per_dda_countby_convs",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_lead",
    "cost_per_one_thousand_ad_impression",
    "cost_per_outbound_click",
    "cost_per_thruplay",
    "cost_per_unique_action_type",
    "cost_per_unique_click",
    "cost_per_unique_conversion",
    "cost_per_unique_inline_link_click",
    "cost_per_unique_outbound_click",
    "cpc",
    "cpm",
    "cpp",
    "created_time",
    "ctr",
    "date_start",
    "date_stop",
    "dda_countby_convs",
    "dda_results",
    "frequency",
    "full_view_impressions",
    "full_view_reach",
    "impressions",
    "inline_link_click_ctr",
    "inline_link_clicks",
    "inline_post_engagement",
    "instagram_upcoming_event_reminders_set",
    "instant_experience_clicks_to_open",
    "instant_experience_clicks_to_start",
    "instant_experience_outbound_clicks",
    "interactive_component_tap",
    "marketing_messages_delivery_rate",
    "mobile_app_purchase_roas",
    "objective",
    "optimization_goal",
    "outbound_clicks",
    "outbound_clicks_ctr",
    "place_page_name",
    "purchase_roas",
    "qualifying_question_qualify_answer_rate",
    "reach",
    "social_spend",
    "spend",
    "updated_time",
    "video_30_sec_watched_actions",
    "video_avg_time_watched_actions",
    "video_continuous_2_sec_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_p100_watched_actions",
    "video_play_actions",
    "video_play_curve_actions",
    "video_play_retention_20_to_60s_actions",
    "video_play_retention_0_to_15s_actions",
    "video_play_retention_graph_actions",
    "video_thruplay_watched_actions",
    "video_time_watched_actions",
    "website_ctr",
    "website_purchase_roas",
].join(",");

function sendAlert(message) {
    console.log(`⚠️ ALERT: ${message}`);
}

async function sendHttpRequest({url, method = 'GET', headers = {}, body = null, timeout = 180000}) {
    const maxAttempts = 8; // Maximum retry attempts
    let attempt = 0;

    while (attempt < maxAttempts) {
        try {
            // Log the attempt
            // console.log(`Attempt ${attempt + 1}: Sending ${method} request to ${url}`);

            // Send the HTTP request
            const response = await axios({
                url,
                method,
                headers,
                data: body,
                timeout,
            });

            // Check the response status
            if (response.status === 200) {
                // Validate the response data
                if (method === "POST") {
                    const invalidItems = response.data.filter(item => item?.code !== 200);
                    if (invalidItems.length > 0) {
                        throw new Error(JSON.stringify(response.data));
                    }
                }
                // console.log('Request succeeded:', response.data);
                return response.data; // Return the valid response data
            }

            throw new Error('Unexpected response status!');
        } catch (error) {
            attempt++;

            // Log the error and retry if attempts remain
            console.error(`Error during attempt ${attempt}: ${error.message}`);

            if (attempt < maxAttempts) {
                const delay = (attempt + 1) * 2000; // Dynamic backoff delay
                console.warn(`Retrying in ${delay / 1000} seconds...`);

                // Send an alert on the third attempt or beyond
                if (attempt >= 3) {
                    sendAlert(`Request rate limit encountered. Attempt ${attempt}. Retrying in ${delay / 1000} seconds.`);
                }

                // Wait before retrying
                await new Promise(resolve => setTimeout(resolve, delay));
            } else {
                // Log and send a final alert if max attempts are reached
                console.error('Max retry attempts reached. Unable to fetch data.');
                sendAlert('Maximum retry attempts reached. Please investigate API limits.');
            }
        }
    }

    // If the function exits the loop, return null (indicates failure)
    return null;
}

function convertListsToDict(data) {
    if (typeof data !== 'object' || data === null) {
        // Return the data as-is if it's not an object or is null
        return data;
    }

    for (const [key, value] of Object.entries(data)) {
        if (Array.isArray(value)) {
            if (value.every(item => typeof item === 'object' && item !== null && 'action_type' in item && 'value' in item)) {
                data[key] = value.reduce((acc, item) => {
                    acc[item.action_type.replace(".", "_")] = Array.isArray(item.value)
                        ? item.value
                        : parseFloat(item.value);
                    return acc;
                }, {});
            } else {
                data[key] = value.map(item =>
                    typeof item === 'object' && item !== null
                        ? convertListsToDict(item)
                        : item
                );
            }
        } else if (typeof value === 'object' && value !== null) {
            data[key] = convertListsToDict(value);
        } else if (typeof value === 'string') {
            if (!key.includes('_id') && !isNaN(value)) {
                data[key] = value.includes('.') ? parseFloat(value) : parseInt(value, 10);
            } else {
                try {
                    const parsedDate = new Date(value);
                    if (!isNaN(parsedDate)) {
                        data[key] = parsedDate;
                    }
                } catch (e) {
                    // Ignore parsing errors
                }
            }
        }
    }
    return data;
}

const fetchAds = async (url, fbAccessToken) => {
    try {
        return await sendHttpRequest({
            url,
            method: "GET",
            headers: {
                "Authorization": `Bearer ${fbAccessToken}`,
                "Content-Type": "gzip"
            }
        });
    } catch (error) {
        console.error(`Error: ${error}`);
        console.error(`Error fetching ads: ${error.response?.status}`);
        return null;
    }
};
const fetchBatchData = async (batchRequests, fbAccessToken) => {
    try {
        return await sendHttpRequest({
            url: BASE_URL,
            method: "POST",
            headers: {
                "Authorization": `Bearer ${fbAccessToken}`,
                "Content-Type": "application/json",
            },
            body: {batch: batchRequests}
        });
    } catch
        (error) {
        console.log(error.response, batchRequests)
        console.error(`Error in batch request: ${error.response?.status}`);
        return null;
    }
};
export const getAdsInsights = async (accountId, fbAccessToken, start_date, end_date, uuid) => {
    const adsUrl = `${BASE_URL}/${accountId}/insights?level=ad&fields=ad_id&limit=50&action_breakdowns=action_type&time_range={"since":"${start_date}","until":"${end_date}"}`;
    let insights = [];
    let nextPage = adsUrl;

    while (nextPage) {
        const adsResponse = await fetchAds(nextPage, fbAccessToken);
        if (!adsResponse) break;

        const adData = adsResponse?.data || [];
        const adIds = adData.map((ad) => ad.ad_id);

        const insightsBatchRequests = adIds.map((adId) => ({
            method: "GET",
            relative_url: `${adId}/insights?level=ad&fields=${FIELDS}&time_range={"since":"${start_date}","until":"${end_date}"}`,
        }));

        const adDetailBatchRequests = adIds.map((adId) => ({
            method: "GET",
            relative_url: `${adId}?fields=status,creative{id,name,video_id,object_id,product_data,product_set_id,object_story_id,effective_object_story_id,object_story_spec,object_store_url,object_type,thumbnail_id,destination_set_id,instagram_permalink_url,link_og_id,link_url,object_url},source_ad_id,name,preview_shareable_link`,
        }));

        const insightsBatchResponse = (await fetchBatchData(insightsBatchRequests, fbAccessToken)) || [];
        const adDetailBatchResponse = (await fetchBatchData(adDetailBatchRequests, fbAccessToken)) || [];
        const adDetailBatch = {};
        adDetailBatchResponse.forEach((item) => {
            if (item.body) {
                const bodyData = JSON.parse(item.body);
                adDetailBatch[bodyData.id] = bodyData;
            }
        });

        if (insightsBatchResponse && adDetailBatchResponse) {
            insightsBatchResponse.forEach((result) => {
                if (result.body) {
                    const insightData = convertListsToDict(JSON.parse(result.body)?.data?.[0]);
                    const creativeData = adDetailBatch[insightData.ad_id]?.creative || {};
                    const status = adDetailBatch[insightData.ad_id]?.status || {};
                    const post_url = creativeData.effective_object_story_id
                        ? `https://www.facebook.com/${creativeData.effective_object_story_id}`
                        : null;

                    // Extract product link from creative
                    insights.push({
                        ...insightData,
                        creative: creativeData,
                        status,
                        post_url,
                        format: creativeData?.object_type || null,
                    });
                }
            });
            await saveFacebookImportStatus(uuid, {
                insights_count: insights.length
            })
        }

        nextPage = adsResponse.paging?.next;
    }

    return insights;
};

async function getFbAdPreview(adId, fbGraphToken) {
    const url = `${BASE_URL}/${adId}/previews?ad_format=MOBILE_FEED_STANDARD`;
    const headers = {
        "Authorization": `Bearer ${fbGraphToken}`,
        "Content-Type": "application/json",
    };

    try {
        const response = await axios.get(url, {headers});
        const preview = response.data;
        if (preview && preview.data && preview.data.length > 0) {
            const body = preview.data[0].body || "";
            const match = body.match(/src="([^"]+)"/);
            if (match) {
                return match[1].replace("amp;", "");
            }
        }
    } catch (error) {
        console.error("Error fetching FB ad preview:", error);
    }
    return null;
}

async function getSource(url, post = null) {
    const headers = {
        "sec-fetch-user": "?1",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-site": "none",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "cache-control": "max-age=0",
        "upgrade-insecure-requests": "1",
        "accept-language": "en-GB,en;q=0.9",
        "sec-ch-ua": `"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"`,
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    };

    try {
        let response;
        if (post) {
            response = await axios.post(url, post, {headers});
        } else {
            response = await axios.get(url, {headers});
        }
        return response.data;
    } catch (error) {
        console.error("Request failed:", error);
        return null;
    }
}

function extractAndDecode(linkUrl) {
    const prefix = "https://l.facebook.com/l.php?u=";
    if (linkUrl) {
        if (linkUrl.startsWith(prefix)) {
            // Remove the prefix
            const remaining = linkUrl.slice(prefix.length);
            // Find the position of the first '&' that indicates the end of the URL parameter
            const endIndex = remaining.indexOf("&");
            const encodedUrl = endIndex !== -1 ? remaining.slice(0, endIndex) : remaining;
            // Decode the URL
            return decodeURIComponent(encodedUrl);
        }
    }
    return linkUrl;
}

export function removeUTM(url) {
    try {
        let urlObj = new URL(url);
        // Force the protocol to be HTTPS
        urlObj.protocol = 'https:';
        let params = new URLSearchParams(urlObj.search);
        // List of UTM parameters to remove
        const utmParams = ['ad_id', 'utm_term', 'fb_campaign_id', 'hsa_grp', 'hsa_ad', 'utm_medium',
            'utm_source', 'utm_placement', 'msclkid', 'campaign_id', 'utm_campaign_group', 'placement',
            'utm_marpipe_id', 'utm_social-type', 'tw_adid', 'utm_variant', 'utm_fbid', 'utm_campaign_id',
            'hsa_mt', 'device', 'twclid', 'utm_device', 'gclid', 'utm_ad_id', 'hsa_net', 'hsa_src', 'utm_location',
            'tw_source', 'utm_adset', 'utm_test', 'campaignid', 'utm_platform', 'hsa_cam', 'fb_ad_id', 'yclid',
            'utm_camp_id', 'fbclid', 'utm_adset_id', 'utm_campaign', 'fb_action_types', 'utm_referrer',
            'utm_source_platform', 'utm_content_id', 'fb_action_ids', 'fb_ref', 'fbadid', 'st-t', 'hsa_tgt',
            'utm_creative_id', 'utm_feed', 'utm_creative', 'hsa_acc', 'dclid', 'utm_ad', 'hsa_kw', 'hsa_ver',
            'ttclid', 'utm_content_type', 'utm_social', 'utm_creative_format', 'fb_source', 'fb_page_id',
            'fb_adgroup_id', 'utm_content', 'adgroupid']
        ;

        utmParams.forEach(param => params.delete(param));

        // Construct the new URL without UTM parameters
        urlObj.search = params.toString();
        return urlObj.toString();
    } catch (error) {
        return null;
    }
}

async function getPropsOfSource(url) {
    const source = await getSource(url);
    if (source) {
        // Use the s flag so that . matches newline characters.
        const pattern = /"props":\s*(.*?)\s*,\s*"placeholderElement":/s;
        const match = source.match(pattern);
        if (match) {
            const capturedText = match[1];
            let previewData;
            try {
                previewData = JSON.parse(capturedText);
            } catch (e) {
                previewData = {};
            }
            const product_link =
                previewData.attachmentsData?.[0]?.attachmentDataList?.[0]?.navigation?.link_url;
            const message = previewData.messageData?.message;
            let productLink = extractAndDecode(product_link) || "";
            return {
                preview_data: previewData || {},
                product_link: productLink,
                product_url: removeUTM(productLink),
                message: message || "",
            };
        }
    }
    return {message: "", product_link: "", product_url: null, preview_data: {}};
}

export async function updateMessagesAndLinks(uuid, clientId) {
    // Retrieve the client document using the provided clientId.
    const client = await findOneDocument("clients", {_id: clientId});
    const accessToken = client.fb_config?.access_token || {};

    // Find assets where ad_id exists and both message and product_link in fb_data do not exist.
    const assets = await findDocuments(
        "assets",
        {
            client_id: clientId,
            ad_id: {$exists: true},
            "meta_data.fb_data.message": {$exists: false},
            "meta_data.fb_data.product_link": {$exists: false},
        },
        {_id: 1, ad_id: 1}
    );
    let startProgress = 20;
    const endProgress = 50;
    const totalTasks = assets.length;
    const progressIncrement = (endProgress - startProgress) / totalTasks;
    let currentProgress = startProgress;
    for (const asset of assets) {
        // Retrieve the Facebook ad preview URL using the asset's ad_id.
        const url = await getFbAdPreview(asset.ad_id, accessToken);
        const props = await getPropsOfSource(url);

        // Update the asset document with the fetched message, product_link, and preview_data.
        await updateOneDocument(
            "assets",
            {_id: new ObjectId(asset._id)},
            {
                $set: {
                    "meta_data.fb_data": {
                        message: props.message,
                        product_link: props.product_link,
                        product_url: props.product_url,
                        preview_data: props.preview_data,
                    },
                },
            }
        );
        currentProgress += progressIncrement;
        await saveFacebookImportStatus(uuid, {
            percentage: currentProgress
        })
    }
}