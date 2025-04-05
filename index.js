import * as Sentry from "@sentry/node"
import dotenv from 'dotenv';
import express from 'express';
import axios from 'axios';
import {MongoClient, ObjectId} from 'mongodb';

Sentry.init({
    dsn: "https://a51aca261c977758f4342257034a5d59@o1178736.ingest.us.sentry.io/4508958246043648",
});
dotenv.config();
const uri = process.env.mongodb_uri;
const BASE_URL = "https://graph.facebook.com/v22.0";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY
const client = new MongoClient(uri, {
    family: 4  // Force IPv4
});
const dbName = 'FluxDB';
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
let schema = [
    {
        "key": "Ad_Name",
        "title": "Ad Name",
        "type": "string",
        "required": true,
        "description": "Ad Name refers to the name or title given to the advertisement.",
        "is_default": true,
        "similar_dictionary": [
            "Ad name",
            "file name",
            "Ad Name",
            "Ad_Name",
            "advertisement name",
            "advert name",
            "video title",
            "video name",
            "adname",
            "ad name"
        ],
        "order_preference": "decs",
        "format": "text",
        "formula": "N/A"
    },
    {
        "key": "spend",
        "title": "Spend",
        "type": "float",
        "required": true,
        "description": "Spend is the total amount of money spent on your ad campaign.",
        "is_default": true,
        "similar_dictionary": [
            "Amount",
            "Amount spent (USD)",
            "spent",
            "ad spend",
            "total spend",
            "budget spent",
            "cost",
            "campaign spend",
            "spend",
            "Spend"
        ],
        "order_preference": "decs",
        "format": "currency",
        "formula": "N/A"
    },
    {
        "key": "aov",
        "title": "AOV",
        "type": "float",
        "required": false,
        "description": "AOV stands for Average Order Value, which is the average amount spent each time a customer places an order.",
        "is_default": true,
        "similar_dictionary": ["Average Order Value", "aov", "avg order value", "order value average", "average purchase value"],
        "order_preference": "decs",
        "format": "currency",
        "formula": "revenue / purchases"
    },
    {
        "key": "cpa",
        "title": "CPA",
        "type": "float",
        "required": false,
        "description": "Cost Per Acquisition",
        "is_default": false,
        "similar_dictionary": ["Cost Per Acquisition", "cpa"],
        "order_preference": "acs",
        "format": "currency",
        "formula": "spend / purchases"
    },
    {
        "key": "cpc",
        "title": "CPC",
        "type": "float",
        "required": false,
        "description": "CPC is the cost per click, which is the amount you pay each time someone clicks on your ad.",
        "is_default": true,
        "similar_dictionary": [
            "CPC (all) (USD)",
            "cpc",
            "cost per click",
            "click cost",
            "cost per tap",
            "click price",
            "cost per link click"
        ],
        "order_preference": "acs",
        "format": "currency",
        "formula": "spend / link_clicks"
    },
    {
        "key": "cpm",
        "title": "CPM",
        "type": "float",
        "required": false,
        "description": "CPM is the cost per thousand impressions, indicating the amount spent per thousand times your ad is shown.",
        "is_default": true,
        "similar_dictionary": [
            "CPM (cost per 1,000 impressions) (USD)",
            "cpm",
            "cost per mille",
            "cost per thousand impressions",
            "impression cost",
            "cost per mille impressions"
        ],
        "order_preference": "acs",
        "format": "currency",
        "formula": "(spend / impressions) * 1000"
    },
    {
        "key": "ctr",
        "title": "CTR",
        "type": "float",
        "required": false,
        "description": "CTR is the number of clicks that your ad receives divided by the number of times your ad is shown: clicks ÷ impressions = CTR",
        "is_default": true,
        "similar_dictionary": ["CTR", "Ctr", "Clickthrough rate", "Clickthrough_rate"],
        "order_preference": "decs",
        "format": "percent",
        "formula": "link_clicks / impressions"
    },
    {
        "key": "hold",
        "title": "Hold",
        "type": "float",
        "required": false,
        "description": "Ratio of 15-second video views to impressions",
        "is_default": true,
        "similar_dictionary": [
            "Hold",
            "%hold",
            "hold%",
            "ad hold",
            "hold status",
            "pause",
            "stopped"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "video_views_15s / impressions"
    },
    {
        "key": "impressions",
        "title": "Impressions",
        "type": "integer",
        "required": false,
        "description": "Impressions are the number of times your ad is shown.",
        "is_default": true,
        "similar_dictionary": ["Impressions", "ad impressions", "total impressions", "views", "impression count"],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "roas",
        "title": "ROAS",
        "type": "float",
        "required": false,
        "description": "ROAS is the return on ad spend, measuring the revenue generated for every dollar spent on advertising.",
        "is_default": true,
        "similar_dictionary": [
            "Purchase ROAS (return on ad spend)",
            "roas",
            "return on ad spend",
            "purchase return",
            "ad ROI",
            "revenue on ad spend"
        ],
        "order_preference": "decs",
        "format": "number",
        "formula": "revenue / spend"
    },
    {
        "key": "lead",
        "title": "LEADS",
        "type": "integer",
        "required": false,
        "description": "Number of leads generated",
        "is_default": true,
        "similar_dictionary": ["Leads"],
        "order_preference": "asc",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "purchases",
        "title": "Purchases",
        "type": "integer",
        "required": false,
        "description": "Purchases indicate the total number of transactions or sales generated from your ad.",
        "is_default": true,
        "similar_dictionary": [
            "Purchases",
            "purchase count",
            "number of purchases",
            "total purchases",
            "sales",
            "transactions"
        ],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "revenue",
        "title": "Revenue",
        "type": "float",
        "required": false,
        "description": "Revenue is the total amount of income generated from sales or services before any expenses are deducted.",
        "is_default": true,
        "similar_dictionary": [
            "total revenue",
            "income",
            "sales revenue",
            "gross revenue",
            "revenue (USD)",
            "earnings"
        ],
        "order_preference": "decs",
        "format": "currency",
        "formula": "N/A"
    },
    {
        "key": "vvr",
        "title": "VVR",
        "type": "float",
        "required": false,
        "description": "Video View Rate (VVR) is the percentage of people who viewed your video ad after it was served.",
        "is_default": true,
        "similar_dictionary": [
            "vvr",
            "vvr%",
            "%vvr",
            "Video View Rate",
            "view rate",
            "video views rate",
            "watch rate",
            "play rate"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "video_views_3s / impressions"
    },
    {
        "key": "video_views_15s",
        "title": "Video Views (15s)",
        "type": "integer",
        "required": false,
        "description": "Video views (15s) represent the number of times a video has been watched for at least 15 seconds.",
        "is_default": true,
        "similar_dictionary": [
            "Video Views (15s)",
            "video_views_15s",
            "15-second views",
            "views 15s",
            "video impressions 15s",
            "15s video plays",
            "watch count 15s",
            "view count 15s"
        ],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_views_3s",
        "title": "Video Views (3s)",
        "type": "integer",
        "required": false,
        "description": "Video views (3s) represent the number of times a video has been watched for at least 3 seconds.",
        "is_default": true,
        "similar_dictionary": [
            "video_views_3s",
            "Video Views (3s)",
            "video impressions 3s",
            "views",
            "video impressions",
            "play count",
            "video plays",
            "watch count",
            "view count"
        ],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "link_clicks",
        "title": "Link Clicks",
        "type": "integer",
        "required": false,
        "description": "The total number of clicks on the link within your ad.",
        "is_default": true,
        "similar_dictionary": [
            "Link Clicks",
            "link clicks",
            "clicks on link",
            "total link clicks",
            "URL clicks",
            "link taps"
        ],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "cpl",
        "title": "CPL",
        "type": "float",
        "required": false,
        "description": "Cost Per Lead",
        "is_default": true,
        "similar_dictionary": [
            "Cost Per Lead",
            "Cost per lead",
            "cost per lead",
            "CPL",
            "cpl",
            "Cpl",
            "cPl",
            "cpL",
            "CPQL",
            "Cpql",
            "cpql"
        ],
        "order_preference": "acs",
        "format": "currency",
        "formula": "spend / lead"
    },
    {
        "key": "video_avg_time_watched",
        "is_default": true,
        "title": "Video Average Time Watched",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": ["video_avg_time_watched", "Video Average Time Watched", "Average Time", "Video Average"],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p25_watched_actions",
        "is_default": true,
        "title": "Video p25 watched actions",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": ["video_p25_watched_actions"],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p50_watched_actions",
        "is_default": true,
        "title": "Video p50 watched actions",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": ["video_p50_watched_actions"],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p75_watched_actions",
        "is_default": true,
        "title": "Video p75 watched actions",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": ["video_p75_watched_actions"],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p95_watched_actions",
        "is_default": true,
        "title": "Video p95 watched actions",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": ["video_p95_watched_actions"],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p100_watched_actions",
        "is_default": true,
        "title": "Video p100 watched actions",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": ["video_p100_watched_actions"],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    }
]

const app = express();
app.use(express.json());
// Static authentication token
const STATIC_TOKEN = 'KV5NfjBPaN9JDWqbDXrjQGoyeMtQWyfG16nTHmUPXFw='; // Replace with a secure, randomly generated token

// Authentication middleware
const authenticate = (req, res, next) => {
    const authToken = req.headers['authorization'];
    if (!authToken || authToken !== STATIC_TOKEN) {
        return res.status(401).send({success: false, message: 'Unauthorized'});
    }
    next();
};

// Background task wrapper
const runInBackground = (task, params) => {
    setTimeout(() => {
        task(params).catch(err => console.error('Background task failed:', err));
    }, 0); // Run immediately but asynchronously
};

async function connectToCollection(collectionName) {
    try {
        await client.connect();
        const database = client.db(dbName);
        return database.collection(collectionName);
    } catch (error) {
        console.error("Error connecting to MongoDB: ", error);
        throw error;
    }
}

async function findDocuments(collectionName, query, projection = {}, sort = {}) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.find(query, {projection}).sort(sort).toArray();
    } catch (error) {
        console.error("Error finding documents: ", error);
        throw error;
    }
}

async function insertMany(collectionName, documents) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.insertMany(documents);
    } catch (error) {
        console.error("Error inserting many documents: ", error);
        throw error;
    }
}

async function findOneDocument(collectionName, query, projection = {}) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.findOne(query, {projection});
    } catch (error) {
        console.error("Error finding one document: ", error);
        throw error;
    }
}

async function aggregateDocuments(collectionName, pipeline) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.aggregate(pipeline).toArray();
    } catch (error) {
        console.error("Error aggregating documents: ", error);
        throw error;
    }
}

async function updateOneDocument(collectionName, filter, update, options = {upsert: true}) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.updateOne(filter, update, options);
    } catch (error) {
        console.error("Error updating one document: ", error);
        throw error;
    }
}

async function updateManyDocuments(collectionName, filter, update) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.updateMany(filter, update);
    } catch (error) {
        console.error("Error updating many documents: ", error);
        throw error;
    }
}

async function insertOneDocument(collectionName, document) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.insertOne(document);
    } catch (error) {
        console.error("Error inserting one document: ", error);
        throw error;
    }
}

async function findAndUpdate(collectionName, filter, update, options = {}) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.findOneAndUpdate(filter, update, options);
    } catch (error) {
        console.error("Error finding and updating document: ", error);
        throw error;
    }
}

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
const getAdsInsights = async (accountId, fbAccessToken, start_date, end_date, uuid) => {
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
            relative_url: `${adId}/insights?level=ad&fields=${FIELDS}`,
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

function convertToObject(data, ad_objective_field_expr, ad_objective_id) {
    const expr = ad_objective_field_expr.split(".")
    return data.map((item) => {
        const {
            ad_name,
            impressions,
            reach,
            ctr,
            frequency,
            spend,
            cpp,
            cpm,
            post_url,
            ad_id,
            format,
            ...restOfItem
        } = item;

        return {
            Ad_Name: ad_name || "null_name",
            impressions: impressions || null,
            reach: reach || null,
            ctr: ctr || null,
            frequency: frequency || null,
            spend: spend || null,
            cpp: cpp || null,
            cpm: cpm || null,
            link_click: item.actions?.link_click || null,
            purchase: item.actions?.purchase || null,
            vvr: item.actions?.video_view / impressions || null,
            hold: item.video_thruplay_watched_actions?.video_view / impressions || null,
            cpa: item.cost_per_action_type?.purchase || null,
            // cvr: item?.[expr[0]]?.[expr[1]] / item.actions?.link_click || null,
            cvr: item.actions?.link_click ? (item?.[expr[0]]?.[expr[1]] ? item[expr[0]][expr[1]] / item.actions?.link_click : 0) : null,
            roas: item.purchase_roas?.omni_purchase || null,
            cpc: item.cost_per_action?.link_click || spend / item.actions?.link_click || null,
            cpl: item.cost_per_action?.lead || null,
            revenue: item.action_values?.purchase || null,
            video_view_3s: item.actions?.video_view || null,
            video_view_15s: item.video_thruplay_watched_actions?.video_view || null,
            video_avg_time_watched: item.video_avg_time_watched_actions?.video_view || null,
            video_p25_watched_actions: item.video_p25_watched_actions?.video_view || null,
            video_p50_watched_actions: item.video_p50_watched_actions?.video_view || null,
            video_p75_watched_actions: item.video_p75_watched_actions?.video_view || null,
            video_p95_watched_actions: item.video_p95_watched_actions?.video_view || null,
            video_p100_watched_actions: item.video_p100_watched_actions?.video_view || null,
            // [ad_objective_id] :  item?.[expr[0]]?.[expr[1]],
            result: item?.[expr[0]]?.[expr[1]],
            cpr: item?.[expr[0]]?.[expr[1]] ? spend / item[expr[0]][expr[1]] : Infinity,
            post_url,
            ad_id,
            format,
            thumbnail_url: item.creative?.thumbnail_url,
            other_fields: {
                ...restOfItem,
            },
        };
    });
}

function findNonEmptyKeys(array) {
    const keysWithValues = new Set();
    array.forEach(obj => {
        for (const [key, value] of Object.entries(obj)) {
            if (value !== null && value !== "") {
                keysWithValues.add(key);
            }
        }
    });
    return Array.from(keysWithValues);
}

function transformObjects(data) {
    return data.map(obj => ({
        [obj.key]: {
            key: obj.key,
            is_default: obj.is_default,
            title: obj.title,
            description: obj.description,
            required: obj.required,
            type: obj.type,
            format: obj.format || null,
            formula: obj.formula || null,
            similar_dictionary: obj.similar_dictionary || [] // Ensure similar_dictionary is initialized as an array
        }
    }));
}

function jaroWinklerDistance(s1, s2) {
    let m = 0;

    if (s1.length === 0 || s2.length === 0) return 0;
    if (s1 === s2) return 1;

    const range = Math.floor(Math.max(s1.length, s2.length) / 2) - 1;
    const s1Matches = new Array(s1.length);
    const s2Matches = new Array(s2.length);

    for (let i = 0; i < s1.length; i++) {
        const low = (i >= range) ? i - range : 0;
        const high = (i + range <= s2.length - 1) ? i + range : s2.length - 1;

        for (let j = low; j <= high; j++) {
            if (!s1Matches[i] && !s2Matches[j] && s1[i] === s2[j]) {
                m++;
                s1Matches[i] = s2Matches[j] = true;
                break;
            }
        }
    }

    if (m === 0) return 0;

    let k = 0;
    let numTrans = 0;
    for (let i = 0; i < s1.length; i++) {
        if (s1Matches[i]) {
            for (let j = k; j < s2.length; j++) {
                if (s2Matches[j]) {
                    k = j + 1;
                    break;
                }
            }
            if (s1[i] !== s2[k - 1]) numTrans++;
        }
    }

    let weight = (m / s1.length + m / s2.length + (m - (numTrans / 2)) / m) / 3;
    const l = Math.min(4, [...s1].findIndex((c, i) => c !== s2[i]) + 1);
    const p = 0.1;

    if (weight > 0.7) weight += l * p * (1 - weight);

    return weight;
}

function findMostSimilarKey(item, array1) {
    let maxSimilarity = -1;
    let mostSimilarKey = null;

    array1.forEach(obj => {
        const key = Object.keys(obj)[0];
        let similarity = 0;

        if (obj[key].similar_dictionary.length !== 0) {
            obj[key].similar_dictionary.forEach(similarItem => {
                similarity = Math.max(similarity, jaroWinklerDistance(item.toLowerCase(), similarItem.toLowerCase()));
            });
        } else {
            similarity = jaroWinklerDistance(item.toLowerCase(), key.toLowerCase());
        }

        if (similarity > maxSimilarity) {
            maxSimilarity = similarity;
            mostSimilarKey = key;
        }
    });

    return {key: mostSimilarKey, similarity: maxSimilarity};
}

function getPercentFields(arr) {
    return arr.filter(item => item.format === 'percent').map(item => item.key);
}

function parseFormula(formula) {
    const dependentFields = formula.match(/([a-zA-Z_]+)/g) || [];
    const formulaFunction = new Function(
        ...dependentFields,
        `return ${formula};`
    );
    return {
        dependentFields,
        formulaFunction
    };
}

function calculateMetrics(inputValues, metrics) {
    let calculatedValues = {...inputValues};

    const dependencies = {};
    metrics.forEach(metric => {
        if (metric.formula !== "N/A") {
            dependencies[metric.key] = parseFormula(metric.formula);
        }
    });

    let pending = true;
    let previousPendingCount = Object.keys(calculatedValues).length;

    while (pending) {
        pending = false;
        metrics.forEach(metric => {
            if (calculatedValues[metric.key] === undefined && dependencies[metric.key]) {
                const {dependentFields, formulaFunction} = dependencies[metric.key];
                const missingFields = dependentFields.filter(field => calculatedValues[field] === undefined);

                if (missingFields.length === 0) {
                    const result = formulaFunction(...dependentFields.map(field => calculatedValues[field]));
                    if (result !== null && !isNaN(result)) {
                        calculatedValues[metric.key] = result;
                    }
                    pending = true;
                }
            }
        });

        const currentPendingCount = Object.keys(calculatedValues).length;

        // Break the loop if no progress is made to prevent an infinite loop
        if (currentPendingCount === previousPendingCount) {
            break;
        }

        previousPendingCount = currentPendingCount;
    }

    // Filter out null or NaN values but retain original input fields
    Object.keys(calculatedValues).forEach(key => {
        if (calculatedValues[key] === null || isNaN(calculatedValues[key])) {
            // Only delete keys that were added during calculation, not the original input keys
            if (!inputValues.hasOwnProperty(key)) {
                delete calculatedValues[key];
            }
        }
    });

    return calculatedValues;
}

function cleanData(value, defaultValue = null) {
    if (!value || value === "") return defaultValue;
    return value.toString().replace(/[\$,%]/g, '');
}

function getFieldType(fieldKey) {
    const field = schema?.find(item => item.key === fieldKey);
    return field ? field.type : null;
}

function processRow(row, mappedColumns) {
    const newRow = {};
    Object.keys(mappedColumns).forEach(dbColumn => {
        const Header = mappedColumns[dbColumn];
        if (Header) {
            if (row.hasOwnProperty(Header)) {
                const fieldType = getFieldType(dbColumn);
                let cleanedData = cleanData(row[Header]);

                switch (fieldType) {
                    case 'integer':
                        newRow[dbColumn] = parseInt(cleanedData, 10) || 0;
                        break;
                    case 'float':
                        newRow[dbColumn] = parseFloat(cleanedData) || 0.0;
                        break;
                    case 'boolean':
                        newRow[dbColumn] = cleanedData.toLowerCase() === 'true';
                        break;
                    default:
                        newRow[dbColumn] = cleanedData;
                }
            } else {
                console.warn(`Missing Data for Header: ${Header}, intended for DB Column: ${dbColumn}`);
            }
        }
    });

    return newRow;
}

function processData(Data, mappedColumns, metrics, agencyId, clientId, userId, import_list_inserted) {
    return Data.map(row => {
        let newRow = processRow(row, mappedColumns);
        newRow = calculateMetrics(newRow, metrics);
        newRow.agency_id = agencyId;
        newRow.client_id = clientId;
        newRow.import_list_id = import_list_inserted.insertedId;
        newRow.user_id = userId;
        newRow.ad_id = row.ad_id;
        newRow.post_url = row.post_url;
        newRow.format = capitalizeFirstChar(row.format).replace("Photo", "Image").replace("Share", "Image");
        newRow.thumbnail_url = row.thumbnail_url;
        newRow.other_fields = row.other_fields;
        return newRow;
    });
}

const capitalizeFirstChar = str => str ? str[0].toUpperCase() + str.slice(1).toLowerCase() : "";

function NormalizeNumberObjects(dataArray, keysToCheck) {
    dataArray.forEach(obj => {
        keysToCheck.forEach(key => {
            if (obj.hasOwnProperty(key)) {
                let value = obj[key];
                if (typeof value === 'string') {
                    value = parseFloat(value);
                }
                obj[key] = value
            }
        });
    });
    return dataArray;
}

function detectAndNormalizePercentageInObjects(dataArray, keysToCheck) {
    // Determine if each key requires normalization
    const normalizationRequired = {};

    keysToCheck.forEach(key => {
        normalizationRequired[key] = false;
        for (const obj of dataArray) {
            if (obj.hasOwnProperty(key)) {
                let value = obj[key];
                if (typeof value === 'string') {
                    value = parseFloat(value);
                }
                if (typeof value === 'number' && !isNaN(value)) {
                    if (value > 1) {
                        normalizationRequired[key] = true;
                        break;
                    }
                }
            }
        }
    });

    // Normalize values if needed for each key
    dataArray.forEach(obj => {
        keysToCheck.forEach(key => {
            if (normalizationRequired[key] && obj.hasOwnProperty(key)) {
                let value = obj[key];
                if (typeof value === 'string') {
                    value = parseFloat(value);
                }
                if (typeof value === 'number' && !isNaN(value)) {
                    obj[key] = value / 100;
                }
            }
        });
    });

    return dataArray;
}

async function saveFacebookImportStatus(uuid, updateValues) {
    const collectionName = 'facebook_imports';
    const filter = {uuid};
    updateValues.updatedAt = new Date()
    const update = {
        $set: updateValues
    };

    try {
        const result = await updateOneDocument(collectionName, filter, update);
        console.log("Facebook import status saved successfully:", result);
    } catch (error) {
        console.error("Failed to save Facebook import status:", error);
    }
}

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

function removeUTM(url) {
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

async function updateMessagesAndLinks(uuid, clientId) {
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
    let startProgress = 30;
    const endProgress = 70;
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

async function generateProduct(uuid, clientId, agencyId) {
    let default_tags_categories = await findDocuments("tags_categories", {client_id: clientId})
    if (default_tags_categories.length === 0) {
        default_tags_categories = await findDocuments(
            "tags_categories",
            {client_id: "global", agency_id: "global"},
            {_id: 0, "client_id": clientId, "agency_id": agencyId, category: 1, description: 1}
        );
        await insertMany("tags_categories", default_tags_categories);
    }
    // let default_tags = await findDocuments("tags",{is_default:true,client_id:clientId})
    // if (default_tags.length === 0 ) {
    //     default_tags = await findDocuments("tags",{is_default:true,client_id:"global", agency_id : "global"},{_id:0});
    //     default_tags.forEach(tag => {
    //         tag.client_id = clientId;
    //         tag.agency_id = agencyId
    //     });
    //     const inserted_tags = await insertMany("tags",default_tags);
    //
    // }
    const assets_links = await aggregateDocuments("assets", [
        {
            $match: {
                "client_id": clientId,
                // "meta_tags.offer": {$exists: false},
                "meta_data.fb_data.product_url": {
                    $exists: true,
                    // "$ne": null,
                    "$ne": ""
                },
            }
        },
        {
            "$group": {
                "_id": "$meta_data.fb_data.product_url",
                "count": {"$sum": 1},
            }
        }, {$sort: {count: -1}}, {
            "$project": {
                url: "$_id", count: "$count", _id: 0,
//   asset_ids:1

            }
        }
    ])
    // Your prompt instructing the extraction details
    // console.log(assets_links,"<<<<<assets_links");
    const prompt_setting = await findOneDocument("settings", {"key": "extractProductPrompt"})
    const prompt = prompt_setting.promptTemplate;

    const tags = await aggregateDocuments("tags", [
        {"$match": {"client_id": clientId}},
        {
            "$group": {
                "_id": {"category": "$category"},
                "tags": {"$push": {"k": "$tag", "v": "$description"}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "category": "$_id.category",
                "tags": {"$arrayToObject": "$tags"},
            }
        },
        {
            "$group": {
                "_id": null,
                "categories": {"$push": {"k": "$category", "v": "$tags"}},
            }
        },
        {"$replaceRoot": {"newRoot": {"$arrayToObject": "$categories"}}},
    ])
    const categories = await aggregateDocuments(
        "tags_categories",
        [
            {"$match": {"client_id": clientId}},
            {"$sort": {"_id": 1}},
            {
                "$group": {
                    "_id": null,
                    "categoryDescriptions": {
                        "$push": {"k": "$category", "v": "$description"}
                    },
                }
            },
            {"$replaceRoot": {"newRoot": {"$arrayToObject": "$categoryDescriptions"}}},
        ],
    )
    const categories_val = categories[0];
    let tag_example = "{"
    Object.keys(categories_val).forEach((k) => {
        tag_example += `${k}:[{
            tag:  "${k}_tag_value1",
            tag_description: "${k}_tag_value1_description"
        },
        .
        .
        .
        ],`;
    });
    tag_example += "// Add additional categories as needed }"
    const joinedCategories = Object.keys(categories_val).join('|');

    // Function to split an array into chunks of a specified size
    function chunkArray(array, size) {
        const chunks = [];
        for (let i = 0; i < array.length; i += size) {
            chunks.push(array.slice(i, i + size));
        }
        return chunks;
    }

    // Function to process one chunk of URLs
    async function extractProductDetailsForChunk(chunk) {
        const prompt_code_part = `You are a Creative Director. Your task is to analyze the product information from the provided links and return detailed products information in a structured JSON format. Follow these instructions precisely:
    Visit and carefully review the content of the provided link.
    Extract the product name and a concise yet detailed product description.
    Identify relevant tags for the product according to the specified categories below. Use the provided Reference Tag Bank (listed below) to prioritize existing tags.
    Only create a new tag if no suitable existing tag from the Reference Tag Bank is found. If a new tag is necessary

tags categories:
    ${JSON.stringify(categories_val, null, 1)}

Reference Tag Bank (internal reference):
    ${JSON.stringify(tags, null, 1)}

urls:
    ${JSON.stringify(chunk.map(item => item.url), null, 1)}
    
description (briefly explain the meaning or context of the tag)
${prompt}
Important rules to follow:
    Any tag created that is not explicitly present in the Reference Tag Bank, including within categories ${joinedCategories}.
    Ensure accuracy, conciseness, and relevance in each tag and description.
    Generate tags using no more than three words unless explicitly permitted by the tag category description.

Your response must follow this exact JSON format: 
    [{
        "product_name": "Product Name Here (The name of the product)",
        "product_description": "Brief and clear product description goes here (A clear and concise one-line description that accurately defines the product's purpose or function.)",
        "landing_url:v"Product URL"
        "tags": ${tag_example},
    },
    .
    .
    .
    ]
Just return json and nothing else.
`;
        console.log(prompt_code_part,"<<<<<<<<prompt_code_part")
        const data = {
            model: prompt_setting.model,
            messages: [
                {role: "system", content: "You are a helpful assistant."},
                {
                    role: "user",
                    content: prompt_code_part
                }
            ],
            temperature: prompt_setting.temperature,
        };
        const response = await axios.post(
            'https://api.openai.com/v1/chat/completions',
            data,
            {
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${OPENAI_API_KEY}`,
                }
            }
        );
        // The API should return the structured JSON as text.

        let output = response.data.choices[0].message.content;
        // console.log(data,"<<<Data");
        // console.log(output,"<<<output");
        // console.log(output,"<<<<<<<<<<<<<<<output");
        output = output.replace(/^```json\s*/, '').replace(/\s*```$/, '').trim();

        // Remove trailing commas before closing braces or brackets and remove newlines
        const contentFixed = output.replace(/,\s*([\}\]])/g, '$1').replace(/\n/g, '');
        // console.log(contentFixed,"<<<contentFixed")
        // Assuming the output is valid JSON
        console.log(`result: ${contentFixed}`);
        return JSON.parse(contentFixed);

    }

    // Main function to process all chunks and accumulate the results
    async function extractAllProductDetails() {
        const chunkSize = 50;
        const chunks = chunkArray(assets_links, chunkSize);
        const allResults = [];
        let startProgress = 70;
        const endProgress = 89;
        const totalTasks = chunks.length;
        const progressIncrement = (endProgress - startProgress) / totalTasks;
        let currentProgress = startProgress;

        for (let i = 0; i < chunks.length; i++) {
            console.log(`Processing chunk ${i + 1} of ${chunks.length}`);
            const result = await extractProductDetailsForChunk(chunks[i]);

            // Merge the current chunk's result into the overall array
            allResults.push(...result);
            currentProgress += progressIncrement;
            await saveFacebookImportStatus(uuid, {
                percentage: currentProgress
            })

        }

        return allResults;
    }

    // Execute and print the final results
    const funnels = await extractAllProductDetails()
    for (let i = 0; i < funnels.length; i++) {
        const funnel = funnels[i]
        // for (let i = 0; i < funnel.tags.length; i++) {
        {
            for (const [key, value] of Object.entries(funnel.tags)) {
                // Wait for the asynchronous operation to complete before continuing
                for (let i = 0; i < value.length; i++) {
                    const update_result = await updateOneDocument("tags", {
                        "tag": value[i].tag,
                        category: key,
                        client_id: clientId,
                        agency_id: agencyId
                    }, {
                        $set: {
                            description: value[i].tag_description,
                            updated_by: "AI-Product",
                            updated_at: new Date()
                        }, $setOnInsert: {created_at: new Date(), created_by: "AI-Product"}
                    }, {upsert: true})
                }
            }
        }


        // }
    }
    let jackpot = await aggregateDocuments("tags", [
        {$match: {client_id: clientId}},
        {
            $group: {
                _id: "$category",
                ids: {$push: "$_id"}
            }
        },
        // Convert each grouped document into a key/value pair.
        {
            $project: {
                _id: 0,
                k: "$_id",
                v: "$ids"
            }
        },
        // Merge all key/value pairs into a single document.
        {
            $group: {
                _id: null,
                categories: {$push: {k: "$k", v: "$v"}}
            }
        },
        // Replace the root with the new document formed from the key/value pairs.
        {
            $replaceRoot: {newRoot: {$arrayToObject: "$categories"}}
        },
    ]);
    let startProgress = 90;
    const endProgress = 99;
    const totalTasks = funnels.length;
    const progressIncrement = (endProgress - startProgress) / totalTasks;
    let currentProgress = startProgress;
    for (let i = 0; i < funnels.length; i++) {
        const funnel = funnels[i]
        const funnelName = funnel.product_name.toLowerCase();
        await updateOneDocument("products",
            {
                funnel_name: funnelName,
                client_id: clientId,
                agency_id: agencyId,

            }, {
                $setOnInsert: {
                    created_at: new Date(),
                    funnel_form_data: {
                        landing_url: funnel.landing_url,
                        funnel_name: funnelName,
                        funnel_description: funnel.product_description
                    },
                    jackpot: jackpot[0] || {},
                    landing_url: funnel.landing_url,
                    funnel_description: funnel.funnel_description
                }
            }, {upsert: true});
        await updateOneDocument("tags", {
            "tag": funnelName,
            category: "offer",
            client_id: clientId,
            agency_id: agencyId
        }, {
            $set: {
                description: funnel.funnel_description,
            }, $setOnInsert: {created_at: new Date(), created_by: "AI"}
        }, {upsert: true})
        await updateManyDocuments("assets", {
            "client_id": clientId,
            "meta_tags.offer": {$exists: false},
            "meta_data.fb_data.product_url": funnel.landing_url
        }, {"$set": {"meta_tags.offer": funnelName}})
        currentProgress += progressIncrement;
        await saveFacebookImportStatus(uuid, {
            percentage: currentProgress
        })

    }

}

async function mainTask(params) {
    let {
        start_date,
        end_date,
        agencyId,
        clientId,
        userId,
        fbAccessToken,
        FBadAccountId,
        importListName,
        uuid,
        ad_objective_id,
        ad_objective_field_expr
    } = params;

    try {
        console.log("start ....", params)
        schema.push({
            "key": "cpr",
            "title": "CPR",
            "type": "float",
            "required": false,
            "description": "CPR is the cost per Result.",
            "is_default": true,
            "similar_dictionary": [
                "cpr",
                "cost per result",],
            "order_preference": "acs",
            "format": "currency",
            "formula": `spend / result`
            // "formula" : `(spend / ${ad_objective_id})`
        })
        schema.push({
            "key": "cvr",
            "title": "CVR",
            "type": "float",
            "required": false,
            "description": "CVR is the conversion rate, which is the percentage of users who completed a desired action after clicking on your ad.",
            "is_default": true,
            "similar_dictionary": [
                "Conversion Rate",
                "cvr",
                "conv rate",
                "conversions rate",
                "action rate",
                "goal rate"
            ],
            "order_preference": "decs",
            "format": "percent",
            "formula": "result / link_clicks"
        },)
        schema.push({
            "key": "result",
            "second_key": ad_objective_id,
            "title": ad_objective_id?.toUpperCase().replaceAll("_", " "),
            "type": "float",
            "required": false,
            "description": "Result.",
            "is_default": true,
            "order_preference": "decs",
            "format": "number",
            "formula": "N/A"
        })
        // schema.push({
        //     "key" : ad_objective_id,
        //     "title" : ad_objective_id?.toUpperCase().replaceAll("_"," "),
        //     "type" : "float",
        //     "required" : false,
        //     "description" : "",
        //     "is_default" : true,
        //     "order_preference" : "decs",
        //     "format" : "number",
        //     "formula" : "N/A"
        // })
        agencyId = new ObjectId(agencyId);
        clientId = new ObjectId(clientId);
        userId = new ObjectId(userId);
        await generateProduct(uuid, clientId, agencyId)
        return
        await saveFacebookImportStatus(uuid, {
            start_date,
            end_date,
            agency_id: agencyId,
            client_id: clientId,
            user_id: userId,
            fb_ad_account_id: FBadAccountId,
            import_list_name: importListName,
            status: "Importing from Facebook",
            percentage: -1,
            createdAt: new Date()
        })
        const metrics = await findDocuments("import_schema", {
            "type": {"$in": ["float", "integer"]},
            "formula": {
                "$exists": true,
                "$nin": [null, "", "N/A"]
            }
        })
        // const schema = (await findAndUpdate("defined_schemas", {
        //     agency_id: agencyId,
        //     client_id: clientId,
        //     in_edit: true
        // }, {
        //     "$set": {"updatedAt": new Date()}
        // }, {upsert: true})).schema || [];
        const MetricsIDs = (await aggregateDocuments("metrics", [
                {
                    $match: {
                        Ad_Name: {$exists: true, $ne: null},
                        asset_id: {$exists: true, $ne: null},
                        client_id: clientId
                    }
                },
                {
                    $project: {
                        keyValue: {k: "$Ad_Name", v: "$asset_id"}
                    }
                },
                {
                    $group: {
                        _id: null,
                        keyValues: {$push: "$keyValue"}
                    }
                },
                {
                    $replaceRoot: {
                        newRoot: {$arrayToObject: "$keyValues"}
                    }
                }
            ]))[0];
        console.log("Getting ads ... ")
        const results = await getAdsInsights(FBadAccountId, fbAccessToken, start_date, end_date, uuid)
        const ads = convertToObject(results, ad_objective_field_expr, ad_objective_id)
        const exist_fields = findNonEmptyKeys(ads)
        const Headers = exist_fields.filter(item => !["post_url", "other_fields", "ad_id", "thumbnail_url",].includes(item));
        const tableColumns = transformObjects(schema);
        const result = Headers.map(item => {
            const {key, similarity} = findMostSimilarKey(item, tableColumns);
            return {
                head: item,
                similar_obj: similarity > 0.75 ? tableColumns.find(obj => Object.keys(obj)[0] === key)[key] : "Exclude"
            };
        });
        const formData = {};
        result.forEach(mapping => {
            if (mapping.similar_obj !== "Exclude") {
                formData[mapping.similar_obj.key] = mapping.head;
            }
        });
        const last_imported_list = (await findDocuments("imported_lists", {
            client_id: clientId
        }, {}, {"createdAt": -1}))?.[0]
        const last_sub_reports = last_imported_list ? await findDocuments("sub_reports", {
            import_list_id: last_imported_list._id
        }, {html_note: 0}) : []
        const importListDocument = {
            date_range: `from:${start_date}-to:${end_date}`,
            start_date: new Date(start_date),
            end_date: new Date(end_date),
            name: importListName,
            agency_id: agencyId,
            client_id: clientId,
            createdAt: new Date(),
            fields:
                schema.reduce((acc, item) => {
                    acc[item.key] = {
                        "type": item.type,
                        "required": item.required,
                        "title": item.title,
                        "description": item.description,
                        "order_preference": item.order_preference,
                        "format": item.format || null,
                        "formula": item.formula || null,
                    };
                    return acc;
                }, {}),
            schema: [...new Set(schema)],
        };
        const import_list_inserted = await insertOneDocument("imported_lists", importListDocument);
        const newDataArray = processData(ads, formData, metrics, agencyId, clientId, userId, import_list_inserted);
        const PercentkeysToCheck = getPercentFields(metrics);
        const keysToCheck = await findDocuments("import_schema", {type: {$in: ["float", "integer"]}}, {key: 1, _id: 0});
        let res = NormalizeNumberObjects(newDataArray, keysToCheck);
        console.log("Validating Records ... ")
        let validatedRecords = detectAndNormalizePercentageInObjects(res, PercentkeysToCheck)
        const AssetsIds = await aggregateDocuments("assets", [
            {
                $match: {
                    client_id: clientId,
                    adname: {$exists: true, $ne: null},
                    _id: {$exists: true, $ne: null},
                }
            },
            {
                $project: {
                    keyValue: {k: "$adname", v: "$_id"}
                }
            },
            {
                $group: {
                    _id: null,
                    keyValues: {$push: "$keyValue"}
                }
            },
            {
                $replaceRoot: {
                    newRoot: {$arrayToObject: "$keyValues"}
                }
            }
        ]);
        let asset_ids = AssetsIds[0] || {}
        for (const entry of validatedRecords) {
            const creative = entry.other_fields ? entry.other_fields.creative : undefined;
            let product_link = null;
            let message = null;

            if (creative) {
                const objectStorySpec = creative.object_story_spec || {};
                product_link =
                    objectStorySpec.link_data?.link ||
                    objectStorySpec.video_data?.call_to_action?.value?.link ||
                    objectStorySpec.template_data?.link;
                message =
                    objectStorySpec.link_data?.message ||
                    objectStorySpec.video_data?.message ||
                    objectStorySpec.template_data?.message;
            }

            if (asset_ids?.[entry.Ad_Name] || MetricsIDs?.[entry.Ad_Name]) {
                entry.asset_id = asset_ids[entry.Ad_Name] || MetricsIDs?.[entry.Ad_Name]
                const set_dict = {
                    agency_id: agencyId,
                    client_id: clientId,
                    import_list_id: import_list_inserted.insertedId,
                    user_id: userId,
                    ad_id: entry.ad_id,
                    adname: entry.Ad_Name,
                    post_url: entry.post_url,
                    format: entry.format,
                    thumbnail_url: entry.thumbnail_url,
                    "meta_data.fb_data.creative": creative,
                };
                if (message) {
                    set_dict["meta_data.fb_data.message"] = message;
                }
                if (product_link) {
                    set_dict["meta_data.fb_data.product_link"] = product_link;
                    set_dict["meta_data.fb_data.product_url"] = removeUTM(product_link);
                }
                // remove this part of code when all asset updated
                await updateOneDocument("assets", {_id: new ObjectId(entry.asset_id)}, {$set: set_dict}
                )
            } else {
                try {
                    const fb_data = {creative};
                    entry.fb_data = fb_data;
                    if (message) {
                        fb_data.message = message;
                    }
                    if (product_link) {
                        fb_data.product_link = product_link;
                        fb_data.product_url = removeUTM(product_link);
                    }
                    const new_asset = await insertOneDocument("assets", {
                        agency_id: agencyId,
                        client_id: clientId,
                        import_list_id: import_list_inserted.insertedId,
                        user_id: userId,
                        ad_id: entry.ad_id,
                        adname: entry.Ad_Name,
                        post_url: entry.post_url,
                        format: entry.format,
                        thumbnail_url: entry.thumbnail_url,
                        meta_data: entry
                    });
                    entry.asset_id = new_asset.insertedId
                    asset_ids[entry.Ad_Name] = new_asset.insertedId
                } catch (error) {
                    console.error("Error inserting new asset:", error);
                }
            }
            entry.createdAt = new Date();
        }
        if (!validatedRecords || validatedRecords.length === 0) {
            await saveFacebookImportStatus(uuid, {
                status: "is_empty",
                percentage: 0
            })
            return {
                statusCode: 200,
                body: JSON.stringify({message: "Is Empty"}),
            }
        }
        console.log("Inserting Metrics ... ")
        const insertedItems = await insertMany("metrics", validatedRecords)
        console.log("Creating report ... ")
        const report_data = await insertOneDocument("reports_data", {
                "import_list_id": import_list_inserted.insertedId,
                "client_id": clientId,
                "agency_id": agencyId,
                "imported_list": importListDocument,
                "uuid": uuid,
                "createdAt": new Date()
            }
        )
        if (last_sub_reports && last_sub_reports.length > 0) {
            for (const last_sub_report of last_sub_reports) {
                await insertOneDocument("sub_reports", {
                        ...((({_id, ...rest}) => rest)(last_sub_report)),
                        "import_list_id": import_list_inserted.insertedId,
                        "client_id": clientId,
                        "agency_id": agencyId,
                        "report_data_id": report_data.insertedId,
                        "createdAt": new Date()
                    }
                );
            }
        } else {
            await insertOneDocument("sub_reports", {
                import_list_id: import_list_inserted.insertedId,
                client_id: clientId,
                agency_id: agencyId,
                title: "Source",
                sortArray: [{"columnId": "spend", "direction": "desc"}],
                filter: {
                    "filters": [{
                        "id": "94487",
                        "columnId": "spend",
                        "operator": ">",
                        "value": 0,
                        "disabled": false
                    }],
                    "operator": "and"
                },
                updatedAt: new Date(),
                createdAt: new Date()
            })
        }
        await saveFacebookImportStatus(uuid, {
            status: "Analyzing imported data",
            percentage: 30
        })
        await updateMessagesAndLinks(uuid, clientId)
        await generateProduct(uuid, clientId, agencyId)
        await saveFacebookImportStatus(uuid, {
            status: "success",
            percentage: 100
        })
    } catch (error) {
        console.error("An error occurred:", error);
        await saveFacebookImportStatus(uuid, {
            status: "failed",
            error: JSON.stringify(error)
        })
        // Re-throw the error to propagate it further
        throw error;
    }
}

// Endpoint to trigger the task
app.post('/run-task', authenticate, (req, res) => {
    const params = req.body;

    // Validate incoming parameters
    if (!params.start_date || !params.end_date || !params.fbAccessToken || !params.FBadAccountId) {
        return res.status(400).send({success: false, message: 'Missing required parameters'});
    }

    // Acknowledge request
    res.status(200).send({success: true, message: 'Task has been queued for processing'});

    // Run the task in the background
    runInBackground(mainTask, params);
});

Sentry.setupExpressErrorHandler(app);

// Optional fallthrough error handler
app.use(function onError(err, req, res, next) {
    // The error id is attached to `res.sentry` to be returned
    // and optionally displayed to the user for support.
    res.statusCode = 500;
    res.end(res.sentry + "\n");
});
// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});

console.log(await mainTask(
    {
        fbAccessToken: "EAAYXHibjFxoBO6vxBI78V3tdAbSkxT5WbqiFUjUc4pCsal5b35r1ZC6rZCSQV4FYSgsJxKqv1EvC03ZAKVu6dAAAzLnHFDZCoZBLy1s826iv54IKD1Ie3mkf6LzDWvihtRu1iECkW3eNvDEdeNseXhaF0QGBzplGZA4NhrubpDw4Ye9d7y35o0loBRZASepixlB5aJaUvzL7LIdiFOugs7ZAnmiNAWBeYLGwOEjBbOZABmugviaztQAZDZD",
        FBadAccountId: "act_555176035960035",
        start_date: "2025-03-29",
        end_date: "2025-03-30",
        agencyId: "6656208cdb5d669b53cc98c5",
        clientId: "67d306be742ef319388d07d1",
        userId: "66b03f924a9351d9433dca51",
        importListName: "Lancer Skincare (US) BACKUP PMT-2Days",
        uuid: "82676d40-10d8-4175-a15d-597f2bd64da5",
        ad_objective_id: "landing_page_views",
        ad_objective_field_expr: "actions.landing_page_view"
    }
))

