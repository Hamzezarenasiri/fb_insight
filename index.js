import dotenv from 'dotenv';
import axios from 'axios';
import {ObjectId} from 'mongodb';
import { findDocuments as findDocumentsRepo, insertMany as insertManyRepo, findOneDocument as findOneDocumentRepo, aggregateDocuments as aggregateDocumentsRepo, updateOneDocument as updateOneDocumentRepo, updateManyDocuments as updateManyDocumentsRepo, insertOneDocument as insertOneDocumentRepo, findAndUpdate as findAndUpdateRepo } from './src/repositories/mongo/common.js';
import { runAthenaQuery as runAthenaQuerySvc } from './src/services/athena/athena.service.js';
import { saveFacebookImportStatus as saveFacebookImportStatusSvc } from './src/services/status/status.service.js';
import { sendHttpRequest as sendHttpRequestSvc } from './src/utils/http.js';
import { getAdsInsights as getAdsInsightsSvc } from './src/services/facebook/facebook.service.js';
import { getAdsLibrary as getAdsLibrarySvc } from './src/services/facebook/adLibrary.service.js';
import { FIELDS as FB_FIELDS } from './src/services/facebook/fields.js';
import { default_schema as DEFAULT_SCHEMA, buildClientSchema } from './src/services/reporting/schema.defaults.js';
import { calculateMetrics as calculateMetricsSvc, fillMissingFields as fillMissingFieldsSvc } from './src/services/reporting/metrics.service.js';
import { transformObjects as transformObjectsSvc, findMostSimilarKey as findMostSimilarKeySvc, getPercentFields as getPercentFieldsSvc } from './src/services/reporting/mapping.service.js';
import { processData as processDataSvc, NormalizeNumberObjects as NormalizeNumberObjectsSvc, detectAndNormalizePercentageInObjects as detectAndNormalizePercentageInObjectsSvc } from './src/services/reporting/process.service.js';
import { convertToObject as convertToObjectSvc, findNonEmptyKeys as findNonEmptyKeysSvc } from './src/services/reporting/transform.service.js';
import { getFbAdPreview as getFbAdPreviewSvc, getPropsOfSource as getPropsOfSourceSvc, removeUTM as removeUTMSvc } from './src/services/enrichment/preview.service.js';
import { updateMessagesAndLinks as updateMessagesAndLinksSvc, generateProduct as generateProductSvc } from './src/services/enrichment/product.service.js';
import { tagging as taggingSvc } from './src/services/enrichment/tagging.service.js';

// Sentry is initialized in src/server.js
dotenv.config();
const BASE_URL = "https://graph.facebook.com/v22.0";
const fluxAPIBaseUrl = "https://flux-api.afarin.top";
const fluxAPIkey = process.env.FLUX_STATIC_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY
// Facebook fields now provided by FB_FIELDS from services
/* const FIELDS = [
    // "ad_quality_ranking",
    "engagement_rate_ranking",
    "conversion_rate_ranking",
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
].join(","); */
import { default_schema as DEFAULT_SCHEMA, buildClientSchema } from './src/services/reporting/schema.defaults.js'
const default_schema = DEFAULT_SCHEMA; /* moved to schema.defaults.js */
/*
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
        "key" : "adset_name",
        "title" : "Adset Name",
        "type" : "string",
        "required" : true,
        "description" : "Adset Name refers to the name or title of the adset",
        "is_default" : true,
        "similar_dictionary" : [ "adset", "Adset Name" ],
        "order_preference" : "decs",
        "format" : "text",
        "formula" : "N/A"
    },{
        "key" : "campaign_name",
        "title" : "Campaign Name",
        "type" : "string",
        "required" : true,
        "description" : "Campaign Name refers to the name or title of the campain",
        "is_default" : true,
        "similar_dictionary" : [ "campaign", "Campaign Name" ],
        "order_preference" : "decs",
        "format" : "text",
        "formula" : "N/A"
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
        "key": "video_p25_watched",
        "is_default": true,
        "title": "Video p25 watched",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": [],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p50_watched",
        "is_default": true,
        "title": "Video p50 watched",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": [],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p75_watched",
        "is_default": true,
        "title": "Video p75 watched",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": [],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p95_watched",
        "is_default": true,
        "title": "Video p95 watched",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": [],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "video_p100_watched",
        "is_default": true,
        "title": "Video p100 watched",
        "description": "",
        "required": false,
        "type": "integer",
        "similar_dictionary": [],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "momentum_rate",
        "is_default": true,
        "title": "Momentum Rate",
        "description": "",
        "required": false,
        "type": "float",
        "similar_dictionary": ["momentum_rate"],
        "order_preference": "decs",
        "format": "percent",
        "formula": "(video_p75_watched / video_p25_watched)"
    },
    {
        "key": "cpr",
        "title": "CPR",
        "type": "float",
        "required": false,
        "description": "CPR is the cost per Result.",
        "is_default": true,
        "similar_dictionary": [
            "cpr",
            "cost per result",
        ],
        "order_preference": "acs",
        "format": "currency",
        "formula": `spend / result`
        // "formula" : `(spend / ${ad_objective_id})`
    },
    {
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
    },
    {
        "key": "Conversion Rate Ranking",
        "title": "conversion_rate_ranking",
        "type": "string",
        "required": false,
        "description": "Conversion Rate Ranking",
        "is_default": true,
        "similar_dictionary": [
            "Conversion Rate Ranking",
            "conversion_rate_ranking"
        ],
        "order_preference": "decs",
        "format": "text",
        "formula": "N/A"
    },
    {
        "key": "Engagement Rate Ranking",
        "title": "engagement_rate_ranking",
        "type": "string",
        "required": false,
        "description": "Engagement Rate Ranking",
        "is_default": true,
        "similar_dictionary": [
            "Engagement Rate Ranking",
            "engagement_rate_ranking"
        ],
        "order_preference": "decs",
        "format": "text",
        "formula": "N/A"
    }
*/
// Athena client initialization moved to src/services/athena/athena.service.js
// HTTP app and routes are defined in src/app.js; keep this file focused on domain functions
// Athena query moved to src/services/athena/athena.service.js

// Authentication middleware moved to src/middlewares/auth.js

// Background task wrapper
// runInBackground replaced by BullMQ queues

// MongoDB helpers moved to src/repositories/mongo/common.js

// A safe AST‐based formula compiler in plain JavaScript, with support for + - * / **, parentheses, and sqr(x).

// You'll need to install/acquire a JS parser like acorn:
//    npm install acorn
// Metrics forward-fill helpers moved to src/services/reporting/metrics.service.js


// fetchAds/fetchBatchData moved to src/services/facebook/facebook.service.js
// getAdsInsights moved to src/services/facebook/facebook.service.js

// getAdsLibrary moved to src/services/facebook/adLibrary.service.js


// convertToObject moved to src/services/reporting/transform.service.js

// findNonEmptyKeys moved to src/services/reporting/transform.service.js

// transformObjects moved to src/services/reporting/mapping.service.js

// jaroWinklerDistance moved to src/services/reporting/mapping.service.js

// findMostSimilarKey moved to src/services/reporting/mapping.service.js

// getPercentFields moved to src/services/reporting/mapping.service.js

// parseFormulaOld moved to src/services/reporting/metrics.service.js

// calculateMetrics moved to src/services/reporting/metrics.service.js

// cleanData moved to src/services/reporting/process.service.js

// getFieldType moved to src/services/reporting/process.service.js

// processRow moved to src/services/reporting/process.service.js

// processData moved to src/services/reporting/process.service.js

// capitalizeFirstChar moved to src/services/reporting/process.service.js

// NormalizeNumberObjects moved to src/services/reporting/process.service.js

// detectAndNormalizePercentageInObjects moved to src/services/reporting/process.service.js


// saveFacebookImportStatus moved to src/services/status/status.service.js

// getFbAdPreview moved to src/services/enrichment/preview.service.js

// getSource moved to src/services/enrichment/preview.service.js

// extractAndDecode moved to src/services/enrichment/preview.service.js

// removeUTM moved to src/services/enrichment/preview.service.js

// getPropsOfSource moved to src/services/enrichment/preview.service.js

// updateMessagesAndLinks moved to src/services/enrichment/product.service.js

/* generateProduct moved to src/services/enrichment/product.service.js */

function mergeArraysByAdName(arr1, arr2) {
    const lookup = arr2.reduce((acc, item) => {
        acc[item.code] = item;
        return acc;
    }, {});

    return arr1.map(item => {
        const code = item.ad_name.split('_')[0];
        if (lookup[code]) {
            return {...item, ...lookup[code]};
        }
        return item;
    });
}

function aggregateByCode(arr) {
    const groups = {};
    arr.forEach(item => {
        const code = item.ad_name.split('_')[0];
        if (!groups[code]) {
            groups[code] = JSON.parse(JSON.stringify(item));
        } else {
            groups[code] = mergeAggregate(groups[code], item);
        }
    });
    return Object.values(groups);
}

function mergeAggregate(obj1, obj2) {
    Object.keys(obj2).forEach(key => {
        if (key === 'ad_name') return;
        const v2 = obj2[key], v1 = obj1[key];
        if (Array.isArray(v2)) {
            if (v1 === undefined) obj1[key] = v2;
        } else if (v2 && typeof v2 === 'object') {
            if (v1 === undefined) obj1[key] = JSON.parse(JSON.stringify(v2));
            else obj1[key] = mergeAggregate(v1, v2);
        } else if (typeof v2 === 'number') {
            const skip = /^(?:cost_per_|cpm$|cpc$|cpp$|ctr$|.*_ctr$)/.test(key);
            if (!skip) obj1[key] = (typeof v1 === 'number' ? v1 : 0) + v2;
            else if (v1 === undefined) obj1[key] = v2;
        } else if (typeof v2 === 'string') {
            if (v1 === undefined) obj1[key] = v2;
        }
    });
    return obj1;
}


// tagging moved to src/services/enrichment/tagging.service.js

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
        ad_objective_field_expr,
        ai
    } = params;
    agencyId = new ObjectId(agencyId);
    clientId = new ObjectId(clientId);
    userId = new ObjectId(userId);
    let defined_schema = await findOneDocumentRepo("defined_schemas", {
        client_id: clientId, "schema": {"$exists": true, "$ne": []},
    })
    let schema = []
    if (defined_schema) {
        schema = defined_schema.schema;
    } else {
        if (["act_70970029", "act_1474898293329309"].includes(FBadAccountId)) {
            schema = buildClientSchema(default_schema, FBadAccountId);
        } else {
            schema = default_schema;
        }
    }
    try {
        console.log("start ....", params)
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
        // await generateProduct(uuid, clientId, agencyId)
        // return
        await saveFacebookImportStatusSvc(uuid, {
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
        const metrics = await findDocumentsRepo("import_schema", {
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
        const MetricsIDs = (await aggregateDocumentsRepo("metrics", [
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
        let results = await getAdsInsightsSvc(FBadAccountId, fbAccessToken, start_date, end_date, uuid, FB_FIELDS)
        await insertManyRepo("fb_insights", results.map(item => ({
            ...item,
            uuid
        })))
        if (["act_70970029", "act_1474898293329309"].includes(FBadAccountId)) {
            results = aggregateByCode(results);
            const athena_result = await runAthenaQuerySvc({ start_date, end_date });
            await insertManyRepo("athena_result", athena_result.map(item => ({
                ...item,
                uuid
            })))
            results = mergeArraysByAdName(results, athena_result)
            await insertManyRepo("merged_results", results.map(item => ({
                ...item,
                uuid
            })))
        }
        const ads = convertToObjectSvc(results, ad_objective_field_expr, ad_objective_id, ["lead", "appts", "show", "sold", "green_appts", "yellow_appts", "red_appts","cpgya","s2a","gya","gyv","cpappts"])
        console.log('SAMPLE', ads[0].Ad_Name, ads[0].video_views_15s, ads[0].impressions, ads[0].hold);
        const exist_fields = findNonEmptyKeysSvc(ads)
        const Headers = exist_fields.filter(item => !["post_url", "other_fields", "ad_id",].includes(item));
        const tableColumns = transformObjectsSvc(schema);
        const result = Headers.map(item => {
            const {key, similarity} = findMostSimilarKeySvc(item, tableColumns);
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
        const last_imported_list = (await findDocumentsRepo("imported_lists", {
            client_id: clientId
        }, {}, {"createdAt": -1}))?.[0]
        const last_sub_reports = last_imported_list ? await findDocumentsRepo("sub_reports", {
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
        const import_list_inserted = await insertOneDocumentRepo("imported_lists", importListDocument);
        await saveFacebookImportStatusSvc(uuid, {
            import_list_id: import_list_inserted.insertedId,
        })
        let newDataArray = processDataSvc(ads, formData, metrics, agencyId, clientId, userId, import_list_inserted, schema, calculateMetricsSvc);
        // ★ restore the correct `hold` that processData zeroed out ★
newDataArray.forEach((row, idx) => {
  row.hold = ads[idx].hold;
});
    console.log("🐛 after processData, hold =", newDataArray[0].hold);

        
        newDataArray = fillMissingFieldsSvc(newDataArray, schema)

            console.log("🐛 after fillMissingFields, hold =", newDataArray[0].hold);

        const PercentkeysToCheck = getPercentFieldsSvc(metrics);
        const keysToCheck = await findDocumentsRepo("import_schema", {type: {$in: ["float", "integer"]}}, {key: 1, _id: 0});
        let res = NormalizeNumberObjectsSvc(newDataArray, keysToCheck);
            console.log("🐛 after NormalizeNumberObjects, hold =", res[0].hold);

        console.log("Validating Records ... ")
        let validatedRecords = detectAndNormalizePercentageInObjectsSvc(res, PercentkeysToCheck)
            console.log("🐛 after detectAndNormalize, hold =", validatedRecords[0].hold);

        console.log(
          "⏺️ INSERT SAMPLE:",
          validatedRecords[0].Ad_Name,
          "hold=",
          validatedRecords[0].hold,
          "type:",
          typeof validatedRecords[0].hold
        );

        
        const AssetsIds = await aggregateDocumentsRepo("assets", [
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
                    "meta_data.fb_data.creative": creative,
                };
                if (message) {
                    set_dict["meta_data.fb_data.message"] = message;
                }
                if (product_link) {
                    set_dict["meta_data.fb_data.product_link"] = product_link;
                    set_dict["meta_data.fb_data.product_url"] = removeUTMSvc(product_link);
                }
                // remove this part of code when all asset updated
                await updateOneDocumentRepo("assets", {_id: new ObjectId(entry.asset_id)}, {$set: set_dict}
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
                        fb_data.product_url = removeUTMSvc(product_link);
                    }
                    const new_asset = await insertOneDocumentRepo("assets", {
                        agency_id: agencyId,
                        client_id: clientId,
                        import_list_id: import_list_inserted.insertedId,
                        user_id: userId,
                        ad_id: entry.ad_id,
                        adname: entry.Ad_Name,
                        post_url: entry.post_url,
                        format: entry.format,
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
            await saveFacebookImportStatusSvc(uuid, {
                status: "is_empty",
                percentage: 0
            })
            return {
                statusCode: 200,
                body: JSON.stringify({message: "Is Empty"}),
            }
        }
        console.log("Inserting Metrics ... ")
        const insertedItems = await insertManyRepo("metrics", validatedRecords)
        console.log("Creating report ... ")
        const report_data = await insertOneDocumentRepo("reports_data", {
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
                await insertOneDocumentRepo("sub_reports", {
                        ...(({_id, ...rest}) => rest)(last_sub_report),
                        "import_list_id": import_list_inserted.insertedId,
                        "client_id": clientId,
                        "agency_id": agencyId,
                        "report_data_id": report_data.insertedId,
                        "createdAt": new Date()
                    }
                );
            }
        } else {
            await insertOneDocumentRepo("sub_reports", {
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
        await saveFacebookImportStatusSvc(uuid, {
            status: "Analyzing imported data",
            percentage: 20
        })
        await updateMessagesAndLinksSvc(uuid, clientId)
        await generateProductSvc(uuid, clientId, agencyId)
        if (ai) {
            const response = await taggingSvc(import_list_inserted.insertedId, clientId, ai)
        } else {
            await saveFacebookImportStatus(uuid, {
                status: "success",
                percentage: 100
            })
        }
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

// /run-task route moved to src/controllers/task.controller.js


async function adLibraryTask(params) {
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
        search_page_ids,
        max_count,
    } = params;
    agencyId = new ObjectId(agencyId);
    clientId = new ObjectId(clientId);
    userId = new ObjectId(userId);
    let schema = []
    try {
        console.log("start adLibrary....", params)
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
        console.log("Getting ads ... ")
        let results = await getAdsLibrarySvc(FBadAccountId, fbAccessToken, start_date, end_date, uuid, search_page_ids, max_count)
        return await insertManyRepo("fb_ad_libraries", results.map(item => ({
            ...item,
            uuid
        })))
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

// /run-ad-library route moved to src/controllers/adLibrary.controller.js

// Express error handler is configured in src/app.js

// Export task functions for use in modular server/controllers
export { mainTask };
export { adLibraryTask };

// console.log(await mainTask(
//     {
//         fbAccessToken: "EAAYXHibjFxoBO6vxBI78V3tdAbSkxT5WbqiFUjUc4pCsal5b35r1ZC6rZCSQV4FYSgsJxKqv1EvC03ZAKVu6dAAAzLnHFDZCoZBLy1s826iv54IKD1Ie3mkf6LzDWvihtRu1iECkW3eNvDEdeNseXhaF0QGBzplGZA4NhrubpDw4Ye9d7y35o0loBRZASepixlB5aJaUvzL7LIdiFOugs7ZAnmiNAWBeYLGwOEjBbOZABmugviaztQAZDZD",
//         FBadAccountId: "act_70970029",
//         start_date: "2025-03-10",
//         end_date: "2025-04-10",
//         agencyId: "6656208cdb5d669b53cc98c5",
//         clientId: "67d306be742ef319388d07d1",
//         userId: "66b03f924a9351d9433dca51",
//         importListName: "SonoBCCF1",
//         uuid: "82676d40-10d8-4175-a15d-597f2bd64da4",
//         ad_objective_id: "leads_all",
//         ad_objective_field_expr: "actions.lead",
//         ai: "gemini"
//     }
// ))
// console.log(await adLibraryTask(
//     {
//         fbAccessToken: "EAAYXHibjFxoBO554Fwz1WOkGN0LTPIQEy6nCsjBEE9iWe3AEgnuH6EFUT17zxm0rLIvcxhk3BKhrJ6cinxMfsSYf7VkUXL0nL1p9ZCS1idEqHPbeJWCZABtYu5mVBRGQGJI4GiTQWBFLsuG8mYtimGQVlMh1gvU6OzlecOjZBZAxTjsxIPBIwdVoZB5z8B6ohZCmRePIyuJ4YmPdIfDmaI9H4ocO8WWZBi894gJ5ZA0TszKTQfgd8aqo5kYF8KFkB14pWVvjyPEpawZDZD",
//         FBadAccountId: "act_70970029",
//         start_date: "2024-05-07",
//         end_date: "2025-05-25",
//         agencyId: "6656208cdb5d669b53cc98c5",
//         clientId: "67d306be742ef319388d07d1",
//         userId: "66b03f924a9351d9433dca51",
//         importListName: "AdLibraryTest",
//         uuid: "111111111111111111111111",
//         search_page_ids:"['98269389167']",
//         max_count : 50
//     })
// )


