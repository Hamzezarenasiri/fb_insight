import * as Sentry from "@sentry/node"
import dotenv from 'dotenv';
import express from 'express';
import axios from 'axios';
import {ObjectId} from 'mongodb';
import AWS from 'aws-sdk';
dotenv.config();

// You’ll need to install/acquire a JS parser like acorn:
//    npm install acorn
import {
    aggregateDocuments,
    findDocuments,
    findOneDocument,
    insertMany,
    insertOneDocument,
    updateManyDocuments,
    updateOneDocument
} from "./mongodb.js";
import {runAthenaQuery} from "./athena.js";
import {saveFacebookImportStatus} from "./common.js";
import {getAdsInsights, removeUTM, updateMessagesAndLinks} from "./getAdsInsights.js";
import {tagging} from "./tagging.js";
import {
    convertToObject,
    detectAndNormalizePercentageInObjects,
    fillMissingFields,
    findMostSimilarKey,
    findNonEmptyKeys,
    getPercentFields,
    NormalizeNumberObjects,
    processData,
    transformObjects
} from "./validateRecords.js";

Sentry.init({
    dsn: "https://a51aca261c977758f4342257034a5d59@o1178736.ingest.us.sentry.io/4508958246043648",
});
const OPENAI_API_KEY = process.env.OPENAI_API_KEY
let default_schema = [
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
        "key": "cpgya",
        "is_default": true,
        "title": "CPGYA",
        "description": "Cost per GYA",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Cost per Gya"
        ],
        "order_preference": "acs",
        "format": "currency",
        "formula": "spend / (green_appts + yellow_appts)"
    },
    {
        "key": "l2a",
        "is_default": true,
        "title": "L2A",
        "description": "Lead to Appointment",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Lead to Appt",
            "L2A"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "appts / lead"
    },
    {
        "key": "l2s",
        "is_default": true,
        "title": "L2S",
        "description": "Lead to Sale",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Lead to Sale",
            "L2S"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "sold / lead"
    },
    {
        "key": "l2c",
        "is_default": true,
        "title": "L2C",
        "description": "Lead to Conversion",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Lead to Conversion"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "show / lead"
    },
    {
        "key": "s2s",
        "is_default": true,
        "title": "S2S",
        "description": "Stage 2 to Sale conversion",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "S2S Conversion"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "sold / show"
    },
    {
        "key": "s2a",
        "is_default": true,
        "title": "S2A",
        "description": "Stage 2 to Appointment conversion",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "S2A Conversion"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "show / appts"
    },
    {
        "key": "gya",
        "is_default": true,
        "title": "GYA",
        "description": "GYA metric",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "GYA"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "(green_appts + yellow_appts) / appts"
    },
    {
        "key": "gyv",
        "is_default": true,
        "title": "GYV",
        "description": "GYV metric",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "GYV"
        ],
        "order_preference": "acs",
        "format": "number",
        "formula": "green_appts + yellow_appts"
    },
    {
        "key": "cpsold",
        "is_default": true,
        "title": "CPSOLD",
        "description": "Cost per sold",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Cost per Sold"
        ],
        "order_preference": "acs",
        "format": "currency",
        "formula": "spend / sold"
    },
    {
        "key": "cpshow",
        "is_default": true,
        "title": "CPSHOW",
        "description": "Cost per show",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Cost per Show"
        ],
        "order_preference": "acs",
        "format": "currency",
        "formula": "spend / show"
    },
    {
        "key": "cpappts",
        "is_default": true,
        "title": "CPAPPTS",
        "description": "Cost per appointment",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Cost per Appointment"
        ],
        "order_preference": "acs",
        "format": "currency",
        "formula": "spend / appts"
    },
    {
        "key": "lead_cvr",
        "is_default": true,
        "title": "Lead CVR",
        "description": "Lead conversion rate",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Lead Conversion Rate"
        ],
        "order_preference": "decs",
        "format": "percent",
        "formula": "lead / link_clicks"
    },
    {
        "key": "sold",
        "is_default": true,
        "title": "SOLD",
        "description": "Number of final sales",
        "required": false,
        "type": "integer",
        "similar_dictionary": [
            "Sales",
            "Sold"
        ],
        "order_preference": "acs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "show",
        "is_default": true,
        "title": "SHOW",
        "description": "Customers showing up at the doctor's office",
        "required": false,
        "type": "integer",
        "similar_dictionary": [
            "Show"
        ],
        "order_preference": "acs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "appts",
        "is_default": true,
        "title": "APPTS",
        "description": "Appointments",
        "required": false,
        "type": "integer",
        "similar_dictionary": [
            "Appointments"
        ],
        "order_preference": "acs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "green_appts",
        "is_default": true,
        "title": "Green Appointments",
        "description": "Green Appointments",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Green Appointments"
        ],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "yellow_appts",
        "is_default": true,
        "title": "Yellow Appointments",
        "description": "Yellow Appointments",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Yellow Appointments"
        ],
        "order_preference": "decs",
        "format": "number",
        "formula": "N/A"
    },
    {
        "key": "red_appts",
        "is_default": true,
        "title": "Red Appointments",
        "description": "Red Appointments",
        "required": false,
        "type": "float",
        "similar_dictionary": [
            "Red Appointments"
        ],
        "order_preference": "acs",
        "format": "number",
        "formula": "N/A"
    }, {
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
    }, {
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
    }
]
AWS.config.update({
    region: process.env.AWS_REGION || 'us-east-1',
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});
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

// A safe AST‐based formula compiler in plain JavaScript, with support for + - * / **, parentheses, and sqr(x).

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
    if (tags.length > 1) {
        return
    }
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
    Identify relevant tags for the product according to the specified categories below.

tags categories:
    ${JSON.stringify(categories_val, null, 1)}

urls:
    ${JSON.stringify(chunk.map(item => item.url).filter(Boolean), null, 1)}
    
description (briefly explain the meaning or context of the tag)
${prompt}
Important rules to follow:
    Any tag created, including within categories ${joinedCategories}.
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
        console.log(prompt_code_part, "<<<<<<<<prompt_code_part")
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
        console.log(contentFixed, "<<<contentFixed")
        // Assuming the output is valid JSON
        console.log(`result: ${contentFixed}`);
        return JSON.parse(contentFixed);

    }

    // Main function to process all chunks and accumulate the results
    async function extractAllProductDetails() {
        const chunkSize = 50;
        const chunks = chunkArray(assets_links, chunkSize);
        const allResults = [];
        let startProgress = 50;
        const endProgress = 60;
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
    let startProgress = 50;
    const endProgress = 60;
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

async function create_report(import_list_inserted, importListDocument) {
    console.log("Creating report ... ")
    const last_imported_list = (await findDocuments("imported_lists", {
        client_id: clientId
    }, {}, {"createdAt": -1}))?.[0]
    const last_sub_reports = last_imported_list ? await findDocuments("sub_reports", {
        import_list_id: last_imported_list._id
    }, {html_note: 0}) : []
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
        ad_objective_field_expr,
        ai
    } = params;
    agencyId = new ObjectId(agencyId);
    clientId = new ObjectId(clientId);
    userId = new ObjectId(userId);
    let defined_schema = await findOneDocument("defined_schemas", {
        client_id: clientId, "schema": {"$exists": true, "$ne": []},
    })
    let schema = []
    if (defined_schema) {
        schema = defined_schema.schema;
    } else {
        schema = default_schema;

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
        let results = await getAdsInsights(FBadAccountId, fbAccessToken, start_date, end_date, uuid)
        await insertMany("fb_insights", results.map(item => ({
            ...item,
            uuid
        })))
        results = aggregateByCode(results);
        const athena_result = await runAthenaQuery(start_date, end_date);
        await insertMany("athena_result", athena_result.map(item => ({
            ...item,
            uuid
        })))
        results = mergeArraysByAdName(results, athena_result)
        await insertMany("merged_results", results.map(item => ({
            ...item,
            uuid
        })))
        const ads = convertToObject(results, ad_objective_field_expr, ad_objective_id, ["lead", "appts", "show", "sold", "green_appts", "yellow_appts", "red_appts",])
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
        await saveFacebookImportStatus(uuid, {
            import_list_id: import_list_inserted.insertedId,
        })
        const metrics = await findDocuments("import_schema", {
            "type": {"$in": ["float", "integer"]},
            "formula": {
                "$exists": true,
                "$nin": [null, "", "N/A"]
            }
        })

        let newDataArray = processData(ads, formData, metrics, agencyId, clientId, userId, import_list_inserted, schema);
        newDataArray = fillMissingFields(newDataArray, schema)
        const PercentkeysToCheck = getPercentFields(metrics);
        const keysToCheck = await findDocuments("import_schema", {type: {$in: ["float", "integer"]}}, {key: 1, _id: 0});
        let res = NormalizeNumberObjects(newDataArray, keysToCheck);
        console.log("Validating Records ... ")
        let validatedRecords = detectAndNormalizePercentageInObjects(res, PercentkeysToCheck)
        let asset_ids = (await aggregateDocuments("assets", [
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
        ]))[0] || {}
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
        await create_report(import_list_inserted, importListDocument);
        await saveFacebookImportStatus(uuid, {
            status: "Analyzing imported data",
            percentage: 20
        })
        await updateMessagesAndLinks(uuid, clientId)
        await generateProduct(uuid, clientId, agencyId)
        if (ai) {
            const response = await tagging(import_list_inserted.insertedId, clientId, ai)
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
        FBadAccountId: "act_70970029",
        start_date: "2025-03-10",
        end_date: "2025-04-10",
        agencyId: "6656208cdb5d669b53cc98c5",
        clientId: "67d306be742ef319388d07d1",
        userId: "66b03f924a9351d9433dca51",
        importListName: "SonoBCCF1",
        uuid: "82676d40-10d8-4175-a15d-597f2bd64da4",
        ad_objective_id: "leads_all",
        ad_objective_field_expr: "actions.lead",
        ai: "gemini"
    }
))

