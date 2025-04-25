import {parseExpressionAt} from "acorn";

const ALLOWED_BINARY_OPS = new Set(["+", "-", "*", "/", "**"]);
const ALLOWED_UNARY_OPS = new Set(["+", "-"]);
const ALLOWED_FUNCTIONS = {
    sqr: (x) => x * x,
};

function validateNode(node) {
    switch (node.type) {
        case "Literal":
            if (typeof node.value !== "number") {
                throw new Error(`Non-numeric literal: ${node.value}`);
            }
            break;

        case "Identifier":
            // variable lookup is allowed
            break;

        case "BinaryExpression":
            if (!ALLOWED_BINARY_OPS.has(node.operator)) {
                throw new Error(`Unsupported operator: ${node.operator}`);
            }
            validateNode(node.left);
            validateNode(node.right);
            break;

        case "UnaryExpression":
            if (!ALLOWED_UNARY_OPS.has(node.operator)) {
                throw new Error(`Unsupported unary operator: ${node.operator}`);
            }
            validateNode(node.argument);
            break;

        case "CallExpression":
            if (
                node.callee.type !== "Identifier" ||
                !(node.callee.name in ALLOWED_FUNCTIONS) ||
                node.arguments.length !== 1
            ) {
                throw new Error(`Unsupported function call: ${node.callee.name}`);
            }
            validateNode(node.arguments[0]);
            break;

        case "ExpressionStatement":
            validateNode(node.expression);
            break;

        default:
            throw new Error(`Unsupported syntax node: ${node.type}`);
    }
}

function evaluateNode(node, row) {
    switch (node.type) {
        case "Literal":
            return node.value;

        case "Identifier":
            return row[node.name];

        case "BinaryExpression": {
            const l = evaluateNode(node.left, row);
            const r = evaluateNode(node.right, row);
            if (l == null || r == null) return null;
            switch (node.operator) {
                case "+":
                    return l + r;
                case "-":
                    return l - r;
                case "*":
                    return l * r;
                case "/":
                    return r === 0 ? null : l / r;
                case "**":
                    return Math.pow(l, r);
            }
        }

        case "UnaryExpression": {
            const v = evaluateNode(node.argument, row);
            if (v == null) return null;
            return node.operator === "-" ? -v : +v;
        }

        case "CallExpression": {
            const fn = ALLOWED_FUNCTIONS[node.callee.name];
            const arg = evaluateNode(node.arguments[0], row);
            if (arg == null) return null;
            return fn(arg);
        }

        default:
            return null; // should never reach
    }
}

function compileFormula(expr) {
    // parse the expression at position 0
    const node = parseExpressionAt(expr, 0, {ecmaVersion: 2020});
    validateNode(node);
    return (row) => {
        try {
            return evaluateNode(node, row);
        } catch {
            return null;
        }
    };
}

export function buildForwardCalculators(schema) {
    const forward = {};
    for (const {key, formula} of schema) {
        if (!formula || formula.toUpperCase() === "N/A") {
            forward[key] = null;
        } else {
            try {
                forward[key] = compileFormula(formula);
            } catch (err) {
                console.warn(`Invalid formula for ${key}: ${err.message}`);
                forward[key] = null;
            }
        }
    }
    return forward;
}

export function fillMissingFields(rows, schema, maxIterations = 5) {
    const forward = buildForwardCalculators(schema);
    let iteration = 0;

    while (iteration < maxIterations) {
        let changed = false;
        for (const row of rows) {
            for (const {key} of schema) {
                if (row[key] == null && typeof forward[key] === "function") {
                    const val = forward[key](row);
                    if (val != null) {
                        row[key] = val;
                        changed = true;
                    }
                }
            }
        }
        if (!changed) break;
        iteration++;
    }

    return rows;
}

export function convertToObject(data, ad_objective_field_expr, ad_objective_id, extraFields = []) {
    const expr = ad_objective_field_expr.split(".");

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

        const extraFieldsValues = extraFields.reduce((acc, field) => {
            acc[field] = item[field];
            return acc;
        }, {});

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
            vvr: impressions ? item.actions?.video_view / impressions : null,
            hold: impressions ? item.video_thruplay_watched_actions?.video_view / impressions : null,
            cpa: item.cost_per_action_type?.purchase || null,
            cvr: item.actions?.link_click
                ? (item?.[expr[0]]?.[expr[1]] ? item[expr[0]][expr[1]] / item.actions?.link_click : 0)
                : null,
            roas: item.purchase_roas?.omni_purchase || null,
            cpc: item.cost_per_action?.link_click || (item.actions?.link_click ? spend / item.actions?.link_click : null),
            cpl: item.cost_per_action?.lead || null,
            revenue: item.action_values?.purchase || null,
            video_view_3s: item.actions?.video_view || null,
            video_view_15s: item.video_thruplay_watched_actions?.video_view || null,
            video_avg_time_watched: item.video_avg_time_watched_actions?.video_view || null,
            video_p25_watched: item.video_p25_watched_actions?.video_view || null,
            video_p50_watched: item.video_p50_watched_actions?.video_view || null,
            video_p75_watched: item.video_p75_watched_actions?.video_view || null,
            video_p95_watched: item.video_p95_watched_actions?.video_view || null,
            video_p100_watched: item.video_p100_watched_actions?.video_view || null,
            momentum_rate: item.video_p25_watched_actions?.video_view ? item.video_p75_watched_actions?.video_view / item.video_p25_watched_actions?.video_view : null,
            // [ad_objective_id] :  item?.[expr[0]]?.[expr[1]],
            result: item?.[expr[0]]?.[expr[1]],
            cpr: item?.[expr[0]]?.[expr[1]] ? spend / item[expr[0]][expr[1]] : Infinity,
            post_url,
            ad_id,
            format,
            thumbnail_url: item.creative?.thumbnail_url,
            ...extraFieldsValues,
            other_fields: {
                ...restOfItem,
            },
        };
    });
}

export function findNonEmptyKeys(array) {
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

export function transformObjects(data) {
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

export function findMostSimilarKey(item, array1) {
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

export function getPercentFields(arr) {
    return arr.filter(item => item.format === 'percent').map(item => item.key);
}

function parseFormulaOld(formula) {
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
            dependencies[metric.key] = parseFormulaOld(metric.formula);
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

function getFieldType(fieldKey, schema) {
    const field = schema?.find(item => item.key === fieldKey);
    return field ? field.type : null;
}

function processRow(row, mappedColumns, schema) {
    const newRow = {};
    Object.keys(mappedColumns).forEach(dbColumn => {
        const Header = mappedColumns[dbColumn];
        if (Header) {
            if (row.hasOwnProperty(Header)) {
                const fieldType = getFieldType(dbColumn, schema);
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

export function processData(Data, mappedColumns, metrics, agencyId, clientId, userId, import_list_inserted, schema) {
    return Data.map(row => {
        let newRow = processRow(row, mappedColumns, schema);
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

export function NormalizeNumberObjects(dataArray, keysToCheck) {
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

export function detectAndNormalizePercentageInObjects(dataArray, keysToCheck) {
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