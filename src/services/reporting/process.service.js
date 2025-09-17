function cleanData(value, defaultValue = null) {
  if (value === undefined || value === null || value === '') return defaultValue;
  return value.toString().replace(/[\$,%]/g, '');
}

function getFieldType(fieldKey, schema) {
  const field = schema?.find(item => item.key === fieldKey);
  return field ? field.type : null;
}

export function processRow(row, mappedColumns, schema) {
  const newRow = {};
  Object.keys(mappedColumns).forEach(dbColumn => {
    const Header = mappedColumns[dbColumn];
    if (Header) {
      if (Object.prototype.hasOwnProperty.call(row, Header)) {
        const fieldType = getFieldType(dbColumn, schema);
        let cleanedData = cleanData(row[Header]);
        switch (fieldType) {
          case 'integer': newRow[dbColumn] = parseInt(cleanedData, 10) || 0; break;
          case 'float': newRow[dbColumn] = parseFloat(cleanedData) || 0.0; break;
          case 'boolean': newRow[dbColumn] = cleanedData.toLowerCase() === 'true'; break;
          default: newRow[dbColumn] = cleanedData;
        }
      }
    }
  });
  return newRow;
}

export const capitalizeFirstChar = str => (str ? str[0].toUpperCase() + str.slice(1).toLowerCase() : '');

export function processData(Data, mappedColumns, metrics, agencyId, clientId, userId, import_list_inserted, schema, calculateMetrics) {
  return Data.map(row => {
    let newRow = processRow(row, mappedColumns, schema);
    newRow = calculateMetrics(newRow, metrics);
    newRow.agency_id = agencyId;
    newRow.client_id = clientId;
    newRow.import_list_id = import_list_inserted.insertedId;
    newRow.user_id = userId;
    newRow.ad_id = row.ad_id;
    newRow.post_url = row.post_url;
    newRow.format = capitalizeFirstChar(row.format).replace('Photo', 'Image').replace('Share', 'Image');
    newRow.other_fields = row.other_fields;
    return newRow;
  });
}

export function NormalizeNumberObjects(dataArray, keysToCheck) {
  dataArray.forEach(obj => {
    keysToCheck.forEach(key => {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        let value = obj[key];
        if (typeof value === 'string') value = parseFloat(value);
        obj[key] = value;
      }
    });
  });
  return dataArray;
}

export function detectAndNormalizePercentageInObjects(dataArray, keysToCheck) {
  return dataArray.map(obj => {
    keysToCheck.forEach(key => {
      if (!Object.prototype.hasOwnProperty.call(obj, key)) return;
      let v = typeof obj[key] === 'string' ? parseFloat(obj[key]) : obj[key];
      if (typeof v === 'number' && !isNaN(v) && v > 1) obj[key] = v / 100;
    });
    return obj;
  });
}


