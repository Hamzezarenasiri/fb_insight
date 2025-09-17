import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } from '@aws-sdk/client-athena';
import { config } from '../../config/env.js';

const athena = new AthenaClient({
  region: config.aws.region,
  credentials: {
    accessKeyId: config.aws.accessKeyId,
    secretAccessKey: config.aws.secretAccessKey,
  },
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function transformAthenaResult(results) {
  const rows = results.ResultSet?.Rows || [];
  if (rows.length < 2) return [];
  const headers = rows[0].Data.map((c) => c.VarCharValue);
  const numericRegex = /^[+-]?\d+(\.\d+)?$/;
  return rows.slice(1).map((row) => {
    const record = {};
    row.Data.forEach((col, idx) => {
      let value = col.VarCharValue;
      if (value !== undefined && value !== null && value !== '' && numericRegex.test(value)) value = Number(value);
      record[headers[idx]] = value;
    });
    return record;
  });
}

export async function runAthenaQuery({ start_date, end_date, sql }) {
  const QueryString = sql || `
        WITH last_file AS (SELECT "$path" AS latest_file
                           FROM sonobellodata
                           ORDER BY "$path" DESC
            LIMIT 1)
        SELECT "opportunity source code" AS code,
               ANY_VALUE("opportunity source name") AS ad_name,
               SUM(CAST(leads AS BIGINT)) AS lead,
               SUM(CAST(appointments AS BIGINT)) AS appts,
               SUM(CAST(shows AS BIGINT)) AS show,
               SUM(CAST(sold AS BIGINT)) AS sold,
               SUM(CAST(sales_price AS DECIMAL(10, 2))) AS sales_price,
               SUM(CAST(cash_collected AS DECIMAL(10, 2))) AS cash_collected,
               SUM(CAST(red_apps AS BIGINT)) AS red_appts,
               SUM(CAST(yellow_apps AS BIGINT)) AS yellow_appts,
               SUM(CAST(green_apps AS BIGINT)) AS green_appts
        FROM sonobellodata
        WHERE "$path" = (SELECT latest_file FROM last_file)
          AND TRY(CAST(date_parse(opportunity_created_date, '%Y-%m-%d') AS DATE))
            BETWEEN DATE '${start_date}' AND DATE '${end_date}'
        GROUP BY "opportunity source code";`;

  const params = {
    QueryString,
    QueryExecutionContext: { Database: config.aws.athenaDatabase },
    ResultConfiguration: { OutputLocation: config.aws.athenaOutput },
  };

  const { QueryExecutionId } = await athena.send(new StartQueryExecutionCommand(params));
  while (true) {
    const exec = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId }));
    const state = exec.QueryExecution?.Status?.State;
    if (state === 'SUCCEEDED') break;
    if (state === 'FAILED' || state === 'CANCELLED') throw new Error(`Athena query ${state}`);
    await sleep(2000);
  }
  let results = await athena.send(new GetQueryResultsCommand({ QueryExecutionId }));
  return transformAthenaResult(results);
}


