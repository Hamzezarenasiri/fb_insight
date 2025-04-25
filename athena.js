import AWS from "aws-sdk";

const athena = new AWS.Athena();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function transformAthenaResult(results) {
    // Check if the results have at least one row (headers) and one data row.
    const rows = results.ResultSet.Rows;
    if (!rows || rows.length < 2) {
        return [];
    }

    // Extract headers from the first row.
    const headers = rows[0].Data.map(col => col.VarCharValue);

    // Regex to check valid number string: optional sign, at least one digit, optional fraction.
    const numericRegex = /^[+-]?\d+(\.\d+)?$/;

    // Process the rest of the rows.
    return rows.slice(1).map(row => {
        const record = {};
        row.Data.forEach((col, idx) => {
            let value = col.VarCharValue;
            // Only attempt conversion if value is non-empty.
            if (value !== undefined && value !== null && value !== '') {
                // If value matches the numeric regex, convert it to a number.
                if (numericRegex.test(value)) {
                    value = Number(value);
                }
            }
            record[headers[idx]] = value;
        });
        return record;
    });
} // Function to run the Athena query
export const runAthenaQuery = async (start_date, end_date) => {
    // Build the SQL query with the given date parameters.
    // Note: Athena expects dates in the format DATE 'YYYY-MM-DD'
    const query = `
        WITH last_file AS (SELECT "$path" AS latest_file
                           FROM sonobellodata
                           ORDER BY "$path" DESC
            LIMIT 1
            )
        SELECT "opportunity source code"                   AS code,
               ANY_VALUE("opportunity source name")        AS ad_name,
               SUM(CAST(leads AS BIGINT))                  AS lead,
               SUM(CAST(appointments AS BIGINT))           AS appts,
               SUM(CAST(shows AS BIGINT))                  AS show,
               SUM(CAST(sold AS BIGINT))                   AS sold,
               SUM(CAST(sales_price AS DECIMAL(10, 2)))    AS sales_price,
               SUM(CAST(cash_collected AS DECIMAL(10, 2))) AS cash_collected,
               SUM(CAST(red_apps AS BIGINT))               AS red_appts,
               SUM(CAST(yellow_apps AS BIGINT))            AS yellow_appts,
               SUM(CAST(green_apps AS BIGINT))             AS green_appts
        FROM sonobellodata
        WHERE "$path" = (SELECT latest_file FROM last_file)
          AND TRY(CAST(date_parse(opportunity_created_date, '%Y-%m-%d') AS DATE))
            BETWEEN DATE '${start_date}' AND DATE '${end_date}'
        GROUP BY "opportunity source code";  `;
    // Set the parameters for Athena query execution using environment variables for configuration
    const params = {
        QueryString: query,
        QueryExecutionContext: {
            Database: process.env.ATHENA_DATABASE
        },
        ResultConfiguration: {
            OutputLocation: process.env.ATHENA_OUTPUT_LOCATION
        }
    };
    try {
        // Start the query execution
        const {QueryExecutionId} = await athena.startQueryExecution(params).promise();
        console.log(`Query submitted successfully. Execution ID: ${QueryExecutionId}`);

        // Poll for query status until it is no longer RUNNING or QUEUED
        let status = 'RUNNING';
        while (status === 'RUNNING' || status === 'QUEUED') {
            const {
                QueryExecution: {Status}
            } = await athena.getQueryExecution({QueryExecutionId}).promise();
            status = Status.State;
            console.log(`Current query status: ${status}`);
            if (status === 'RUNNING' || status === 'QUEUED') {
                await sleep(2000); // Wait for 2 seconds before polling again
            }
        }

        // Check query status and process results if the query succeeded
        if (status === 'SUCCEEDED') {
            let results = await athena.getQueryResults({QueryExecutionId}).promise();
            results = transformAthenaResult(results);
            return results;
        } else {
            console.error(`Query did not succeed. Final status: ${status}`);
        }
    } catch (error) {
        console.error('Error running query:', error);
    }
};