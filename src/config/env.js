import dotenv from 'dotenv';
dotenv.config();

export const config = {
  port: process.env.PORT || 3000,
  mongoUri: process.env.mongodb_uri,
  sentryDsn: process.env.SENTRY_DSN,
  staticToken: process.env.STATIC_TOKEN,
  aws: {
    region: process.env.AWS_REGION || 'us-east-1',
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    athenaDatabase: process.env.ATHENA_DATABASE,
    athenaOutput: process.env.ATHENA_OUTPUT_LOCATION,
  },
  flux: {
    apiBase: process.env.FLUX_API_BASE || 'https://flux-api.afarin.top',
    apiKey: process.env.FLUX_STATIC_API_KEY,
  },
  ghl: {
    apiBase: process.env.GHL_API_BASE || 'https://services.leadconnectorhq.com',
    apiVersion: process.env.GHL_API_VERSION || '2021-07-28',
  },
};

