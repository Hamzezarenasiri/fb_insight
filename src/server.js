import dotenv from 'dotenv';
import * as Sentry from '@sentry/node';
import { app } from './app.js';

dotenv.config();

Sentry.init({ dsn: process.env.SENTRY_DSN });

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API listening on ${PORT}`);
});


