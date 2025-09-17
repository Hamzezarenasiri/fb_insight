import { Worker } from 'bullmq';
import { config } from '../../config/env.js';
import { adLibraryTask } from '../../../index.js';

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  password: process.env.REDIS_PASSWORD || undefined,
};

export const adLibraryWorker = new Worker('run-ad-library', async (job) => {
  await adLibraryTask(job.data);
}, { connection });


