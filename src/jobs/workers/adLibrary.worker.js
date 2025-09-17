import { Worker } from 'bullmq';
import { config } from '../../config/env.js';
import { adLibraryTask } from '../../../index.js';
import { logProgress, startTimer, elapsedMs } from '../../utils/logger.js';

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  password: process.env.REDIS_PASSWORD || undefined,
};

export const adLibraryWorker = new Worker('run-ad-library', async (job) => {
  const t0 = startTimer();
  const ctx = { jobId: job.id, uuid: job.data?.uuid, queue: 'run-ad-library' };
  logProgress('worker.adlib.start', { dataKeys: Object.keys(job.data || {}) }, ctx);
  try {
    await adLibraryTask(job.data);
    logProgress('worker.adlib.done', { duration_ms: elapsedMs(t0) }, ctx);
  } catch (err) {
    logProgress('worker.adlib.error', { duration_ms: elapsedMs(t0), error: String(err?.message || err) }, ctx);
    throw err;
  }
}, { connection, concurrency: 2 });


