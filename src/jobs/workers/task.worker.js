import { Worker } from 'bullmq';
import { config } from '../../config/env.js';
import { mainTask } from '../../../index.js';
import { logProgress, startTimer, elapsedMs } from '../../utils/logger.js';

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  password: process.env.REDIS_PASSWORD || undefined,
};

export const taskWorker = new Worker('run-task', async (job) => {
  const t0 = startTimer();
  const ctx = { jobId: job.id, uuid: job.data?.uuid, queue: 'run-task' };
  logProgress('worker.task.start', { dataKeys: Object.keys(job.data || {}) }, ctx);
  try {
    await mainTask(job.data);
    logProgress('worker.task.done', { duration_ms: elapsedMs(t0) }, ctx);
  } catch (err) {
    logProgress('worker.task.error', { duration_ms: elapsedMs(t0), error: String(err?.message || err) }, ctx);
    throw err;
  }
}, { connection, concurrency: 2 });


