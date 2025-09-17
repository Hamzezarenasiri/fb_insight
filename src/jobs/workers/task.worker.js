import { Worker } from 'bullmq';
import { config } from '../../config/env.js';
import { mainTask } from '../../../index.js';

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  password: process.env.REDIS_PASSWORD || undefined,
};

export const taskWorker = new Worker('run-task', async (job) => {
  await mainTask(job.data);
}, { connection });


