import { Queue } from 'bullmq';
import { config } from '../config/env.js';

const connection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  password: process.env.REDIS_PASSWORD || undefined,
};

export const taskQueue = new Queue('run-task', { connection });
export const adLibraryQueue = new Queue('run-ad-library', { connection });


