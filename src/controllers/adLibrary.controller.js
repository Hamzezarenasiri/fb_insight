import { adLibraryQueue } from '../jobs/queues.js';

export async function runAdLibraryController(req, res) {
  const params = req.validatedBody || req.body;

  res.status(200).send({success: true, message: 'Task has been queued for processing'});
  await adLibraryQueue.add('run-ad-library', params, { removeOnComplete: true, removeOnFail: false, attempts: 3, backoff: { type: 'exponential', delay: 2000 } });
}


