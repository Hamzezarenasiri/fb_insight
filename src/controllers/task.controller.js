import { taskQueue } from '../jobs/queues.js';

export async function runTaskController(req, res) {
  const auth = req.get('authorization') || '';
  if (!auth) return res.status(401).json({ error: 'missing Authorization' });

  const params = req.validatedBody || req.body;

  res.status(200).send({success: true, message: 'Task has been queued for processing'});
  await taskQueue.add('run-task', params, {
    jobId: params.uuid,
    removeOnComplete: true,
    removeOnFail: false,
    attempts: 1
  });
}


