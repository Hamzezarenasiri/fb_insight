import { taskQueue } from '../jobs/queues.js';
import { config } from '../config/env.js';
import { findOneDocument } from '../repositories/mongo/common.js';
import { ObjectId } from 'mongodb';

export async function runTaskController(req, res) {
  const auth = req.get('authorization') || '';
  if (!auth) return res.status(401).json({ error: 'missing Authorization' });
  // Enforce strong auth automatically for HIPAA clients
  try {
    const params = req.validatedBody || req.body || {};
    const clientId = params?.clientId;
    let hipaaClient = false;
    if (clientId) {
      let _id = clientId;
      try { _id = new ObjectId(String(clientId)); } catch {}
      const client = await findOneDocument('clients', { _id }, { compliance: 1, hipaa: 1 });
      hipaaClient = Boolean(client?.compliance?.hipaa || client?.hipaa);
    }
    if (config.hipaa.mode || hipaaClient) {
      const token = (req.get('authorization') || '').trim();
      if (!config.staticToken || token !== config.staticToken) {
        return res.status(401).json({ error: 'invalid token' });
      }
    }
  } catch {}

  const params = req.validatedBody || req.body;

  res.status(200).send({success: true, message: 'Task has been queued for processing'});
  await taskQueue.add('run-task', params, {
    jobId: params.uuid,
    removeOnComplete: true,
    removeOnFail: false,
    attempts: 1
  });
}


