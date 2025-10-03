import { taskQueue } from '../jobs/queues.js';
import { config } from '../config/env.js';
import { findOneDocument } from '../repositories/mongo/common.js';
import { ObjectId } from 'mongodb';

export async function runTaskController(req, res) {
  const params = req.validatedBody || req.body || {};
  const authHeader = req.get('authorization') || '';
  // Enforce strong auth automatically for HIPAA clients
  try {
    const clientId = params?.clientId;
    let hipaaClient = false;
    if (clientId) {
      let _id = clientId;
      try { _id = new ObjectId(String(clientId)); } catch {}
      const client = await findOneDocument('clients', { _id }, { compliance: 1, hipaa: 1 });
      hipaaClient = Boolean(client?.compliance?.hipaa || client?.hipaa);
    }
    if (config.hipaa.mode || hipaaClient) {
      // Accept token from Authorization (exact or Bearer), X-API-Key, or body (staticToken/apiKey/token)
      const provided = new Set();
      const rawAuth = (authHeader || '').trim();
      if (rawAuth) {
        provided.add(rawAuth);
        if (rawAuth.toLowerCase().startsWith('bearer ')) {
          provided.add(rawAuth.slice(7).trim());
        }
      }
      const xApiKey = (req.get('x-api-key') || '').trim();
      if (xApiKey) provided.add(xApiKey);
      const bodyToken = (params.staticToken || params.apiKey || params.token || '').toString().trim();
      if (bodyToken) provided.add(bodyToken);

      if (!config.staticToken || ![...provided].includes(config.staticToken)) {
        return res.status(401).json({ error: 'invalid token' });
      }
    } else {
      // Non-HIPAA: keep lightweight check for presence of Authorization header to avoid open endpoint
      if (!authHeader) return res.status(401).json({ error: 'missing Authorization' });
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


