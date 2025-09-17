import axios from 'axios';
import { config } from '../../config/env.js';
import { findDocuments } from '../../repositories/mongo/common.js';

export async function tagging(importListId, clientId, ai) {
  const assets_ids_tagging = (await findDocuments(
    'metrics',
    { client_id: clientId, import_list_id: importListId, asset_id: { $exists: true, $ne: null } },
    { _id: 0, asset_id: 1 }
  )).map((doc) => doc.asset_id.toString());
  const payload = {
    ai,
    asset_ids: assets_ids_tagging,
    imported_list_id: importListId,
    force_update_tags: false,
    force_update_description: false,
    force_update_transcription: false,
  };
  return axios.post(
    `${config.flux.apiBase}/tagging-task/bulk_tag`,
    payload,
    { headers: { 'x-api-key': config.flux.apiKey, 'Content-Type': 'application/json' } }
  );
}


