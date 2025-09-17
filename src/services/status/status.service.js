import { findAndUpdate } from '../../repositories/mongo/common.js';

export async function saveFacebookImportStatus(uuid, updateValues) {
  const collectionName = 'facebook_imports';
  const filter = { uuid };
  const update = { $set: { ...updateValues, updatedAt: new Date() }, $setOnInsert: { createdAt: new Date(), uuid } };
  return findAndUpdate(collectionName, filter, update, { upsert: true, returnDocument: 'after' });
}


