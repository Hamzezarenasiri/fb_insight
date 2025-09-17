import { findAndUpdate } from '../../repositories/mongo/common.js';

export async function saveFacebookImportStatus(uuid, updateValues) {
  const collectionName = 'facebook_imports';
  const filter = { uuid };
  // Prevent conflicts: never allow 'createdAt'/'uuid' from caller into $set
  const { createdAt: _createdAt, uuid: _uuid, ...safeValues } = updateValues || {};
  const update = {
    $set: { ...safeValues, updatedAt: new Date() },
    $setOnInsert: { createdAt: new Date(), uuid }
  };
  return findAndUpdate(collectionName, filter, update, { upsert: true, returnDocument: 'after' });
}


