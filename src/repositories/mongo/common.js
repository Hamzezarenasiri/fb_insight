import { getCollection } from './client.js';

export async function findDocuments(collectionName, query, projection = {}, sort = {}) {
  const col = await getCollection(collectionName);
  return col.find(query, { projection }).sort(sort).toArray();
}

export async function findOneDocument(collectionName, query, projection = {}, sort = {}) {
  const col = await getCollection(collectionName);
  return col.findOne(query, { projection, sort });
}

export async function insertOneDocument(collectionName, document) {
  const col = await getCollection(collectionName);
  return col.insertOne(document);
}

export async function insertMany(collectionName, documents) {
  const col = await getCollection(collectionName);
  return col.insertMany(documents);
}

export async function updateOneDocument(collectionName, filter, update, options = { upsert: true }) {
  const col = await getCollection(collectionName);
  return col.updateOne(filter, update, options);
}

export async function updateManyDocuments(collectionName, filter, update) {
  const col = await getCollection(collectionName);
  return col.updateMany(filter, update);
}

export async function findAndUpdate(collectionName, filter, update, options = {}) {
  const col = await getCollection(collectionName);
  return col.findOneAndUpdate(filter, update, options);
}

export async function aggregateDocuments(collectionName, pipeline) {
  const col = await getCollection(collectionName);
  return col.aggregate(pipeline).toArray();
}


