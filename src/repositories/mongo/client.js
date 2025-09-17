import { MongoClient } from 'mongodb';
import { config } from '../../config/env.js';

const client = new MongoClient(config.mongoUri, { family: 4 });
const dbName = 'FluxDB';

export async function getDb() {
  await client.connect();
  return client.db(dbName);
}

export async function getCollection(name) {
  const db = await getDb();
  return db.collection(name);
}


