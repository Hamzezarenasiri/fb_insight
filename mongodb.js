import {MongoClient} from "mongodb";

const uri = process.env.MONGODB_URI;
const client = new MongoClient(uri, {
    family: 4  // Force IPv4
});
const dbName = 'FluxDB';

async function connectToCollection(collectionName) {
    try {
        await client.connect();
        const database = client.db(dbName);
        return database.collection(collectionName);
    } catch (error) {
        console.error("Error connecting to MongoDB: ", error);
        throw error;
    }
}

export async function findDocuments(collectionName, query, projection = {}, sort = {}) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.find(query, {projection}).sort(sort).toArray();
    } catch (error) {
        console.error("Error finding documents: ", error);
        throw error;
    }
}

export async function insertMany(collectionName, documents) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.insertMany(documents);
    } catch (error) {
        console.error("Error inserting many documents: ", error);
        throw error;
    }
}

export async function findOneDocument(collectionName, query, projection = {}, sort = {}) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.findOne(query, {projection, sort});
    } catch (error) {
        console.error("Error finding one document: ", error);
        throw error;
    }
}

export async function aggregateDocuments(collectionName, pipeline) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.aggregate(pipeline).toArray();
    } catch (error) {
        console.error("Error aggregating documents: ", error);
        throw error;
    }
}

export async function updateOneDocument(collectionName, filter, update, options = {upsert: true}) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.updateOne(filter, update, options);
    } catch (error) {
        console.error("Error updating one document: ", error);
        throw error;
    }
}

export async function updateManyDocuments(collectionName, filter, update) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.updateMany(filter, update);
    } catch (error) {
        console.error("Error updating many documents: ", error);
        throw error;
    }
}

export async function insertOneDocument(collectionName, document) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.insertOne(document);
    } catch (error) {
        console.error("Error inserting one document: ", error);
        throw error;
    }
}

async function findAndUpdate(collectionName, filter, update, options = {}) {
    try {
        const collection = await connectToCollection(collectionName);
        return await collection.findOneAndUpdate(filter, update, options);
    } catch (error) {
        console.error("Error finding and updating document: ", error);
        throw error;
    }
}