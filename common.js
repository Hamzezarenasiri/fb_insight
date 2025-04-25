import {updateOneDocument} from "./mongodb.js";

export async function saveFacebookImportStatus(uuid, updateValues) {
    const collectionName = 'facebook_imports';
    const filter = {uuid};
    updateValues.updatedAt = new Date()

    let update = {
        $set: updateValues
    };
    if ('status' in updateValues) {
        update.$addToSet = {status_history: updateValues.status}
    }
    try {
        const result = await updateOneDocument(collectionName, filter, update);
        console.log("Facebook import status saved successfully:", result);
    } catch (error) {
        console.error("Failed to save Facebook import status:", error);
    }
}