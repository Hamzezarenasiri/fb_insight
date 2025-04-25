import {findDocuments} from "./mongodb.js";
import axios from "axios";

const fluxAPIBaseUrl = "https://flux-api.afarin.top";
const fluxAPIkey = process.env.FLUX_STATIC_API_KEY;

export async function tagging(importListId, clientId, ai) {
    const assets_ids_tagging = (await findDocuments(
        "metrics",
        {
            client_id: clientId,
            import_list_id: importListId,
        },
        {asset_id: 1, _id: 0}
    )).map((doc) => doc.asset_id.toString());
    const payload = {
        ai: ai,
        asset_ids: assets_ids_tagging,
        imported_list_id: importListId,
        force_update_tags: false,
        force_update_description: false,
        force_update_transcription: false
    }
    return await axios.post(
        `${fluxAPIBaseUrl}/tagging-task/bulk_tag`,
        payload,
        {
            headers: {
                'x-api-key': fluxAPIkey,
                'Content-Type': 'application/json',
            }
        }
    )
}