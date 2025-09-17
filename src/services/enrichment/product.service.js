import { findDocuments, aggregateDocuments, insertMany, updateOneDocument, insertOneDocument, findOneDocument } from '../../repositories/mongo/common.js';
import { getFbAdPreview, getPropsOfSource } from './preview.service.js';

export async function updateMessagesAndLinks(uuid, clientId) {
  const client = await findOneDocument('clients', { _id: clientId });
  const accessToken = client.fb_config?.access_token || {};
  const assets = await findDocuments('assets', { client_id: clientId, ad_id: { $exists: true }, 'meta_data.fb_data.message': { $exists: false }, 'meta_data.fb_data.product_link': { $exists: false } }, { _id: 1, ad_id: 1 });
  const startProgress = 20; const endProgress = 50; const totalTasks = assets.length; const progressIncrement = (endProgress - startProgress) / totalTasks; let currentProgress = startProgress;
  for (const asset of assets) {
    const url = await getFbAdPreview(asset.ad_id, accessToken);
    const props = await getPropsOfSource(url);
    await updateOneDocument('assets', { _id: asset._id }, { $set: { 'meta_data.fb_data': { message: props.message, product_link: props.product_link, product_url: props.product_url, preview_data: props.preview_data } } });
    currentProgress += progressIncrement;
  }
}

export async function generateProduct(uuid, clientId, agencyId) {
  const tags = await aggregateDocuments('tags', [
    { $match: { client_id: clientId } },
    { $sort: { _id: 1 } },
    { $project: { _id: 0, category: 1, tag: 1, description: 1 } },
    { $group: { _id: '$category', pairs: { $push: { k: '$tag', v: '$description' } } } },
    { $project: { _id: 0, category: '$_id', tags: { $arrayToObject: { $map: { input: { $filter: { input: '$pairs', as: 'p', cond: { $and: [ { $ne: ['$$p.k', null] }, { $ne: ['$$p.k', ''] }, { $ne: ['$$p.v', null] } ] } } }, as: 'p', in: { k: '$$p.k', v: '$$p.v' } } } } } },
    { $group: { _id: null, categories: { $push: { k: '$category', v: '$tags' } } } },
    { $project: { _id: 0, categories: { $map: { input: { $filter: { input: '$categories', as: 'c', cond: { $and: [ { $ne: ['$$c.k', null] }, { $ne: ['$$c.k', ''] }, { $ne: ['$$c.v', null] } ] } } }, as: 'c', in: { k: '$$c.k', v: '$$c.v' } } } } },
    { $replaceRoot: { newRoot: { $arrayToObject: '$categories' } } }
  ]);
  if (tags.length > 1) return;
  const categories = await aggregateDocuments('tags_categories', [
    { $match: { client_id: clientId } },
    { $sort: { _id: 1 } },
    { $group: { _id: null, categoryDescriptions: { $push: { k: '$category', v: { $ifNull: ['$description', ''] } } } } },
    { $project: { _id: 0, categoryDescriptions: { $map: { input: { $filter: { input: '$categoryDescriptions', as: 'kv', cond: { $and: [ { $ne: ['$$kv.k', null] }, { $ne: ['$$kv.k', ''] }, { $ne: ['$$kv.v', null] } ] } } }, as: 'kv', in: { k: '$$kv.k', v: '$$kv.v' } } } } },
    { $replaceRoot: { newRoot: { $arrayToObject: '$categoryDescriptions' } } }
  ]);
  // This function previously orchestrated OpenAI extraction calls; leave implementation to enrichment.openai or calling code.
  return { categories: categories[0] };
}


