import { findDocuments, aggregateDocuments, insertMany, updateOneDocument, insertOneDocument, findOneDocument } from '../../repositories/mongo/common.js';
import { getFbAdPreview, getPropsOfSource } from './preview.service.js';
import { logProgress } from '../../utils/logger.js';

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
  logProgress('enrichment.products.build.start', { clientId: String(clientId) }, { uuid });
  let tags;
  try {
  tags = await aggregateDocuments('tags', [
    { $match: { client_id: clientId } },
    { $sort: { _id: 1 } },
    { $project: { _id: 0, category: 1, tag: 1, description: 1 } },
    { $group: { _id: '$category', pairs: { $push: { k: '$tag', v: '$description' } } } },
    { $project: { _id: 0, category: '$_id', tags: { $arrayToObject: { $map: { input: { $filter: { input: '$pairs', as: 'p', cond: { $and: [ { $ne: ['$$p.k', null] }, { $ne: ['$$p.k', ''] }, { $ne: ['$$p.v', null] } ] } } }, as: 'p', in: { k: '$$p.k', v: '$$p.v' } } } } } },
    { $group: { _id: null, categories: { $push: { k: '$category', v: '$tags' } } } },
    { $project: { _id: 0, categories: { $map: { input: { $filter: { input: '$categories', as: 'c', cond: { $and: [ { $ne: ['$$c.k', null] }, { $ne: ['$$c.k', ''] }, { $ne: ['$$c.v', null] } ] } } }, as: 'c', in: { k: '$$c.k', v: '$$c.v' } } } } },
    { $replaceRoot: { newRoot: { $arrayToObject: '$categories' } } }
  ]);
  } catch (e) {
    logProgress('enrichment.products.build.tags.fallback', { error: String(e?.message || e) }, { uuid });
    const rows = await findDocuments('tags', { client_id: clientId }, { category: 1, tag: 1, description: 1 });
    const categoriesObj = {};
    for (const r of rows) {
      if (!r?.category || !r?.tag || r?.tag === '') continue;
      categoriesObj[r.category] = categoriesObj[r.category] || {};
      if (r.description !== null && r.description !== undefined) categoriesObj[r.category][r.tag] = r.description;
    }
    tags = [categoriesObj];
  }
  if (tags.length > 1) return;
  let categories;
  try {
  categories = await aggregateDocuments('tags_categories', [
    { $match: { client_id: clientId } },
    { $sort: { _id: 1 } },
    { $group: { _id: null, categoryDescriptions: { $push: { k: '$category', v: { $ifNull: ['$description', ''] } } } } },
    { $project: { _id: 0, categoryDescriptions: { $map: { input: { $filter: { input: '$categoryDescriptions', as: 'kv', cond: { $and: [ { $ne: ['$$kv.k', null] }, { $ne: ['$$kv.k', ''] }, { $ne: ['$$kv.v', null] } ] } } }, as: 'kv', in: { k: '$$kv.k', v: '$$kv.v' } } } } },
    { $replaceRoot: { newRoot: { $arrayToObject: '$categoryDescriptions' } } }
  ]);
  } catch (e) {
    logProgress('enrichment.products.build.categories.fallback', { error: String(e?.message || e) }, { uuid });
    const rows = await findDocuments('tags_categories', { client_id: clientId }, { category: 1, description: 1 });
    const catObj = {};
    for (const r of rows) {
      if (!r?.category) continue;
      catObj[r.category] = r.description || '';
    }
    categories = [catObj];
  }
  // This function previously orchestrated OpenAI extraction calls; leave implementation to enrichment.openai or calling code.
  const result = { categories: categories[0] };
  logProgress('enrichment.products.build.done', { categories: Object.keys(result.categories || {}).length }, { uuid });
  return result;
}


