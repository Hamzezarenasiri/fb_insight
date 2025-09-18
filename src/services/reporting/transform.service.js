export function convertToObject(data, ad_objective_field_expr, ad_objective_id, extraFields = []) {
  const expr = ad_objective_field_expr.split('.');
  return data.map((item) => {
    const {
      ad_name, impressions, reach, ctr, frequency, spend, cpp, cpm, post_url,
      ad_id, format, conversion_rate_ranking, engagement_rate_ranking,
      adset_name, campaign_name, ...restOfItem
    } = item;
    const extraFieldsValues = extraFields.reduce((acc, field) => {
      acc[field] = item[field];
      return acc;
    }, {});
    // Prefer inline_link_clicks; fallback to actions.link_click when present
    const linkClicks = item.inline_link_clicks ?? item.actions?.link_click ?? null;
    const extraFieldsValues = extraFields.reduce((acc, field) => {
      acc[field] = item[field];
      return acc;
    }, {});
    return {
      Ad_Name: ad_name || 'null_name',
      impressions: impressions ?? null,
      reach: reach ?? null,
      ctr: ctr ?? null,
      frequency: frequency ?? null,
      spend: spend ?? null,
      cpp: cpp ?? null,
      cpm: cpm ?? null,
      link_clicks: linkClicks,
      link_click: item.actions?.link_click ?? null,
      purchase: item.actions?.purchase ?? null,
      vvr: impressions ? ((item.actions?.video_view ?? 0) / impressions) : 0,
      hold: impressions ? ((item.video_thruplay_watched_actions?.video_view ?? 0) / impressions) : 0,
      cpa: item.cost_per_action_type?.purchase ?? null,
      cvr: linkClicks ? (item?.[expr[0]]?.[expr[1]] ? item[expr[0]][expr[1]] / linkClicks : 0) : null,
      roas: item.purchase_roas?.omni_purchase ?? null,
      cpc: item.cost_per_action_type?.link_click ?? (linkClicks ? spend / linkClicks : null),
      cpl: item.cost_per_action_type?.lead ?? null,
      revenue: item.action_values?.purchase ?? null,
      video_views_3s: item.actions?.video_view ?? null,
      video_views_15s: item.video_thruplay_watched_actions?.video_view ?? null,
      video_avg_time_watched: item.video_avg_time_watched_actions?.video_view ?? null,
      video_p25_watched: item.video_p25_watched_actions?.video_view ?? null,
      video_p50_watched: item.video_p50_watched_actions?.video_view ?? null,
      video_p75_watched: item.video_p75_watched_actions?.video_view ?? null,
      video_p95_watched: item.video_p95_watched_actions?.video_view ?? null,
      video_p100_watched: item.video_p100_watched_actions?.video_view ?? null,
      momentum_rate: item.video_p25_watched_actions?.video_view ? item.video_p75_watched_actions?.video_view / item.video_p25_watched_actions?.video_view : null,
      result: item?.[expr[0]]?.[expr[1]],
      cpr: item?.[expr[0]]?.[expr[1]] ? spend / item[expr[0]][expr[1]] : Infinity,
      post_url, ad_id, format, conversion_rate_ranking, engagement_rate_ranking, campaign_name, adset_name,
      ...extraFieldsValues,
      other_fields: { ...restOfItem },
    };
  });
}

export function findNonEmptyKeys(array) {
  const keysWithValues = new Set();
  array.forEach(obj => {
    for (const [key, value] of Object.entries(obj)) {
      if (value !== null && value !== '') keysWithValues.add(key);
    }
  });
  return Array.from(keysWithValues);
}


