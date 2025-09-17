export const default_schema = [
  { "key": "Ad_Name", "is_default": true, "title": "Ad Name", "description": "Ad Name", "required": true, "type": "text", "similar_dictionary": ["Ad Name"], "order_preference": "decs", "format": "text", "formula": "N/A" },
  { "key": "impressions", "is_default": true, "title": "Impressions", "description": "Number of impressions", "required": true, "type": "integer", "similar_dictionary": ["impressions"], "order_preference": "decs", "format": "number", "formula": "N/A" },
  { "key": "reach", "is_default": true, "title": "Reach", "description": "Unique people reached", "required": true, "type": "integer", "similar_dictionary": ["reach"], "order_preference": "decs", "format": "number", "formula": "N/A" },
  { "key": "ctr", "is_default": true, "title": "CTR", "description": "Click-through rate", "required": false, "type": "float", "similar_dictionary": ["ctr"], "order_preference": "decs", "format": "percent", "formula": "N/A" },
  { "key": "frequency", "is_default": true, "title": "Frequency", "description": "Average impressions per person", "required": false, "type": "float", "similar_dictionary": ["frequency"], "order_preference": "decs", "format": "number", "formula": "N/A" },
  { "key": "spend", "is_default": true, "title": "Spend", "description": "Ad spend", "required": true, "type": "float", "similar_dictionary": ["spend"], "order_preference": "decs", "format": "currency", "formula": "N/A" },
  { "key": "cpp", "is_default": true, "title": "CPP", "description": "Cost per purchase", "required": false, "type": "float", "similar_dictionary": ["cpp"], "order_preference": "decs", "format": "currency", "formula": "N/A" },
  { "key": "cpm", "is_default": true, "title": "CPM", "description": "Cost per mille", "required": false, "type": "float", "similar_dictionary": ["cpm"], "order_preference": "decs", "format": "currency", "formula": "N/A" },
  { "key": "link_click", "is_default": true, "title": "Link Clicks", "description": "Number of link clicks", "required": false, "type": "integer", "similar_dictionary": ["link clicks"], "order_preference": "decs", "format": "number", "formula": "N/A" },
  { "key": "purchase", "is_default": true, "title": "Purchases", "description": "Number of purchases", "required": false, "type": "integer", "similar_dictionary": ["purchase"], "order_preference": "decs", "format": "number", "formula": "N/A" },
  { "key": "result", "is_default": true, "title": "Result", "description": "Objective result", "required": false, "type": "float", "similar_dictionary": ["result"], "order_preference": "decs", "format": "number", "formula": "N/A" },
];

export function buildClientSchema(base, accountId) {
  if (["act_70970029", "act_1474898293329309"].includes(accountId)) {
    return base.concat([
      { "key": "cpgya", "is_default": true, "title": "CPGYA", "description": "Cost per GYA", "required": false, "type": "float", "similar_dictionary": ["Cost per Gya"], "order_preference": "acs", "format": "currency", "formula": "spend / (green_appts + yellow_appts)" },
      { "key": "l2a", "is_default": true, "title": "L2A", "description": "Lead to Appointment", "required": false, "type": "float", "similar_dictionary": ["Lead to Appt", "L2A"], "order_preference": "decs", "format": "percent", "formula": "appts / lead" },
      { "key": "l2s", "is_default": true, "title": "L2S", "description": "Lead to Sale", "required": false, "type": "float", "similar_dictionary": ["Lead to Sale", "L2S"], "order_preference": "decs", "format": "percent", "formula": "sold / lead" },
      { "key": "l2c", "is_default": true, "title": "L2C", "description": "Lead to Conversion", "required": false, "type": "float", "similar_dictionary": ["Lead to Conversion"], "order_preference": "decs", "format": "percent", "formula": "show / lead" },
      { "key": "s2s", "is_default": true, "title": "S2S", "description": "Stage 2 to Sale conversion", "required": false, "type": "float", "similar_dictionary": ["S2S Conversion"], "order_preference": "decs", "format": "percent", "formula": "sold / show" },
      { "key": "s2a", "is_default": true, "title": "S2A", "description": "Stage 2 to Appointment conversion", "required": false, "type": "float", "similar_dictionary": ["S2A Conversion"], "order_preference": "decs", "format": "percent", "formula": "show / appts" },
      { "key": "gya", "is_default": true, "title": "GYA", "description": "GYA metric", "required": false, "type": "float", "similar_dictionary": ["GYA"], "order_preference": "decs", "format": "percent", "formula": "(green_appts + yellow_appts) / appts" },
      { "key": "gyv", "is_default": true, "title": "GYV", "description": "GYV metric", "required": false, "type": "float", "similar_dictionary": ["GYV"], "order_preference": "acs", "format": "number", "formula": "green_appts + yellow_appts" },
      { "key": "cpsold", "is_default": true, "title": "CPSOLD", "description": "Cost per sold", "required": false, "type": "float", "similar_dictionary": ["Cost per Sold"], "order_preference": "acs", "format": "currency", "formula": "spend / sold" },
      { "key": "cpshow", "is_default": true, "title": "CPSHOW", "description": "Cost per show", "required": false, "type": "float", "similar_dictionary": ["Cost per Show"], "order_preference": "acs", "format": "currency", "formula": "spend / show" },
      { "key": "cpappts", "is_default": true, "title": "CPAPPTS", "description": "Cost per appointment", "required": false, "type": "float", "similar_dictionary": ["Cost per Appointment"], "order_preference": "acs", "format": "currency", "formula": "spend / appts" },
      { "key": "lead_cvr", "is_default": true, "title": "Lead CVR", "description": "Lead conversion rate", "required": false, "type": "float", "similar_dictionary": ["Lead Conversion Rate"], "order_preference": "decs", "format": "percent", "formula": "lead / link_clicks" },
      { "key": "sold", "is_default": true, "title": "SOLD", "description": "Number of final sales", "required": false, "type": "integer", "similar_dictionary": ["Sales", "Sold"], "order_preference": "acs", "format": "number", "formula": "N/A" },
      { "key": "show", "is_default": true, "title": "SHOW", "description": "Customers showing up", "required": false, "type": "integer", "similar_dictionary": ["Show"], "order_preference": "acs", "format": "number", "formula": "N/A" },
      { "key": "appts", "is_default": true, "title": "APPTS", "description": "Appointments", "required": false, "type": "integer", "similar_dictionary": ["Appointments"], "order_preference": "acs", "format": "number", "formula": "N/A" },
      { "key": "green_appts", "is_default": true, "title": "Green Appointments", "description": "Green Appointments", "required": false, "type": "float", "similar_dictionary": ["Green Appointments"], "order_preference": "decs", "format": "number", "formula": "N/A" },
      { "key": "yellow_appts", "is_default": true, "title": "Yellow Appointments", "description": "Yellow Appointments", "required": false, "type": "float", "similar_dictionary": ["Yellow Appointments"], "order_preference": "decs", "format": "number", "formula": "N/A" },
      { "key": "red_appts", "is_default": true, "title": "Red Appointments", "description": "Red Appointments", "required": false, "type": "float", "similar_dictionary": ["Red Appointments"], "order_preference": "acs", "format": "number", "formula": "N/A" },
    ]);
  }
  return base;
}


