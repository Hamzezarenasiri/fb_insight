export function transformObjects(data) {
  return data.map(obj => ({
    [obj.key]: {
      key: obj.key,
      is_default: obj.is_default,
      title: obj.title,
      description: obj.description,
      required: obj.required,
      type: obj.type,
      format: obj.format || null,
      formula: obj.formula || null,
      similar_dictionary: obj.similar_dictionary || [],
    },
  }));
}

function jaroWinklerDistance(s1, s2) {
  let m = 0;
  if (s1.length === 0 || s2.length === 0) return 0;
  if (s1 === s2) return 1;
  const range = Math.floor(Math.max(s1.length, s2.length) / 2) - 1;
  const s1Matches = new Array(s1.length);
  const s2Matches = new Array(s2.length);
  for (let i = 0; i < s1.length; i++) {
    const low = i >= range ? i - range : 0;
    const high = i + range <= s2.length - 1 ? i + range : s2.length - 1;
    for (let j = low; j <= high; j++) {
      if (!s1Matches[i] && !s2Matches[j] && s1[i] === s2[j]) { m++; s1Matches[i] = s2Matches[j] = true; break; }
    }
  }
  if (m === 0) return 0;
  let k = 0; let numTrans = 0;
  for (let i = 0; i < s1.length; i++) {
    if (s1Matches[i]) {
      for (let j = k; j < s2.length; j++) { if (s2Matches[j]) { k = j + 1; break; } }
      if (s1[i] !== s2[k - 1]) numTrans++;
    }
  }
  let weight = (m / s1.length + m / s2.length + (m - numTrans / 2) / m) / 3;
  const l = Math.min(4, [...s1].findIndex((c, i) => c !== s2[i]) + 1);
  const p = 0.1;
  if (weight > 0.7) weight += l * p * (1 - weight);
  return weight;
}

export function findMostSimilarKey(item, array1) {
  let maxSimilarity = -1;
  let mostSimilarKey = null;
  array1.forEach(obj => {
    const key = Object.keys(obj)[0];
    let similarity = 0;
    if (obj[key].similar_dictionary.length !== 0) {
      obj[key].similar_dictionary.forEach(similarItem => {
        similarity = Math.max(similarity, jaroWinklerDistance(item.toLowerCase(), similarItem.toLowerCase()));
      });
    } else {
      similarity = jaroWinklerDistance(item.toLowerCase(), key.toLowerCase());
    }
    if (similarity > maxSimilarity) { maxSimilarity = similarity; mostSimilarKey = key; }
  });
  return { key: mostSimilarKey, similarity: maxSimilarity };
}

export function getPercentFields(arr) {
  return arr.filter(item => item.format === 'percent').map(item => item.key);
}


