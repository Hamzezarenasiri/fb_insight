import axios from 'axios';

export async function sendHttpRequest({ url, method = 'GET', headers = {}, body = null, timeout = 180000 }) {
  const maxAttempts = 8;
  let attempt = 0;
  while (attempt < maxAttempts) {
    try {
      const response = await axios({ url, method, headers, data: body, timeout, validateStatus: () => true });
      const status = response.status;
      if (status >= 200 && status < 300) return response.data;
      if ([408, 429, 500, 502, 503, 504].includes(status)) {
        attempt++;
        const backoff = Math.min(1000 * 2 ** attempt, 15000);
        await new Promise(r => setTimeout(r, backoff));
        continue;
      }
      throw new Error(`HTTP ${status}: ${response.statusText}`);
    } catch (err) {
      attempt++;
      if (attempt >= maxAttempts) throw err;
      const backoff = Math.min(1000 * 2 ** attempt, 15000);
      await new Promise(r => setTimeout(r, backoff));
    }
  }
}


