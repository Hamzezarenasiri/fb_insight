import axios from 'axios';
import { logProgress, startTimer, elapsedMs } from './logger.js';

function redactUrl(url) {
  try {
    const u = new URL(url);
    if (u.searchParams.has('access_token')) u.searchParams.set('access_token', '***');
    return u.toString();
  } catch {
    return url.replace(/access_token=[^&]+/i, 'access_token=***');
  }
}

export async function sendHttpRequest({ url, method = 'GET', headers = {}, body = null, timeout = 180000 }) {
  const maxAttempts = 8;
  let attempt = 0;
  while (attempt < maxAttempts) {
    try {
      const t0 = startTimer();
      const response = await axios({ url, method, headers, data: body, timeout, validateStatus: () => true });
      const status = response.status;
      if (status >= 200 && status < 300) {
        logProgress('http.success', { method, status, duration_ms: elapsedMs(t0) }, { url: redactUrl(url) });
        return response.data;
      }
      if ([408, 429, 500, 502, 503, 504].includes(status)) {
        attempt++;
        const backoff = Math.min(1000 * 2 ** attempt, 15000);
        logProgress('http.retry', { method, status, attempt, backoff_ms: backoff }, { url: redactUrl(url) });
        await new Promise(r => setTimeout(r, backoff));
        continue;
      }
      logProgress('http.error', { method, status }, { url: redactUrl(url) });
      throw new Error(`HTTP ${status}: ${response.statusText}`);
    } catch (err) {
      attempt++;
      if (attempt >= maxAttempts) {
        logProgress('http.fail', { method, error: String(err?.message || err), attempt }, { url: redactUrl(url) });
        throw err;
      }
      const backoff = Math.min(1000 * 2 ** attempt, 15000);
      logProgress('http.catch.retry', { method, error: String(err?.message || err), attempt, backoff_ms: backoff }, { url: redactUrl(url) });
      await new Promise(r => setTimeout(r, backoff));
    }
  }
}


