export function logProgress(stage, meta = {}, context = {}) {
  try {
    const payload = { ts: new Date().toISOString(), stage, ...context, ...meta };
    console.log(JSON.stringify(payload));
  } catch (e) {
    console.log(`[logProgress:${stage}]`, meta, context);
  }
}

export function startTimer() {
  return process.hrtime.bigint();
}

export function elapsedMs(start) {
  try {
    const diff = process.hrtime.bigint() - start;
    return Number(diff / 1000000n);
  } catch {
    return 0;
  }
}


