// Per-request memory profiler measuring RSS and V8 Heap metrics
/**
 * Takes a point-in-time snapshot of the Node.js memory footprint.
 */
export function getMemorySnapshot() {
  const mem = process.memoryUsage();
  return {
    raw: mem,
    rssMb: (mem.rss / (1024 * 1024)).toFixed(2),
    heapUsedMb: (mem.heapUsed / (1024 * 1024)).toFixed(2),
    externalMb: (mem.external / (1024 * 1024)).toFixed(2),
  };
}

/**
 * Tracks memory metrics for the lifecycle of an HTTP request.
 */
export function attachMemoryProfiler(req, res) {
  const startMem = getMemorySnapshot();
  const startTime = process.hrtime.bigint();

  res.on('finish', () => {
    const endMem = getMemorySnapshot();
    const durationMs = Number(process.hrtime.bigint() - startTime) / 1_000_000;

    console.log(
      `[TELEMETRY] ${req.method} ${req.url} - ${res.statusCode} | ` +
      `Duration: ${durationMs.toFixed(1)}ms | ` +
      `RSS: ${endMem.rssMb} MB | ` +
      `HeapUsed: ${endMem.heapUsedMb} MB`
    );
  });
}

export default { getMemorySnapshot, attachMemoryProfiler };
