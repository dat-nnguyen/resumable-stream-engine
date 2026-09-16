// Application entrypoint and graceful shutdown lifecycle
import config from "./config/index.js";
import { createServer } from "./http/server.js";
import { healthCheck, closePool } from "./db/client.js";

async function bootstrap() {
    console.log('='.repeat(60));
    console.log(' Starting Resumable File Streaming Engine with Backpressure');
    console.log('='.repeat(60));

    // 1. Verify database connectivity
    const isDbHealthy = await healthCheck();
    if (!isDbHealthy) {
        console.error('FATAL: Could not connect to PostgreSQL database. Exiting.');
        process.exit(1);
    }
    console.log('✓ PostgreSQL connection established and healthy.');
    console.log(`✓ Storage directory: ${config.storage.storageDir}`);
    console.log(`✓ HighWaterMark buffer window: ${config.server.chunkHighWaterMark} bytes`);

    // 2. Start native HTTP server
    const server = createServer();
    const port = config.server.port;

    server.listen(port, () => {
        console.log(`✓ HTTP Server listening on port ${port}`);
        console.log(`  - Upload endpoint:   POST  http://localhost:${port}/api/uploads`);
        console.log(`  - Chunk upload:      PATCH http://localhost:${port}/api/uploads/:id`);
        console.log(`  - Probe offset:      HEAD  http://localhost:${port}/api/uploads/:id`);
        console.log(`  - Download file:     GET   http://localhost:${port}/api/downloads/:id`);
        console.log(`  - Healthcheck:       GET   http://localhost:${port}/health`);
        console.log('='.repeat(60));
    });

    // 3. Graceful shutdown handler
    let isShuttingDown = false;
    async function gracefulShutdown(signal) {
        if (isShuttingDown) return;
        isShuttingDown = true;

        console.log(`\nReceived ${signal}. Shutting down gracefully...`);
        server.close(async () => {
            console.log('✓ HTTP server closed to new connections.');
            try {
                await closePool();
                console.log('✓ PostgreSQL connection pool drained and closed.');
                process.exit(0);
            } catch (err) {
                console.error('Error closing database pool:', err);
                process.exit(1);
            }
        });

        // Force shutdown if cleanup hangs past 10 seconds
        setTimeout(() => {
            console.error('Forcefully terminating after timeout.');
            process.exit(1);
        }, 10000).unref();
    }

    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
}

bootstrap().catch((err) => {
    console.error('Fatal error during application startup:', err);
    process.exit(1);
});
