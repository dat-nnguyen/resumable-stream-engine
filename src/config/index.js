"use strict";

import path from "path";
import fs from "node:fs";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

try {
    process.loadEnvFile?.(path.resolve(__dirname, "../../.env"));
} catch {

}

const STORAGE_DIR = path.resolve(process.cwd(), process.env.STORAGE_DIR || "./src/storage");
const databaseUrl = process.env.DATABASE_URL || 'postgresql://postgres@localhost:5432/resumable-stream-engine';
if (!fs.existsSync(STORAGE_DIR)) {
    fs.mkdirSync(STORAGE_DIR, { recursive: true });
}

export const config = Object.freeze({
    server: {
        port: process.env.PORT || 3000,
        maxConcurrentUploads: parseInt(process.env.MAX_CONCURRENT_UPLOADS || '5', 10),
        chunkHighWaterMark: parseInt(process.env.CHUNK_HIGH_WATER_MARK || '65536', 10),
        streamInactivityTimeoutMs: parseInt(process.env.STREAM_INACTIVITY_TIMEOUT_MS || '30000', 10),
        fsyncOnChunk: process.env.FSYNC_ON_CHUNK !== 'false',
    },
    storage: {
        storageDir: STORAGE_DIR,
    },
    database: {
        url: databaseUrl,
        pool: {
            max: parseInt(process.env.DB_POOL_MAX || '20', 10),
            connectionTimeoutMillis: parseInt(process.env.DB_CONNECTION_TIMEOUT_MILLIS || '5000', 10),
        }
    }
});

export default config;