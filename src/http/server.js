// Native HTTP server initialization and request routing
import http from "node:http";
import { attachMemoryProfiler } from "./middlewares/memory-profiler.js";
import {
    sendJson,
    initSession,
    uploadChunk,
    probeSession,
    downloadFile,
    abortSession
} from "./controllers/upload-controller.js";
import { healthCheck } from "../db/client.js";

/**
 * Attaches standard CORS headers to responses
 */
function setCorsHeaders(res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, HEAD, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Content-Range, Range, X-Chunk-SHA256, Authorization');
    res.setHeader('Access-Control-Expose-Headers', 'Range, Content-Range, X-Upload-Status, X-Uploaded-Bytes, X-Total-Bytes, Location');
}

/**
 * Creates and configures the native HTTP server
 */
export function createServer() {
    return http.createServer(async (req, res) => {
        // 1. Attach memory telemetry
        attachMemoryProfiler(req, res);

        // 2. Set CORS headers
        setCorsHeaders(res);

        // 3. Handle Preflight OPTIONS
        if (req.method === 'OPTIONS') {
            res.writeHead(204);
            res.end();
            return;
        }

        const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
        const pathname = url.pathname;

        try {
            // Healthcheck endpoint
            if (pathname === '/health' && req.method === 'GET') {
                const isHealthy = await healthCheck();
                return sendJson(res, isHealthy ? 200 : 503, {
                    status: isHealthy ? 'healthy' : 'unhealthy',
                    timestamp: new Date().toISOString(),
                });
            }

            // POST /api/uploads - Initialize Session
            if (pathname === '/api/uploads' && req.method === 'POST') {
                return await initSession(req, res);
            }

            // Route matching: /api/uploads/:id
            const uploadMatch = pathname.match(/^\/api\/uploads\/([a-zA-Z0-9_-]+)$/);
            if (uploadMatch) {
                const sessionId = uploadMatch[1];

                if (req.method === 'PATCH') {
                    return await uploadChunk(req, res, sessionId);
                }
                if (req.method === 'HEAD' || req.method === 'GET') {
                    return await probeSession(req, res, sessionId);
                }
                if (req.method === 'DELETE') {
                    return await abortSession(req, res, sessionId);
                }
            }

            // Route matching: /api/downloads/:id
            const downloadMatch = pathname.match(/^\/api\/downloads\/([a-zA-Z0-9_-]+)$/);
            if (downloadMatch && req.method === 'GET') {
                const sessionId = downloadMatch[1];
                return await downloadFile(req, res, sessionId);
            }

            // 404 Route Not Found
            return sendJson(res, 404, {
                error: 'Not Found',
                method: req.method,
                path: pathname,
            });
        } catch (error) {
            console.error('Unhandled request error:', error);
            if (!res.headersSent) {
                return sendJson(res, 500, {
                    error: 'Internal Server Error',
                    message: error.message,
                });
            }
        }
    });
}

export default createServer;
