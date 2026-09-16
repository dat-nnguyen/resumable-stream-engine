// Upload controller orchestrating requests with core streams and database
import path from "node:path";
import fs from "node:fs";
import fsp from "node:fs/promises";
import crypto from "node:crypto";
import config from "../../config/index.js";
import { runChunkPipeline } from "../../core/pipeline-runner.js";
import { parseContentRange, parseDownloadRange } from "../middlewares/range-parser.js";
import {
    createSession,
    getSessionById,
    recordChunkProgress,
    updateSessionStatus,
    finalizeSession
} from "../../db/session-repository.js";

/**
 * Sends a structured JSON response with appropriate headers
 */
export function sendJson(res, statusCode, data, headers = {}) {
    const payload = JSON.stringify(data);
    res.writeHead(statusCode, {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
        ...headers,
    });
    res.end(payload);
}

/**
 * Safely parses incoming JSON request stream with size limits
 */
export async function parseJsonBody(req) {
    return new Promise((resolve, reject) => {
        let body = '';
        req.on('data', (chunk) => {
            body += chunk.toString();
            if (body.length > 1024 * 1024) { // 1MB JSON limit
                req.destroy();
                reject(new Error('Payload too large'));
            }
        });

        req.on('end', () => {
            try {
                resolve(body ? JSON.parse(body) : {});
            } catch (err) {
                reject(new Error('Invalid JSON format'));
            }
        });

        req.on('error', reject);
    });
}

/**
 * Computes authoritative full-file SHA-256 via zero-memory stream (ADR-0003)
 */
export async function computeFileSha256(filePath) {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash('sha256');
        const stream = fs.createReadStream(filePath);
        stream.on('data', (chunk) => hash.update(chunk));
        stream.on('end', () => resolve(hash.digest('hex')));
        stream.on('error', reject);
    });
}

/**
 * POST /api/uploads - Initializes an upload session and sparse file
 */
export async function initSession(req, res) {
    try {
        const body = await parseJsonBody(req);
        const { filename, totalBytes } = body;

        if (!filename || typeof filename !== 'string' || filename.trim() === '') {
            return sendJson(res, 400, { error: 'Invalid or missing filename' });
        }

        const numericTotalBytes = Number(totalBytes);
        if (!numericTotalBytes || Number.isNaN(numericTotalBytes) || numericTotalBytes <= 0) {
            return sendJson(res, 400, { error: 'totalBytes must be a positive integer greater than 0' });
        }

        const sessionId = crypto.randomUUID();
        const filePath = path.join(config.storage.storageDir, `${sessionId}.bin`);

        // Sparse File Allocation via truncate (ADR-0002) - takes < 1ms
        const fh = await fsp.open(filePath, 'w+');
        await fh.truncate(numericTotalBytes);
        await fh.close();

        // Create atomic DB session
        const session = await createSession({
            id: sessionId,
            filename: filename.trim(),
            totalBytes: numericTotalBytes,
        });

        return sendJson(res, 201, {
            id: session.id,
            filename: session.filename,
            totalBytes: Number(session.total_bytes),
            uploadedBytes: Number(session.uploaded_bytes),
            status: session.status,
            createdAt: session.created_at,
        }, {
            'Location': `/api/uploads/${sessionId}`,
        });
    } catch (error) {
        console.error('Error initializing upload session:', error);
        return sendJson(res, 500, { error: 'Failed to initialize upload session', details: error.message });
    }
}

/**
 * PATCH /api/uploads/:id - Streams an incoming byte chunk at precise POSIX offset
 */
export async function uploadChunk(req, res, sessionId) {
    try {
        const session = await getSessionById(sessionId);
        if (!session) {
            return sendJson(res, 404, { error: `Upload session not found: ${sessionId}` });
        }

        if (session.status === 'COMPLETED') {
            return sendJson(res, 409, {
                error: 'Upload session already completed',
                status: session.status,
                uploadedBytes: Number(session.uploaded_bytes),
                finalSha256: session.final_sha256,
            });
        }

        if (session.status === 'ABORTED') {
            return sendJson(res, 410, { error: 'Upload session has been aborted' });
        }

        const contentRangeHeader = req.headers['content-range'];
        const range = parseContentRange(contentRangeHeader, Number(session.total_bytes));
        if (!range) {
            return sendJson(res, 400, {
                error: 'Missing or invalid Content-Range header. Expected: bytes <start>-<end>/<total>',
            });
        }

        const currentCommittedBytes = Number(session.uploaded_bytes);

        // Overlap Idempotency & Alignment Guards (ADR-0004)
        if (range.start !== currentCommittedBytes) {
            // Already committed chunk (idempotent retry)
            if (range.end < currentCommittedBytes) {
                return sendJson(res, 200, {
                    message: 'Chunk already committed (idempotent retry)',
                    status: session.status,
                    uploadedBytes: currentCommittedBytes,
                }, { 'Range': `bytes=0-${currentCommittedBytes}` });
            }

            // Gap detected: client skipped ahead
            if (range.start > currentCommittedBytes) {
                return sendJson(res, 409, {
                    error: 'Chunk offset gap detected: chunk start does not match committed uploaded bytes',
                    expectedOffset: currentCommittedBytes,
                    receivedStart: range.start,
                }, { 'Range': `bytes=0-${currentCommittedBytes}` });
            }
        }

        // Connection Lifecycle & Inactivity Timeout Guard (ADR-0006)
        const abortController = new AbortController();
        const inactivityTimer = setTimeout(() => {
            abortController.abort(new Error('Stream inactivity timeout'));
        }, config.server.streamInactivityTimeoutMs);

        const onSocketClose = () => {
            if (!req.complete) {
                abortController.abort(new Error('Client aborted socket'));
                updateSessionStatus(sessionId, 'PAUSED').catch(() => {});
            }
        };
        req.on('close', onSocketClose);

        const filePath = path.join(config.storage.storageDir, `${sessionId}.bin`);
        let stats;

        try {
            stats = await runChunkPipeline({
                inputStream: req,
                filePath,
                startOffset: range.start,
                expectedLength: range.length,
                expectedChunkHash: req.headers['x-chunk-sha256'] || null,
                signal: abortController.signal,
            });
        } finally {
            clearTimeout(inactivityTimer);
            req.off('close', onSocketClose);
        }

        // Atomically record chunk progress in DB (ADR-0004, ADR-0005)
        const updatedSession = await recordChunkProgress({
            sessionId,
            startOffset: range.start,
            endOffset: stats.currentOffset,
            bytesWritten: stats.bytesWritten,
            chunkHash: stats.chunkHash,
        });

        // Check if upload is fully completed
        const isCompleted = Number(updatedSession.uploaded_bytes) === Number(session.total_bytes);

        if (isCompleted) {
            // Authoritative full-file hash calculation pass (ADR-0003)
            const finalSha256 = await computeFileSha256(filePath);
            const completedSession = await finalizeSession(sessionId, finalSha256);

            return sendJson(res, 200, {
                status: 'COMPLETED',
                uploadedBytes: Number(completedSession.uploaded_bytes),
                totalBytes: Number(completedSession.total_bytes),
                finalSha256: completedSession.final_sha256,
            }, {
                'Range': `bytes=0-${completedSession.uploaded_bytes}`,
            });
        }

        // Partial chunk success: return 206 Partial Content
        return sendJson(res, 206, {
            status: 'UPLOADING',
            uploadedBytes: Number(updatedSession.uploaded_bytes),
            totalBytes: Number(session.total_bytes),
            chunkBytesWritten: stats.bytesWritten,
        }, {
            'Range': `bytes=0-${updatedSession.uploaded_bytes}`,
        });

    } catch (error) {
        console.error(`Error in uploadChunk for session ${sessionId}:`, error);

        if (error.name === 'AbortError' || error.message?.includes('aborted') || error.message?.includes('timeout')) {
            await updateSessionStatus(sessionId, 'PAUSED').catch(() => {});
            return sendJson(res, 408, { error: 'Upload stream was interrupted or timed out. Session is PAUSED.' });
        }

        if (error.message?.includes('mismatch')) {
            return sendJson(res, 400, { error: error.message });
        }

        return sendJson(res, 500, { error: 'Failed to process chunk', details: error.message });
    }
}

/**
 * HEAD / GET /api/uploads/:id - Probes resumption offset and session status (ADR-0001)
 */
export async function probeSession(req, res, sessionId) {
    try {
        const session = await getSessionById(sessionId);
        if (!session) {
            return sendJson(res, 404, { error: `Upload session not found: ${sessionId}` });
        }

        const uploadedBytes = Number(session.uploaded_bytes);
        const totalBytes = Number(session.total_bytes);

        const headers = {
            'Range': `bytes=0-${uploadedBytes}`,
            'X-Upload-Status': session.status,
            'X-Uploaded-Bytes': String(uploadedBytes),
            'X-Total-Bytes': String(totalBytes),
            'Accept-Ranges': 'bytes',
        };

        if (req.method === 'HEAD') {
            res.writeHead(200, headers);
            res.end();
            return;
        }

        return sendJson(res, 200, {
            id: session.id,
            filename: session.filename,
            totalBytes,
            uploadedBytes,
            status: session.status,
            finalSha256: session.final_sha256,
            createdAt: session.created_at,
            updatedAt: session.updated_at,
        }, headers);
    } catch (error) {
        console.error('Error probing session:', error);
        return sendJson(res, 500, { error: 'Failed to probe session', details: error.message });
    }
}

/**
 * GET /api/downloads/:id - Streams completed file back to client with RFC 7233 Range support
 */
export async function downloadFile(req, res, sessionId) {
    try {
        const session = await getSessionById(sessionId);
        if (!session || session.status !== 'COMPLETED') {
            return sendJson(res, 404, { error: 'File not found or upload not yet completed' });
        }

        const filePath = path.join(config.storage.storageDir, `${sessionId}.bin`);
        const totalBytes = Number(session.total_bytes);

        const rangeHeader = req.headers['range'];
        const parsedRange = parseDownloadRange(rangeHeader, totalBytes);

        if (rangeHeader && !parsedRange) {
            res.writeHead(416, {
                'Content-Range': `bytes */${totalBytes}`,
            });
            return res.end();
        }

        if (parsedRange) {
            res.writeHead(206, {
                'Content-Range': `bytes ${parsedRange.start}-${parsedRange.end}/${totalBytes}`,
                'Accept-Ranges': 'bytes',
                'Content-Length': parsedRange.length,
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': `attachment; filename="${encodeURIComponent(session.filename)}"`,
            });

            const stream = fs.createReadStream(filePath, {
                start: parsedRange.start,
                end: parsedRange.end,
            });
            stream.pipe(res);
            return;
        }

        res.writeHead(200, {
            'Content-Length': totalBytes,
            'Accept-Ranges': 'bytes',
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': `attachment; filename="${encodeURIComponent(session.filename)}"`,
        });

        const fullStream = fs.createReadStream(filePath);
        fullStream.pipe(res);
    } catch (error) {
        console.error('Error downloading file:', error);
        return sendJson(res, 500, { error: 'Failed to download file', details: error.message });
    }
}

/**
 * DELETE /api/uploads/:id - Aborts upload session and removes sparse file
 */
export async function abortSession(req, res, sessionId) {
    try {
        const session = await getSessionById(sessionId);
        if (!session) {
            return sendJson(res, 404, { error: `Upload session not found: ${sessionId}` });
        }

        await updateSessionStatus(sessionId, 'ABORTED');

        const filePath = path.join(config.storage.storageDir, `${sessionId}.bin`);
        await fsp.unlink(filePath).catch(() => {});

        res.writeHead(204);
        res.end();
    } catch (error) {
        console.error('Error aborting session:', error);
        return sendJson(res, 500, { error: 'Failed to abort session', details: error.message });
    }
}
