import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import config from '../src/config/index.js';
import { createServer } from '../src/http/server.js';
import { closePool, healthCheck } from '../src/db/client.js';

describe('Resumable Streaming Flow End-to-End Integration', () => {
    let server;
    let baseUrl;

    before(async () => {
        const isHealthy = await healthCheck();
        assert.ok(isHealthy, 'PostgreSQL database must be reachable for integration test');

        server = createServer();
        await new Promise((resolve) => server.listen(0, resolve));
        const port = server.address().port;
        baseUrl = `http://localhost:${port}`;
    });

    after(async () => {
        if (server) {
            server.close();
        }
        await closePool();
        // Clean up test storage files
        const files = await fs.readdir(config.storage.storageDir).catch(() => []);
        for (const file of files) {
            if (file.endsWith('.bin')) {
                await fs.unlink(path.join(config.storage.storageDir, file)).catch(() => {});
            }
        }
    });

    it('should verify /health endpoint returns 200 healthy', async () => {
        const res = await fetch(`${baseUrl}/health`);
        assert.strictEqual(res.status, 200);
        const data = await res.json();
        assert.strictEqual(data.status, 'healthy');
        assert.ok(data.timestamp);
    });

    it('should execute full 3-chunk resumable upload lifecycle with gap guard, idempotency, and checksum validation', async () => {
        const TOTAL_SIZE = 3000;
        const fileBuffer = crypto.randomBytes(TOTAL_SIZE);
        const fullExpectedSha256 = crypto.createHash('sha256').update(fileBuffer).digest('hex');

        // 1. Initialize Upload Session
        const initRes = await fetch(`${baseUrl}/api/uploads`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename: 'production-firmware.bin', totalBytes: TOTAL_SIZE }),
        });
        assert.strictEqual(initRes.status, 201);
        const session = await initRes.json();
        assert.ok(session.id);
        assert.strictEqual(session.filename, 'production-firmware.bin');
        assert.strictEqual(session.totalBytes, TOTAL_SIZE);
        assert.strictEqual(session.uploadedBytes, 0);
        assert.strictEqual(session.status, 'INITIALIZED');
        const sessionId = session.id;

        // Verify sparse file allocation on disk
        const filePath = path.join(config.storage.storageDir, `${sessionId}.bin`);
        const fileStat = await fs.stat(filePath);
        assert.strictEqual(fileStat.size, TOTAL_SIZE);

        // 2. Upload Chunk 1: bytes 0-999 / 3000
        const chunk1 = fileBuffer.subarray(0, 1000);
        const chunk1Hash = crypto.createHash('sha256').update(chunk1).digest('hex');
        const chunk1Res = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Range': 'bytes 0-999/3000',
                'X-Chunk-SHA256': chunk1Hash,
            },
            body: chunk1,
            duplex: 'half',
        });
        assert.strictEqual(chunk1Res.status, 206);
        const chunk1Data = await chunk1Res.json();
        assert.strictEqual(chunk1Data.status, 'UPLOADING');
        assert.strictEqual(chunk1Data.uploadedBytes, 1000);

        // 3. Probe offset via HEAD request (ADR-0001)
        const headRes = await fetch(`${baseUrl}/api/uploads/${sessionId}`, { method: 'HEAD' });
        assert.strictEqual(headRes.status, 200);
        assert.strictEqual(headRes.headers.get('range'), 'bytes=0-1000');
        assert.strictEqual(headRes.headers.get('x-upload-status'), 'UPLOADING');
        assert.strictEqual(headRes.headers.get('x-uploaded-bytes'), '1000');
        assert.strictEqual(headRes.headers.get('x-total-bytes'), '3000');

        // 4. Test Gap Detection: attempt to upload Chunk 3 (2000-2999) before Chunk 2 (ADR-0004)
        const chunk3Premature = fileBuffer.subarray(2000, 3000);
        const gapRes = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Range': 'bytes 2000-2999/3000',
            },
            body: chunk3Premature,
            duplex: 'half',
        });
        assert.strictEqual(gapRes.status, 409, 'Must return 409 Conflict upon offset gap');
        const gapData = await gapRes.json();
        assert.strictEqual(gapData.expectedOffset, 1000);

        // 5. Test Idempotent Retry: re-upload Chunk 1 (0-999)
        const retryRes = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Range': 'bytes 0-999/3000',
            },
            body: chunk1,
            duplex: 'half',
        });
        assert.strictEqual(retryRes.status, 200, 'Must return 200 OK for already-committed chunk');
        const retryData = await retryRes.json();
        assert.strictEqual(retryData.uploadedBytes, 1000);

        // 6. Test Corrupt Chunk Checksum: upload Chunk 2 with corrupted SHA-256 header
        const chunk2 = fileBuffer.subarray(1000, 2000);
        const badHashRes = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Range': 'bytes 1000-1999/3000',
                'X-Chunk-SHA256': '0000000000000000000000000000000000000000000000000000000000000000',
            },
            body: chunk2,
            duplex: 'half',
        });
        assert.strictEqual(badHashRes.status, 400, 'Must reject corrupted chunk hash with 400 Bad Request');

        // 7. Upload Chunk 2 legitimately: bytes 1000-1999 / 3000
        const chunk2Hash = crypto.createHash('sha256').update(chunk2).digest('hex');
        const chunk2Res = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Range': 'bytes 1000-1999/3000',
                'X-Chunk-SHA256': chunk2Hash,
            },
            body: chunk2,
            duplex: 'half',
        });
        assert.strictEqual(chunk2Res.status, 206);
        const chunk2Data = await chunk2Res.json();
        assert.strictEqual(chunk2Data.uploadedBytes, 2000);

        // 8. Upload Final Chunk 3: bytes 2000-2999 / 3000
        const chunk3Hash = crypto.createHash('sha256').update(chunk3Premature).digest('hex');
        const chunk3Res = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Range': 'bytes 2000-2999/3000',
                'X-Chunk-SHA256': chunk3Hash,
            },
            body: chunk3Premature,
            duplex: 'half',
        });
        assert.strictEqual(chunk3Res.status, 200);
        const chunk3Data = await chunk3Res.json();
        assert.strictEqual(chunk3Data.status, 'COMPLETED');
        assert.strictEqual(chunk3Data.uploadedBytes, 3000);
        assert.strictEqual(chunk3Data.finalSha256, fullExpectedSha256);

        // 9. Download Full File & Validate Byte Accuracy
        const downloadRes = await fetch(`${baseUrl}/api/downloads/${sessionId}`);
        assert.strictEqual(downloadRes.status, 200);
        assert.strictEqual(downloadRes.headers.get('content-length'), '3000');
        const downloadedBytes = Buffer.from(await downloadRes.arrayBuffer());
        assert.deepStrictEqual(downloadedBytes, fileBuffer);

        // 10. Download RFC 7233 Range: bytes 500-1499
        const rangeRes = await fetch(`${baseUrl}/api/downloads/${sessionId}`, {
            headers: { 'Range': 'bytes=500-1499' }
        });
        assert.strictEqual(rangeRes.status, 206);
        assert.strictEqual(rangeRes.headers.get('content-range'), 'bytes 500-1499/3000');
        assert.strictEqual(rangeRes.headers.get('content-length'), '1000');
        const rangeBytes = Buffer.from(await rangeRes.arrayBuffer());
        assert.deepStrictEqual(rangeBytes, fileBuffer.subarray(500, 1500));

        // 11. Download RFC 7233 Suffix Range: bytes=-500 (last 500 bytes)
        const suffixRes = await fetch(`${baseUrl}/api/downloads/${sessionId}`, {
            headers: { 'Range': 'bytes=-500' }
        });
        assert.strictEqual(suffixRes.status, 206);
        assert.strictEqual(suffixRes.headers.get('content-range'), 'bytes 2500-2999/3000');
        assert.strictEqual(suffixRes.headers.get('content-length'), '500');
        const suffixBytes = Buffer.from(await suffixRes.arrayBuffer());
        assert.deepStrictEqual(suffixBytes, fileBuffer.subarray(2500, 3000));
    });

    it('should delete sparse file and abort session on DELETE /api/uploads/:id', async () => {
        const initRes = await fetch(`${baseUrl}/api/uploads`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename: 'aborted-upload.bin', totalBytes: 5000 }),
        });
        const { id: sessionId } = await initRes.json();
        const filePath = path.join(config.storage.storageDir, `${sessionId}.bin`);

        // Verify file exists
        await fs.access(filePath);

        // Abort session
        const deleteRes = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
            method: 'DELETE',
        });
        assert.strictEqual(deleteRes.status, 204);

        // Verify file removed
        await assert.rejects(fs.access(filePath));

        // Verify probe returns ABORTED status
        const probeRes = await fetch(`${baseUrl}/api/uploads/${sessionId}`);
        const probeData = await probeRes.json();
        assert.strictEqual(probeData.status, 'ABORTED');
    });
});
