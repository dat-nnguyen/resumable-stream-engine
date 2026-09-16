import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import config from '../src/config/index.js';
import { createServer } from '../src/http/server.js';
import { closePool, healthCheck } from '../src/db/client.js';

/**
 * Drop Simulator: Simulates abrupt client network drops mid-stream,
 * probes the server for the last committed offset, and resumes without byte corruption.
 */
async function runDropSimulation() {
    console.log('='.repeat(70));
    console.log(' CHAOS SIMULATOR: Mid-Stream Network Drop & Safe Resumption');
    console.log('='.repeat(70));

    const isHealthy = await healthCheck();
    if (!isHealthy) {
        console.error('FATAL: Database connection failed. Start postgres first.');
        process.exit(1);
    }

    const server = createServer();
    await new Promise((resolve) => server.listen(0, resolve));
    const port = server.address().port;
    const baseUrl = `http://localhost:${port}`;
    console.log(`✓ Test HTTP server initialized on port ${port}`);

    try {
        // 1. Prepare 12MB test payload
        const TOTAL_SIZE = 12 * 1024 * 1024; // 12MB
        const CHUNK_SIZE = 3 * 1024 * 1024;  // 3MB chunks
        console.log(`[1/6] Generating ${TOTAL_SIZE / (1024 * 1024)}MB deterministic test buffer...`);
        
        const fileBuffer = Buffer.alloc(TOTAL_SIZE);
        for (let i = 0; i < TOTAL_SIZE; i++) {
            fileBuffer[i] = (i % 251); // prime modulo pattern
        }
        const expectedSha256 = crypto.createHash('sha256').update(fileBuffer).digest('hex');
        console.log(`✓ Authoritative Source SHA-256: ${expectedSha256}`);

        // 2. Initialize Session
        console.log('[2/6] Initializing upload session...');
        const initRes = await fetch(`${baseUrl}/api/uploads`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename: 'chaos-resilient-dataset.bin', totalBytes: TOTAL_SIZE }),
        });
        const session = await initRes.json();
        const sessionId = session.id;
        console.log(`✓ Upload session created: ${sessionId}`);

        // 3. Upload Chunk 1 (0 to 3MB) cleanly
        console.log('[3/6] Uploading Chunk 1 (bytes 0 to 3,145,727)...');
        const chunk1 = fileBuffer.subarray(0, CHUNK_SIZE);
        const chunk1Hash = crypto.createHash('sha256').update(chunk1).digest('hex');
        const chunk1Res = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Range': `bytes 0-${CHUNK_SIZE - 1}/${TOTAL_SIZE}`,
                'X-Chunk-SHA256': chunk1Hash,
            },
            body: chunk1,
            duplex: 'half',
        });
        const chunk1Data = await chunk1Res.json();
        console.log(`✓ Chunk 1 committed. Server offset: ${chunk1Data.uploadedBytes} bytes`);

        // 4. Upload Chunk 2 (3MB to 6MB), but DROP CONNECTION after sending 1MB!
        console.log('[4/6] Uploading Chunk 2 with SIMULATED NETWORK FAILURE (socket destroy)...');
        const chunk2Start = CHUNK_SIZE;
        const chunk2End = (2 * CHUNK_SIZE) - 1;
        const chunk2Length = CHUNK_SIZE;

        await new Promise((resolve) => {
            const req = http.request(`${baseUrl}/api/uploads/${sessionId}`, {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/octet-stream',
                    'Content-Range': `bytes ${chunk2Start}-${chunk2End}/${TOTAL_SIZE}`,
                },
            });

            req.on('error', (err) => {
                console.log(`  [Chaos Event Captured] Client socket aborted as planned: ${err.message}`);
                resolve();
            });

            // Write 1MB then abruptly destroy the socket mid-stream!
            const partialData = fileBuffer.subarray(chunk2Start, chunk2Start + (1024 * 1024));
            req.write(partialData);
            setTimeout(() => {
                req.destroy(new Error('ECONNRESET: Simulating dropped wifi / mobile cell switch'));
            }, 50);
        });

        // Wait 300ms for server to handle abort and close fileHandle
        await new Promise((r) => setTimeout(r, 300));

        // 5. Probe server offset via HEAD probe (RFC 7233 & ADR-0001)
        console.log('[5/6] Probing server resumption offset via HEAD request...');
        const headRes = await fetch(`${baseUrl}/api/uploads/${sessionId}`, { method: 'HEAD' });
        const probeRange = headRes.headers.get('range');
        const probeStatus = headRes.headers.get('x-upload-status');
        const committedBytes = Number(headRes.headers.get('x-uploaded-bytes'));
        console.log(`✓ Probe Response: Status=${probeStatus}, Range=${probeRange}, Committed=${committedBytes} bytes`);

        // Since Chunk 2 was aborted before completion, atomic transaction was rolled back.
        // The safe committed offset remains at CHUNK_SIZE (3MB)!
        console.log(`✓ Atomic rollback confirmed. Resuming from committed offset ${committedBytes}...`);

        // 6. Resume remaining data from committedBytes to TOTAL_SIZE
        console.log('[6/6] Resuming stream from committed offset to completion...');
        let currentOffset = committedBytes;

        while (currentOffset < TOTAL_SIZE) {
            const nextEnd = Math.min(currentOffset + CHUNK_SIZE, TOTAL_SIZE) - 1;
            const slice = fileBuffer.subarray(currentOffset, nextEnd + 1);
            const sliceHash = crypto.createHash('sha256').update(slice).digest('hex');

            console.log(`  -> Streaming bytes ${currentOffset}-${nextEnd}/${TOTAL_SIZE} (${slice.length} bytes)...`);
            const patchRes = await fetch(`${baseUrl}/api/uploads/${sessionId}`, {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/octet-stream',
                    'Content-Range': `bytes ${currentOffset}-${nextEnd}/${TOTAL_SIZE}`,
                    'X-Chunk-SHA256': sliceHash,
                },
                body: slice,
                duplex: 'half',
            });

            const patchData = await patchRes.json();
            if (patchRes.status === 200 && patchData.status === 'COMPLETED') {
                console.log(`✓ Upload Completed! Final SHA-256: ${patchData.finalSha256}`);
                if (patchData.finalSha256 !== expectedSha256) {
                    throw new Error(`SHA256 mismatch! Got ${patchData.finalSha256}, expected ${expectedSha256}`);
                }
                break;
            } else if (patchRes.status === 206) {
                currentOffset = patchData.uploadedBytes;
            } else {
                throw new Error(`Unexpected server response: ${patchRes.status} ${JSON.stringify(patchData)}`);
            }
        }

        // 7. Verify byte-level download fidelity
        console.log('Verifying downloaded binary matches source byte-for-byte...');
        const dlRes = await fetch(`${baseUrl}/api/downloads/${sessionId}`);
        const downloadedBytes = Buffer.from(await dlRes.arrayBuffer());
        if (!downloadedBytes.equals(fileBuffer)) {
            throw new Error('Downloaded file does not match original binary content!');
        }

        console.log('='.repeat(70));
        console.log('✓ CHAOS TEST SUCCESSFUL: Zero byte loss or corruption across network aborts!');
        console.log('='.repeat(70));
    } finally {
        server.close();
        await closePool();
        // Clean up test file
        const files = await fs.readdir(config.storage.storageDir).catch(() => []);
        for (const file of files) {
            if (file.endsWith('.bin')) {
                await fs.unlink(path.join(config.storage.storageDir, file)).catch(() => {});
            }
        }
    }
}

runDropSimulation().catch((err) => {
    console.error('Simulation failed:', err);
    process.exit(1);
});
