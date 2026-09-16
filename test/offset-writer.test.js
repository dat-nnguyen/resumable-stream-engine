import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { OffsetWriterStream } from '../src/core/offset-writer-stream.js';

describe('OffsetWriterStream (POSIX pwrite & fdatasync)', () => {
    let tempDir;

    beforeEach(async () => {
        tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'offset-writer-test-'));
    });

    afterEach(async () => {
        await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    });

    it('should write chunks sequentially starting from offset 0', async () => {
        const filePath = path.join(tempDir, 'test-seq.bin');
        const fileHandle = await fs.open(filePath, 'w+');

        const writer = new OffsetWriterStream({ fileHandle, startOffset: 0 });
        const chunk1 = Buffer.from('CHUNK_ONE_');
        const chunk2 = Buffer.from('CHUNK_TWO_');
        const chunk3 = Buffer.from('CHUNK_THREE');

        await pipeline(Readable.from([chunk1, chunk2, chunk3]), writer);

        assert.strictEqual(writer.bytesWritten, chunk1.length + chunk2.length + chunk3.length);
        assert.strictEqual(writer.currentOffset, writer.bytesWritten);

        const savedData = await fs.readFile(filePath);
        assert.deepStrictEqual(savedData, Buffer.concat([chunk1, chunk2, chunk3]));
    });

    it('should write at a specific non-zero POSIX offset in a sparse file (ADR-0002)', async () => {
        const filePath = path.join(tempDir, 'sparse-offset.bin');
        const TOTAL_SIZE = 10000;
        const START_OFFSET = 4000;

        // Allocate sparse file of 10,000 bytes
        const initHandle = await fs.open(filePath, 'w+');
        await initHandle.truncate(TOTAL_SIZE);
        await initHandle.close();

        // Open handle for writing at offset 4000
        const fileHandle = await fs.open(filePath, 'r+');
        const writer = new OffsetWriterStream({ fileHandle, startOffset: START_OFFSET });

        const payload = crypto.randomBytes(1500);
        await pipeline(Readable.from([payload]), writer);

        assert.strictEqual(writer.bytesWritten, 1500);
        assert.strictEqual(writer.currentOffset, 5500);

        // Verify disk contents at precise byte coordinates
        const fileBytes = await fs.readFile(filePath);
        assert.strictEqual(fileBytes.length, TOTAL_SIZE);

        // Prefix [0 - 3999] must be all zeros (sparse unallocated bytes)
        const prefix = fileBytes.subarray(0, START_OFFSET);
        assert.ok(prefix.every(byte => byte === 0), 'Prefix must be sparse null bytes');

        // Middle [4000 - 5499] must match payload exactly
        const writtenSlice = fileBytes.subarray(START_OFFSET, START_OFFSET + 1500);
        assert.deepStrictEqual(writtenSlice, payload);

        // Suffix [5500 - 9999] must remain all zeros
        const suffix = fileBytes.subarray(START_OFFSET + 1500);
        assert.ok(suffix.every(byte => byte === 0), 'Suffix must remain sparse null bytes');
    });

    it('should call fileHandle.sync() in _final to guarantee crash durability (ADR-0005)', async () => {
        const filePath = path.join(tempDir, 'sync-test.bin');
        const fileHandle = await fs.open(filePath, 'w+');

        let syncCalled = false;
        const originalSync = fileHandle.sync.bind(fileHandle);
        fileHandle.sync = async () => {
            syncCalled = true;
            return originalSync();
        };

        const writer = new OffsetWriterStream({ fileHandle, startOffset: 0 });
        await pipeline(Readable.from([Buffer.from('CRITICAL_DURABLE_BYTES')]), writer);

        assert.ok(syncCalled, 'fileHandle.sync() must be executed upon stream completion');
    });

    it('should propagate write errors through pipeline when handle is closed or invalid', async () => {
        const filePath = path.join(tempDir, 'error-test.bin');
        const fileHandle = await fs.open(filePath, 'w+');
        await fileHandle.close(); // prematurely close

        const writer = new OffsetWriterStream({ fileHandle, startOffset: 0 });
        await assert.rejects(
            pipeline(Readable.from([Buffer.from('should fail')]), writer)
        );
    });
});
