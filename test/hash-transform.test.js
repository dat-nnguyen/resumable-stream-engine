import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { HashTransformStream } from '../src/core/hash-transform-stream.js';

describe('HashTransformStream (Zero-Memory Incremental Hasher)', () => {
    it('should compute the correct SHA-256 digest for an empty stream', async () => {
        const hasher = new HashTransformStream();
        const chunks = [];
        const sink = new Writable({
            write(chunk, encoding, callback) {
                chunks.push(chunk);
                callback();
            }
        });

        await pipeline(Readable.from([]), hasher, sink);

        const expectedHash = crypto.createHash('sha256').digest('hex');
        assert.strictEqual(hasher.bytesProcessed, 0);
        assert.strictEqual(hasher.digest, expectedHash);
        assert.strictEqual(chunks.length, 0);
    });

    it('should incrementally hash a multi-chunk stream accurately', async () => {
        const hasher = new HashTransformStream();
        const originalData = [
            Buffer.from('Hello, '),
            Buffer.from('resumable '),
            Buffer.from('stream engine!'),
        ];
        const combined = Buffer.concat(originalData);
        const expectedHash = crypto.createHash('sha256').update(combined).digest('hex');

        const collected = [];
        const sink = new Writable({
            write(chunk, encoding, callback) {
                collected.push(chunk);
                callback();
            }
        });

        await pipeline(Readable.from(originalData), hasher, sink);

        assert.strictEqual(hasher.bytesProcessed, combined.length);
        assert.strictEqual(hasher.digest, expectedHash);
        assert.deepStrictEqual(Buffer.concat(collected), combined);
    });

    it('should guarantee passthrough integrity without appending digest downstream (ADR-0003)', async () => {
        const hasher = new HashTransformStream();
        const payload = crypto.randomBytes(65536); // 64KB random buffer
        const expectedHash = crypto.createHash('sha256').update(payload).digest('hex');

        const collected = [];
        const sink = new Writable({
            write(chunk, encoding, callback) {
                collected.push(chunk);
                callback();
            }
        });

        await pipeline(Readable.from([payload]), hasher, sink);

        const downstreamData = Buffer.concat(collected);

        // Crucial invariant: length must be EXACTLY 65536, NOT 65536 + 64 hex bytes!
        assert.strictEqual(downstreamData.length, payload.length);
        assert.deepStrictEqual(downstreamData, payload);
        assert.strictEqual(hasher.digest, expectedHash);
        assert.strictEqual(hasher.bytesProcessed, payload.length);
    });

    it('should return null digest before stream finishes', () => {
        const hasher = new HashTransformStream();
        assert.strictEqual(hasher.digest, null);
        assert.strictEqual(hasher.bytesProcessed, 0);
    });

    it('should maintain backpressure when piped to a slow consumer', async () => {
        const hasher = new HashTransformStream();
        const chunks = Array.from({ length: 50 }, (_, i) => Buffer.alloc(1024, i));
        const fullBuffer = Buffer.concat(chunks);
        const expectedHash = crypto.createHash('sha256').update(fullBuffer).digest('hex');

        const collected = [];
        const slowSink = new Writable({
            write(chunk, encoding, callback) {
                collected.push(chunk);
                // Simulate backpressured slow disk write
                setTimeout(callback, 2);
            }
        });

        await pipeline(Readable.from(chunks), hasher, slowSink);

        assert.strictEqual(hasher.bytesProcessed, fullBuffer.length);
        assert.strictEqual(hasher.digest, expectedHash);
        assert.deepStrictEqual(Buffer.concat(collected), fullBuffer);
    });
});
