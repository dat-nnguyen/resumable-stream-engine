import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseContentRange, parseDownloadRange } from '../src/http/middlewares/range-parser.js';

describe('Range Parser Middleware', () => {
    describe('parseContentRange (RFC 7233 PATCH / Uploads)', () => {
        it('should correctly parse valid content ranges', () => {
            const result = parseContentRange('bytes 0-499/1000', 1000);
            assert.deepStrictEqual(result, {
                start: 0,
                end: 499,
                total: 1000,
                length: 500,
            });
        });

        it('should correctly parse the final chunk of an upload', () => {
            const result = parseContentRange('bytes 500-999/1000', 1000);
            assert.deepStrictEqual(result, {
                start: 500,
                end: 999,
                total: 1000,
                length: 500,
            });
        });

        it('should correctly parse single-byte chunk', () => {
            const result = parseContentRange('bytes 0-0/1000', 1000);
            assert.deepStrictEqual(result, {
                start: 0,
                end: 0,
                total: 1000,
                length: 1,
            });
        });

        it('should return null if header is missing or empty', () => {
            assert.strictEqual(parseContentRange(null, 1000), null);
            assert.strictEqual(parseContentRange('', 1000), null);
            assert.strictEqual(parseContentRange(undefined, 1000), null);
        });

        it('should return null if unit is not bytes', () => {
            assert.strictEqual(parseContentRange('items 0-499/1000', 1000), null);
            assert.strictEqual(parseContentRange('characters 0-499/1000', 1000), null);
        });

        it('should return null if total does not match expected file total', () => {
            assert.strictEqual(parseContentRange('bytes 0-499/5000', 1000), null);
        });

        it('should return null if range is inverted (start > end)', () => {
            assert.strictEqual(parseContentRange('bytes 500-200/1000', 1000), null);
        });

        it('should return null if end offset is out of bounds (end >= total)', () => {
            assert.strictEqual(parseContentRange('bytes 500-1000/1000', 1000), null);
            assert.strictEqual(parseContentRange('bytes 0-1500/1000', 1000), null);
        });

        it('should return null on invalid syntax or non-numeric values', () => {
            assert.strictEqual(parseContentRange('bytes abc-def/1000', 1000), null);
            assert.strictEqual(parseContentRange('bytes 0-499/*', 1000), null);
            assert.strictEqual(parseContentRange('invalid format', 1000), null);
        });
    });

    describe('parseDownloadRange (RFC 7233 GET / Downloads)', () => {
        const TOTAL_SIZE = 1000;

        it('should parse standard explicit range (bytes=0-499)', () => {
            const result = parseDownloadRange('bytes=0-499', TOTAL_SIZE);
            assert.deepStrictEqual(result, {
                start: 0,
                end: 499,
                length: 500,
                total: TOTAL_SIZE,
            });
        });

        it('should parse open-ended range from start offset to EOF (bytes=500-)', () => {
            const result = parseDownloadRange('bytes=500-', TOTAL_SIZE);
            assert.deepStrictEqual(result, {
                start: 500,
                end: 999,
                length: 500,
                total: TOTAL_SIZE,
            });
        });

        it('should parse open-ended range starting from zero (bytes=0-)', () => {
            const result = parseDownloadRange('bytes=0-', TOTAL_SIZE);
            assert.deepStrictEqual(result, {
                start: 0,
                end: 999,
                length: 1000,
                total: TOTAL_SIZE,
            });
        });

        it('should parse suffix range for the last N bytes (bytes=-200)', () => {
            const result = parseDownloadRange('bytes=-200', TOTAL_SIZE);
            assert.deepStrictEqual(result, {
                start: 800,
                end: 999,
                length: 200,
                total: TOTAL_SIZE,
            });
        });

        it('should clamp suffix range if requested bytes exceed total size (bytes=-5000)', () => {
            const result = parseDownloadRange('bytes=-5000', TOTAL_SIZE);
            assert.deepStrictEqual(result, {
                start: 0,
                end: 999,
                length: 1000,
                total: TOTAL_SIZE,
            });
        });

        it('should return null for missing or null header', () => {
            assert.strictEqual(parseDownloadRange(null, TOTAL_SIZE), null);
            assert.strictEqual(parseDownloadRange(undefined, TOTAL_SIZE), null);
            assert.strictEqual(parseDownloadRange('', TOTAL_SIZE), null);
        });

        it('should return null if unit is not bytes', () => {
            assert.strictEqual(parseDownloadRange('items=0-499', TOTAL_SIZE), null);
        });

        it('should return null if range is inverted (start > end)', () => {
            assert.strictEqual(parseDownloadRange('bytes=600-400', TOTAL_SIZE), null);
        });

        it('should return null if start is at or beyond total size', () => {
            assert.strictEqual(parseDownloadRange('bytes=1000-1500', TOTAL_SIZE), null);
            assert.strictEqual(parseDownloadRange('bytes=1000-', TOTAL_SIZE), null);
        });

        it('should clamp end offset if it exceeds total size', () => {
            const result = parseDownloadRange('bytes=500-2000', TOTAL_SIZE);
            assert.deepStrictEqual(result, {
                start: 500,
                end: 999,
                length: 500,
                total: TOTAL_SIZE,
            });
        });
    });
});
