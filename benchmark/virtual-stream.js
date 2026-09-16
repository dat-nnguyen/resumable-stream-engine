import { Readable } from 'node:stream';
import crypto from 'node:crypto';

/**
 * VirtualDataStream generates an arbitrary number of synthetic bytes
 * without allocating large memory or reading from disk.
 * 
 * Uses deterministic repeating patterns and computes real-time SHA-256.
 */
export class VirtualDataStream extends Readable {
    #totalBytes;
    #bytesPushed = 0;
    #chunkSize;
    #patternBuffer;
    #hasher;
    #digest = null;

    /**
     * @param {Object} options
     * @param {number} options.totalBytes - Total bytes to generate
     * @param {number} [options.chunkSize=65536] - Size of each generated buffer
     * @param {number} [options.seedByte=0x41] - Starting ASCII character pattern
     */
    constructor({ totalBytes, chunkSize = 65536, seedByte = 0x41 }) {
        super({ highWaterMark: chunkSize });
        this.#totalBytes = totalBytes;
        this.#chunkSize = chunkSize;
        this.#hasher = crypto.createHash('sha256');

        // Pre-allocate single reusable template buffer
        this.#patternBuffer = Buffer.alloc(chunkSize);
        for (let i = 0; i < chunkSize; i++) {
            this.#patternBuffer[i] = (seedByte + (i % 26));
        }
    }

    _read(size) {
        if (this.#bytesPushed >= this.#totalBytes) {
            if (!this.#digest) {
                this.#digest = this.#hasher.digest('hex');
            }
            this.push(null);
            return;
        }

        const remaining = this.#totalBytes - this.#bytesPushed;
        const currentChunkSize = Math.min(this.#chunkSize, remaining);

        let chunk;
        if (currentChunkSize === this.#chunkSize) {
            chunk = Buffer.from(this.#patternBuffer);
        } else {
            chunk = Buffer.from(this.#patternBuffer.subarray(0, currentChunkSize));
        }

        this.#hasher.update(chunk);
        this.#bytesPushed += chunk.length;
        this.push(chunk);
    }

    get bytesGenerated() {
        return this.#bytesPushed;
    }

    get sha256() {
        return this.#digest || this.#hasher.copy().digest('hex');
    }
}

export default VirtualDataStream;
