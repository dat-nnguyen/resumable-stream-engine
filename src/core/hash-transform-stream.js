"use strict";

import { Transform } from "node:stream";
import { createHash } from "node:crypto";

/**
 * @typedef {Object} HashResult
 * @property {string} digest - The SHA-256 digest in hexadecimal format
 * @property {number} bytesProcessed - The number of bytes processed
 */

export class HashTransformStream extends Transform {
    /**
     * @private
     * @type {crypto.Hash}
     */
    #sha256;

    /**
     * @private
     * @type {string | null}
     */
    #digest = null;

    /**
     * @private
     * @type {number}
     */
    #bytesProcessed = 0;

    constructor() {
        super();
        this.#sha256 = createHash("sha256");
    }

    /**
     * The core transformation method that processes incoming data chunks.
     * Updates the internal SHA-256 hash state and tracks bytes processed.
     * This method is called by the stream pipeline for each chunk.
     *
     * @private
     * @param {Buffer | string} chunk - The data chunk to process
     * @param {string} _encoding - The encoding of the chunk (unused)
     * @param {function} callback - The callback function to call when processing is complete
     */
    _transform(chunk, _encoding, callback) {
        this.#sha256.update(chunk);
        this.#bytesProcessed += chunk.length;
        callback(null, chunk);
    }

    _flush(callback) {
        this.#digest = this.#sha256.digest("hex");
        callback();
    }

    get digest() {
        return this.#digest;
    }

    get bytesProcessed() {
        return this.#bytesProcessed;
    }
}

export default HashTransformStream;