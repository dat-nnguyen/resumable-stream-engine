"use strict";

import { Writable } from 'stream';

export class OffsetWriterStream extends Writable {
    #fileHandle;
    #currentOffset;
    #startOffset;

    constructor({ fileHandle, startOffset = 0, highWaterMark = 64 * 1024 }) {
        super({ highWaterMark });
        this.#fileHandle = fileHandle;
        this.#currentOffset = startOffset;
        this.#startOffset = startOffset;
    }

    async _write(chunk, encoding, callback) {
        try {
            await this.#fileHandle.write(chunk, 0, chunk.length, this.#currentOffset);
            this.#currentOffset += chunk.length;
            callback();
        } catch (error) {
            callback(error);
        }
    }

    async _final(callback) {
        try {
            await this.#fileHandle.sync();
            callback();
        } catch (error) {
            callback(error);
        }
    }

    _destroy(error, callback) {
        this.#fileHandle.close().then(() => {
            callback(error);
        }).catch((closeError) => {
            callback(closeError || error);
        });
    }

    get bytesWritten() {
        return this.#currentOffset - this.#startOffset;
    }

    get currentOffset() {
        return this.#currentOffset;
    }

}
