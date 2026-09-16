// Stream pipeline coordinator integrating AbortSignal and safe resource cleanup
import { pipeline } from "node:stream/promises";
import fs from "node:fs/promises";
import { HashTransformStream } from "./hash-transform-stream.js";
import { OffsetWriterStream } from "./offset-writer-stream.js";
import config from "../config/index.js";

export async function runChunkPipeline({
    inputStream,
    filePath,
    startOffset,
    expectedLength,
    expectedChunkHash = null,
    signal = null
}) {
    let fileHandle = null;

    try {
        fileHandle = await fs.open(filePath, 'r+');
        const hashStream = new HashTransformStream({highWaterMark: config.server.chunkHighWaterMark});
        const writerStream = new OffsetWriterStream({
            fileHandle,
            startOffset,
            highWaterMark: config.server.chunkHighWaterMark,
        });

        const pipelineOptions = signal ? { signal } : {};
        await pipeline(inputStream, hashStream, writerStream, pipelineOptions);

        if (expectedLength !== null && writerStream.bytesWritten !== expectedLength) {
            throw new Error (
                `Chunk length mismatch: ${writerStream.bytesWritten} vs expected ${expectedLength}`
            )
        }

        if (
            expectedChunkHash &&
            hashStream.digest?.toLowerCase() !== expectedChunkHash.toLowerCase()
        ) {
            throw new Error(
                `Chunk hash mismatch: expected ${expectedChunkHash}, received ${hashStream.digest}`
            );
        }

        return {
            bytesWritten: writerStream.bytesWritten,
            currentOffset: writerStream.currentOffset,
            chunkHash: hashStream.digest,
        };

    } finally {
        if (fileHandle) {
            await fileHandle.close().catch(() => {});
        }
    }
}