# Resumable Stream Engine

A high-performance streaming backend for memory-bounded, fault-tolerant file transfers over HTTP.

## Language

**Upload Session**:
A stateful record tracking the lifecycle, expected size, and byte progress of an in-flight or completed file transfer.
_Avoid_: Upload job, file transfer instance

**Chunk**:
A contiguous slice of a file transmitted within a single HTTP request, bounded by a start byte and an end byte offset.
_Avoid_: Part, slice, block, fragment

**Offset Writer**:
A storage stream that writes incoming byte buffers directly to precise disk positions using POSIX file descriptors.
_Avoid_: Disk appender, file saver

**Backpressure**:
A flow-control mechanism signaling the incoming network stream to pause reading when the downstream storage writer buffer is full.
_Avoid_: Rate limiting, throttling

**Content-Range**:
An HTTP header defining the exact byte span and total expected file size of an uploaded chunk adhering to RFC 7233.
_Avoid_: Byte window, range specifier

**Sparse File**:
A filesystem allocation strategy where unwritten byte regions consume zero physical disk blocks until written.
_Avoid_: Empty file, holey file, zeroed file

**Resumption Probe**:
An HTTP HEAD request querying the currently committed byte offset of an active upload session.
_Avoid_: Status check, offset poll

**Chunk Digest**:
An incremental cryptographic checksum computed across the byte payload of a single chunk.
_Avoid_: Part hash, slice checksum

**Chunk Overlap**:
A retransmitted byte slice that falls within an already committed byte interval, processed idempotently.
_Avoid_: Duplicate chunk, repeated bytes

**Fdatasync**:
A POSIX system call that flushes modified file buffer pages to physical media before committing offset state.
_Avoid_: Flush, disk commit
