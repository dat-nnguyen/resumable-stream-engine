# Resumable Stream Engine

A Node.js HTTP service for uploading and downloading large files in chunks without running out of memory.

---

## Problems This Project Solves

1. **Out-of-Memory (OOM) crashes**: Loading large files (e.g. 5GB+) into memory buffers before writing to disk exhausts Node.js RAM. This project uses Node.js streams with backpressure to keep memory usage bounded (under 25MB V8 heap) regardless of file size.
2. **Failed uploads starting over from scratch**: If a network connection drops at 95%, standard uploads force the client to restart from 0%. This project allows clients to query the server's current byte offset and resume uploading from where the connection was lost.
3. **Data corruption during retries**: If a chunk is resent or arrives out of order, appending blindly corrupts the file. This project writes each chunk to disk at an exact byte offset using file handles (`pwrite`) and validates chunk order in PostgreSQL.

---

## What It Is Used For

- Uploading large assets (video files, disk images, backups, database dumps) over HTTP.
- Applications with users on unstable or mobile connections where connection drops are common.
- Services that need to accept large files on low-memory servers (e.g., small containers or VMs).

---

## Quick Demo (curl)

### 1. Initialize an upload session

```bash
curl -X POST http://localhost:3000/api/uploads \
  -H "Content-Type: application/json" \
  -d '{"filename": "archive.zip", "totalBytes": 1000}'
```

**Response (`201 Created`):**
```json
{
  "id": "c1a2b3c4-d5e6-7890-abcd-ef1234567890",
  "filename": "archive.zip",
  "totalBytes": 1000,
  "uploadedBytes": 0,
  "status": "INITIALIZED"
}
```

### 2. Upload the first chunk (bytes 0 to 499)

```bash
curl -X PATCH http://localhost:3000/api/uploads/c1a2b3c4-d5e6-7890-abcd-ef1234567890 \
  -H "Content-Type: application/octet-stream" \
  -H "Content-Range: bytes 0-499/1000" \
  --data-binary @chunk1.bin
```

**Response (`206 Partial Content`):**
```json
{
  "status": "UPLOADING",
  "uploadedBytes": 500,
  "totalBytes": 1000,
  "chunkBytesWritten": 500
}
```

### 3. Connection dropped? Probe where to resume

Send a `HEAD` request to find out how many bytes the server has saved:

```bash
curl -I http://localhost:3000/api/uploads/c1a2b3c4-d5e6-7890-abcd-ef1234567890
```

**Response Headers:**
```http
HTTP/1.1 200 OK
Range: bytes=0-500
X-Upload-Status: UPLOADING
X-Uploaded-Bytes: 500
X-Total-Bytes: 1000
```

### 4. Resume upload with the next chunk (bytes 500 to 999)

```bash
curl -X PATCH http://localhost:3000/api/uploads/c1a2b3c4-d5e6-7890-abcd-ef1234567890 \
  -H "Content-Type: application/octet-stream" \
  -H "Content-Range: bytes 500-999/1000" \
  --data-binary @chunk2.bin
```

**Response (`200 OK` - Upload complete):**
```json
{
  "status": "COMPLETED",
  "uploadedBytes": 1000,
  "totalBytes": 1000,
  "finalSha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}
```

### 5. Download the file (full or range)

```bash
# Download the whole file
curl -O http://localhost:3000/api/downloads/c1a2b3c4-d5e6-7890-abcd-ef1234567890

# Or download a specific byte slice (RFC 7233)
curl -H "Range: bytes=0-499" http://localhost:3000/api/downloads/c1a2b3c4-d5e6-7890-abcd-ef1234567890 -o slice.bin
```

---

## How It Works

1. **Sparse file pre-allocation**: When a session is created, the engine opens the file and calls `truncate(totalBytes)`. The filesystem allocates metadata immediately without zero-filling the disk, taking less than 1ms.
2. **POSIX offset writing (`pwrite`)**: Each chunk is written directly to its target byte offset (`fileHandle.write(chunk, 0, len, offset)`). It doesn't rely on a shared sequential write cursor.
3. **Stream backpressure**: The incoming HTTP request is piped through a SHA-256 transform stream directly to the disk writer stream. If the disk write buffer fills up, Node pauses reading from the network socket until the buffer drains.
4. **Crash durability**: When a chunk finishes writing, `fileHandle.sync()` flushes data to disk before the PostgreSQL transaction commits the new `uploaded_bytes` offset. If the server crashes, uncommitted bytes are ignored on reconnect.

---

## Getting Started

### Prerequisites

- Node.js 18+ (uses native ESM and `node:test`)
- Docker (for PostgreSQL)

### 1. Setup

```bash
# Clone and enter directory
git clone https://github.com/dat-nnguyen/resumable-stream-engine.git
cd resumable-stream-engine

# Copy environment variables
cp .env.example .env

# Start PostgreSQL
docker compose up -d

# Install dependencies
npm install
```

### 2. Start the server

```bash
npm start
```

Server starts on `http://localhost:3000`.

---

## Tests

The test suite uses Node's built-in test runner (`node:test`) with no external test frameworks.

```bash
npm test
```

### What the tests cover:

- `test/range-parser.test.js`: RFC 7233 `Content-Range` and `Range` header parsing (valid ranges, open-ended ranges, suffix ranges, and malformed inputs).
- `test/hash-transform.test.js`: Streaming SHA-256 calculation, verifying zero memory accumulation and ensuring digests are not appended to the file.
- `test/offset-writer.test.js`: Writing chunks at specific file offsets, verifying sparse byte padding, and verifying `sync()` execution.
- `test/resumable-flow.test.js`: Full end-to-end flow with PostgreSQL (upload initialization, chunk uploads, gap detection, retry idempotency, HEAD probe, range downloads, and session abortion).

---

## Benchmarks & Chaos Simulation

### 1. Memory and Throughput Benchmark

Generates and streams 64MB of synthetic data in 8MB chunks using backpressure while sampling V8 Heap and RSS every 25ms:

```bash
npm run benchmark
```

**Results:**
- **Upload Throughput**: ~45–60 MB/s
- **Download Throughput**: ~300 MB/s
- **Peak V8 Heap**: ~12–13 MB (demonstrates heap usage does not grow with file size)

### 2. Network Drop Simulator

Simulates real-world network interruption by forcefully destroying the client socket mid-stream:

```bash
npm run simulate:drop
```

**What it does:**
1. Starts a 12MB upload and completes Chunk 1 (3MB).
2. Starts Chunk 2 and abruptly aborts the socket after 1MB.
3. Verifies the server pauses the session and saves the committed 3MB offset.
4. Probes the server with `HEAD /api/uploads/:id` to retrieve the committed offset.
5. Resumes uploading remaining chunks from offset 3MB to completion.
6. Downloads the full file and verifies the SHA-256 hash matches the original byte-for-byte.

---

## License

MIT
