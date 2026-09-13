# Resumable File Streaming Engine with Backpressure

A high-performance, memory-bounded streaming engine built in Node.js designed to handle large file uploads and downloads (10GB+) over HTTP without Out-Of-Memory (OOM) crashes.

---

## Key Architecture & Primitives

```text
[Client]
   │
   │  HTTP POST / PATCH (Range / Content-Range)
   ▼
[HTTP Request Stream (node:http)]
   │
   ├───► [Crypto Hash Transform Stream] ──► On-the-fly SHA-256 Digest (<50MB RAM)
   │
   ▼ (Manual Backpressure: stream.write() === false / 'drain')
[Offset Writer Writable Stream] ──► Direct POSIX File Descriptor (pwrite)
   │
   ▼ (fdatasync & Atomic Commit)
[PostgreSQL Database] (Tracks byte offsets, chunk logs, and session lifecycle)
```

---

## Project Structure

```text
resumable-stream-engine/
├── .env.example                    # Environment template (PORT, DB URI, STORAGE_PATH)
├── .gitignore                      # Git ignore rules
├── package.json                    # ESM package ("type": "module"), scripts
├── docker-compose.yml              # Local PostgreSQL 16 service
├── README.md                       # Architecture & documentation
│
├── src/
│   ├── config/index.js             # Configuration & environment variables
│   ├── core/                       # Stream Engine Core (POSIX I/O & pipelines)
│   │   ├── offset-writer-stream.js # Custom Writable writing to POSIX FD via byte offset + drain
│   │   ├── hash-transform-stream.js# Zero-memory incremental SHA-256 calculation
│   │   └── pipeline-runner.js      # Stream coordinator integrating AbortSignal & cleanup
│   ├── db/                         # Persistence Layer
│   │   ├── client.js               # PostgreSQL connection pool (pg.Pool)
│   │   ├── schema.sql              # DDL schema for upload_sessions and chunk_logs
│   │   └── session-repository.js   # Atomic state updates and offset queries
│   ├── http/                       # Network Protocol Layer (Pure node:http)
│   │   ├── middlewares/
│   │   │   ├── range-parser.js     # RFC 7233 Content-Range / Range parser
│   │   │   └── memory-profiler.js  # Live RSS / V8 Heap telemetry tracker
│   │   ├── controllers/
│   │   │   └── upload-controller.js# Request dispatcher binding Core Streams, DB, and HTTP
│   │   └── server.js               # Native HTTP server bootstrap & router
│   ├── storage/.gitkeep            # Upload directory placeholder
│   └── app.js                      # Application bootstrap & graceful shutdown
│
├── benchmark/                      # Performance & Chaos Engineering Suite
│   ├── virtual-stream.js           # Synthetic N-GB stream generator (zero disk pre-fill)
│   ├── drop-simulator.js           # Network drop simulator testing mid-stream abort & resume
│   └── run-benchmark.js            # Stress test harness measuring throughput & <50MB peak RAM
│
└── test/                           # Test Suite (node:test)
    ├── offset-writer.test.js       # Offset writing & backpressure pause/drain tests
    ├── hash-transform.test.js      # Incremental checksum accuracy tests
    ├── range-parser.test.js        # RFC 7233 parsing edge cases & validation
    └── resumable-flow.test.js      # Full upload, interrupt, resume, verify lifecycle
```

---

## Quickstart

### Prerequisites

- Node.js 18+ (tested on Node.js 26)
- Docker & Docker Compose (for PostgreSQL)

### 1. Setup Environment & Database

```bash
cp .env.example .env
docker-compose up -d
```

### 2. Start Service

```bash
npm start
```

### 3. Run Tests & Benchmarks

```bash
npm test
npm run benchmark
npm run simulate:drop
```
