# Chunk Checksums and Final Stream Verification

Standard OpenSSL SHA-256 state cannot be serialized and restored across paused HTTP requests in Node.js. We decided to require an `X-Chunk-SHA256` header for each incoming chunk to reject corrupt network packets on-the-fly, combined with a single streaming verification pass when all bytes are committed to compute the authoritative file SHA-256. This prevents full-file retransmissions on single-chunk errors while guaranteeing end-to-end cryptographic integrity.
