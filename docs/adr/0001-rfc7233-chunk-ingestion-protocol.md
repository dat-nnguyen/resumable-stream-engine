# RFC 7233 Content-Range Ingestion Protocol

We need an HTTP protocol for streaming, chunked, and resumable file uploads. We decided to use `POST /api/uploads` to initialize sessions, `PATCH /api/uploads/:id` with RFC 7233 `Content-Range: bytes <start>-<end>/<total>` to transmit byte chunks, and `HEAD /api/uploads/:id` to probe committed offsets. This provides standardized byte-range semantics without requiring custom client SDKs or proprietary upload protocols like Tus.
