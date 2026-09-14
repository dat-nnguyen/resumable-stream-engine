# Chunk Boundary Fdatasync Durability

We need to guarantee that acknowledged chunks cannot be lost if the server loses power or the process crashes. We decided to invoke `fileHandle.sync()` (`fdatasync`) at the boundary of each completed chunk before persisting the updated byte offset to PostgreSQL and returning HTTP 200/206. This ensures physical durability while amortizing sync latency across chunk payloads rather than micro-writes.
