# Sparse File Allocation via Truncate

We need to allocate destination storage for large files (10GB+) upon upload session creation. We decided to allocate files as sparse files using `fileHandle.truncate(totalBytes)` rather than eager pre-allocation (`fallocate`). This achieves sub-millisecond initialization across macOS and Linux while ensuring physical storage blocks are only consumed as chunks are committed to disk.
