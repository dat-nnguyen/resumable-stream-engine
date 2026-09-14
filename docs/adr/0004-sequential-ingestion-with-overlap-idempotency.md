# Sequential Ingestion with Overlap Idempotency

We need an ingestion ordering strategy that balances simplicity, streaming predictability, and retry safety. We decided to enforce sequential chunk arrival matching the committed byte offset while handling retransmitted byte overlaps idempotently. This prevents interval fragmentation and race conditions during streaming while gracefully tolerating network retries.
