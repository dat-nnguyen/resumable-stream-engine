// RFC 7233 Content-Range & Range header parsing middleware
import config from "../../config/index.js";

export function parseContentRange(value, fileSize) {
    if (!value || typeof value !== 'string') return null;

    // Content-Range: bytes 0-499/12345
    const match = value.match(/^bytes\s+(\d+)-(\d+)\/(\d+|\*)$/i);
    if (!match) return null;

    const start = Number(match[1]);
    const end = Number(match[2]);
    const totalStr = match[3];
    const total = totalStr === '*' ? null : Number(totalStr);

    if (Number.isNaN(start) || Number.isNaN(end)) return null;
    if (total !== null && Number.isNaN(total)) return null;

    const length = end - start + 1;

    if (start < 0 || length <= 0) return null;
    if (fileSize != null) {
        if (total === null || total !== fileSize) return null;
        if (end >= fileSize) return null;
    } else if (total !== null && end >= total) {
        return null;
    }

    return {
        start,
        end,
        length,
        total
    };
}

export function parseDownloadRange(headerValue, totalFileSize) {
    if (!headerValue || typeof headerValue !== 'string' || !totalFileSize || totalFileSize <= 0) return null;

    // Suffix range: bytes=-500 (last 500 bytes)
    const suffixMatch = headerValue.match(/^bytes=-(\d+)$/i);
    if (suffixMatch) {
        const suffixLength = Number(suffixMatch[1]);
        if (Number.isNaN(suffixLength) || suffixLength <= 0) return null;
        const length = Math.min(suffixLength, totalFileSize);
        const start = Math.max(0, totalFileSize - length);
        const end = totalFileSize - 1;
        return {
            start,
            end,
            length: end - start + 1,
            total: totalFileSize,
        };
    }

    // Standard or open-ended range: bytes=0-499 or bytes=500-
    const standardMatch = headerValue.match(/^bytes=(\d+)-(\d*)$/i);
    if (!standardMatch) return null;

    const start = Number(standardMatch[1]);
    const endStr = standardMatch[2];

    if (Number.isNaN(start) || start < 0 || start >= totalFileSize) return null;

    let end = (endStr && endStr.length > 0) ? Number(endStr) : (totalFileSize - 1);
    if (Number.isNaN(end) || end < start) return null;

    // RFC 7233: If end is >= totalFileSize, clamp to totalFileSize - 1
    if (end >= totalFileSize) {
        end = totalFileSize - 1;
    }

    const length = end - start + 1;

    return {
        start,
        end,
        length,
        total: totalFileSize,
    };
}

export function createContentRangeHeader(range, totalFileSize) {
    if (!range || totalFileSize === null) return null;

    const { start, end, length } = range;
    return `bytes ${start}-${end}/${length === totalFileSize ? '*' : totalFileSize}`;
}