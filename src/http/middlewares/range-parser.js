// RFC 7233 Content-Range & Range header parsing middleware
import config from "../../config/index.js";

export function parseContentRange(value, fileSize) {
    if (!value) return null;

    // Content-Range: bytes 0-499 / 12345
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
    if (total !== null && start + length > total) return null;
    if (total !== null && fileSize !== null && end >= fileSize) return null;

    return {
        start,
        end,
        length,
        total
    };
}

export function parseDownloadRange(headerValue, totalFileSize) {
    if (!headerValue || typeof headerValue !== 'string') return null;

    const match = headerValue.match(/^bytes=(\d+)-(\d*)$/i);
    if (!match) return null;

    const start = Number(match[1]);
    const endStr = match[2];

    if (Number.isNaN(start) || start < 0) return null;

    // If end is omitted (e.g. "bytes=1000-"), end defaults to totalFileSize - 1
    const end = (endStr && endStr.length > 0) ? Number(endStr) : (totalFileSize - 1);

    if (Number.isNaN(end) || end < start) return null;
    if (totalFileSize !== null && (start >= totalFileSize || end >= totalFileSize)) return null;

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