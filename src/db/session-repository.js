"use strict";

import { query, withTransaction } from './client.js';

export async function createSession({ id, filename, totalBytes }) {
    try {
        const result = await query(`
            INSERT INTO upload_sessions(id, filename, total_bytes, uploaded_bytes, status)
            VALUES ($1, $2, $3, 0, 'INITIALIZED') RETURNING *;
        `, [id, filename, totalBytes]);
        return result.rows[0];
    } catch (error) {
        console.error(`Error creating upload session ${id}:`, error);
        throw error;
    }
}

export async function getSessionById(id) {
    try {
        const result = await query(`
            SELECT * FROM upload_sessions WHERE id = $1
        `, [id]);
        return result.rows[0];
    } catch (error) {
        console.error(`Error getting upload session ${id}:`, error);
        throw error;
    }
}

export async function recordChunkProgress({ sessionId, startOffset, endOffset, bytesWritten, chunkHash }) {
    return await withTransaction(async (client) => {
        // 1. Log the chunk
        await client.query(`
            INSERT INTO chunk_logs (session_id, start_offset, end_offset, bytes_written, chunk_hash)
            VALUES ($1, $2, $3, $4, $5);
        `, [sessionId, startOffset, endOffset, bytesWritten, chunkHash]);

        // 2. Atomically update session progress and state
        const updateRes = await client.query(`
            UPDATE upload_sessions
            SET 
                uploaded_bytes = GREATEST(uploaded_bytes, $2),
                status = CASE 
                    WHEN GREATEST(uploaded_bytes, $2) = total_bytes THEN 'COMPLETED'
                    ELSE 'UPLOADING'
                END
            WHERE id = $1
            RETURNING *;
        `, [sessionId, endOffset]);

        return updateRes.rows[0];
    });
}


export async function updateSessionStatus(id, status) {
    try {
        const result = await query(`
            UPDATE upload_sessions SET status = $2 WHERE id = $1 RETURNING *;
        `, [id, status]);
        return result.rows[0];
    } catch (error) {
        console.error(`Error updating upload session ${id}:`, error);
        throw error;
    }
}


export async function finalizeSession(id, finalSha256) {
    try {
        const result = await query(`
            UPDATE upload_sessions SET final_sha256 = $2, status = 'COMPLETED' WHERE id = $1 RETURNING *;
        `, [id, finalSha256]);
        return result.rows[0];
    } catch (error) {
        console.error(`Error finalizing upload session ${id}:`, error);
        throw error;
    }
}