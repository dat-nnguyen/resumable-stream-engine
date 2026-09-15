"use strict";

import pg from 'pg';
import config from '../config/index.js';

const { Pool } = pg;

const pool = new Pool({
    connectionString: config.database.url,
    ...config.database.pool,
});

pool.on('error', (err) => {
    console.error('Unexpected error in PostgreSQL pool:', err.message);
});
/**
 * Executes a SQL query and returns the result.
 * @param {string} text - The SQL query text
 * @param {Array} params - The query parameters
 * @returns {Promise<Object>} The query result
 */
export async function query(text, params) {
    try {
        const res = await pool.query(text, params);
        return res;
    } catch (error) {
        console.error('Database query error:', error.message, 'SQL:', text, 'Params:', params);
        throw error;
    }
}

/**
 * Executes a callback within a managed transaction.
 * Automatically handles BEGIN, COMMIT, ROLLBACK, and client release.
 * @template T
 * @param {(client: import('pg').PoolClient) => Promise<T>} callback
 * @returns {Promise<T>}
 */
export async function withTransaction(callback) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const result = await callback(client);
        await client.query('COMMIT');
        return result;
    } catch (error) {
        await client.query('ROLLBACK').catch((rbErr) => {
            console.error('Error rolling back transaction:', rbErr.message);
        });
        throw error;
    } finally {
        client.release();
    }
}

export async function healthCheck() {
    try {
        await pool.query('SELECT NOW()');
        return true;
    } catch (error) {
        console.error('Database health check failed:', error.message);
        return false;
    }
}

/**
 * Closes the database connection pool
 */
export async function closePool() {
    await pool.end();   
}

export default pool;
