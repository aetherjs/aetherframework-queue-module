/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/utils/error-handler
 */

/**
 * Standard error class for queue operations
 */
export class QueueError extends Error {
  /**
   * Create a queue error
   * @param {string} message - Error message
   * @param {string} code - Error code
   * @param {Object} details - Additional error details
   */
  constructor(message, code, details = {}) {
    super(message);
    this.name = 'QueueError';
    this.code = code;
    this.details = details;
    this.timestamp = new Date().toISOString();
  }
}

/**
 * Handle errors in async operations
 * @param {Function} fn - Async function to execute
 * @param {Object} context - Error context
 * @returns {Promise<*|null>} Result or null if error occurred
 */
export async function safeExecute(fn, context = {}) {
  try {
    return await fn();
  } catch (error) {
    console.error(`Error in ${context.operation || 'unknown operation'}:`, error);
    
    // Convert to standard QueueError if not already
    if (!(error instanceof QueueError)) {
      throw new QueueError(
        error.message || 'Unknown error occurred',
        context.errorCode || 'INTERNAL_ERROR',
        { originalError: error.message, stack: error.stack }
      );
    }
    throw error;
  }
}
