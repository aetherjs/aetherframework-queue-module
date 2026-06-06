/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/middleware/retry-manager
 */


/**
 * Create a retry manager middleware
 * @param {Object} options - Retry options
 * @param {number} options.maxRetries - Maximum retry attempts
 * @param {number} options.baseDelay - Base delay in ms for backoff
 * @returns {Function} Middleware function
 */
export default function createRetryManager(options = {}) {
  const {
    maxRetries = 3,
    baseDelay = 1000
  } = options;

  return async (job, next) => {
    let lastError;
    const attempts = job.attempts || 0;

    for (let i = 0; i <= maxRetries; i++) {
      try {
        // Update attempt count
        job.attempts = i + 1;
        await next();
        return; // Success
      } catch (error) {
        lastError = error;
        
        // If max retries reached, throw error
        if (i === maxRetries) {
          break;
        }
        
        // Calculate exponential backoff delay
        const delay = baseDelay * Math.pow(2, i);
        
        // Wait before retry
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }

    // All retries failed
    throw new Error(`Job ${job.id} failed after ${maxRetries + 1} attempts: ${lastError.message}`);
  };
}
