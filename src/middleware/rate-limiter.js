/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/middleware/rate-limiter
 */


class RateLimiter {
  /**
   * Create a rate limiter instance
   * @param {Object} options - Limiter options
   * @param {number} options.maxJobs - Maximum jobs allowed in window
   * @param {number} options.windowMs - Time window in milliseconds
   */
  constructor(options = {}) {
    this.maxJobs = options.maxJobs || 100;
    this.windowMs = options.windowMs || 60000; // Default: 100 jobs per minute
    this.jobs = []; // Timestamps of processed jobs
  }

  /**
   * Middleware function to check rate limit
   * @param {Object} job - Current job
   * @param {Function} next - Next middleware/function
   * @returns {Promise<void>}
   */
  async execute(job, next) {
    const now = Date.now();
    
    // Remove expired timestamps
    this.jobs = this.jobs.filter(timestamp => now - timestamp < this.windowMs);
    
    if (this.jobs.length >= this.maxJobs) {
      const oldestJob = this.jobs;
      const waitTime = this.windowMs - (now - oldestJob);
      
      throw new Error(`Rate limit exceeded. Please wait ${waitTime}ms before retrying.`);
    }
    
    // Record current job timestamp
    this.jobs.push(now);
    
    await next();
  }
}

/**
 * Factory function to create rate limiter middleware
 * @param {Object} options - Limiter options
 * @returns {Function} Middleware function
 */
export default function createRateLimiter(options) {
  const limiter = new RateLimiter(options);
  
  return async (job, next) => {
    await limiter.execute(job, next);
  };
}
