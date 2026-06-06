/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/middleware/job-logger
 */

/**
 * Create a job logger middleware
 * @param {Object} options - Logger options
 * @param {Function} options.logger - Custom logger function (default: console.log)
 * @param {boolean} options.logData - Whether to log job data (default: false)
 * @returns {Function} Middleware function
 */
export default function createJobLogger(options = {}) {
  const {
    logger = console.log,
    logData = false
  } = options;
  
  return async (job, next) => {
    const startTime = Date.now();
    const jobId = job.id || 'unknown';
    const queueName = job.queueName || 'unknown';
    
    logger(`[Job Start] ID: ${jobId}, Queue: ${queueName}, Time: ${new Date().toISOString()}`);
    
    if (logData) {
      logger(`[Job Data] ID: ${jobId}`, JSON.stringify(job.data));
    }
    
    try {
      await next();
      
      const duration = Date.now() - startTime;
      logger(`[Job Complete] ID: ${jobId}, Duration: ${duration}ms, Status: success`);
    } catch (error) {
      const duration = Date.now() - startTime;
      logger(`[Job Failed] ID: ${jobId}, Duration: ${duration}ms, Error: ${error.message}`);
      throw error;
    }
  };
}
