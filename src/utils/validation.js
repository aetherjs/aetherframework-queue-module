/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/utils/validation
 */


/**
 * Validate queue configuration
 * @param {Object} config - Configuration object
 * @throws {Error} If configuration is invalid
 */
export function validateQueueConfig(config) {
  if (!config) {
    throw new Error('Queue configuration is required');
  }
  
  if (!config.name || typeof config.name !== 'string') {
    throw new Error('Queue name must be a non-empty string');
  }
  
  if (config.concurrency !== undefined) {
    if (typeof config.concurrency !== 'number' || config.concurrency < 1) {
      throw new Error('Concurrency must be a positive number');
    }
  }
  
  if (config.driver && typeof config.driver !== 'string') {
    throw new Error('Driver must be a string identifier');
  }
}

/**
 * Validate job data
 * @param {Object} jobData - Job data object
 * @throws {Error} If job data is invalid
 */
export function validateJobData(jobData) {
  if (jobData === undefined || jobData === null) {
    throw new Error('Job data cannot be null or undefined');
  }
  
  // Allow primitives and objects, but not functions
  if (typeof jobData === 'function') {
    throw new Error('Job data cannot be a function');
  }
}
