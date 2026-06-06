/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/middleware/priority-schedular
 */

/**
 * Create a priority scheduler middleware
 * @param {Object} options - Scheduler options
 * @param {number} options.defaultPriority - Default priority if not specified (lower number = higher priority)
 * @returns {Function} Middleware function
 */
export default function createPriorityScheduler(options = {}) {
  const defaultPriority = options.defaultPriority || 5;

  return async (job, next) => {
    // Ensure job has a priority field
    if (job.priority === undefined || job.priority === null) {
      job.priority = defaultPriority;
    }

    // Validate priority range (e.g., 1-10)
    if (typeof job.priority !== 'number' || job.priority < 1 || job.priority > 10) {
      throw new Error(`Invalid priority: ${job.priority}. Must be between 1 and 10.`);
    }

    // Proceed to next middleware/processor
    // Note: The actual re-ordering happens in the Driver's pop() method 
    // or the QueueManager's internal queue structure.
    await next();
  };
}
