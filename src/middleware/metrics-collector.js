/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/middleware/metrics-collector
 */


class MetricsStore {
  constructor() {
    this.metrics = {
      totalJobs: 0,
      successfulJobs: 0,
      failedJobs: 0,
      totalProcessingTime: 0,
      averageProcessingTime: 0
    };
  }

  recordSuccess(duration) {
    this.metrics.totalJobs++;
    this.metrics.successfulJobs++;
    this.metrics.totalProcessingTime += duration;
    this.metrics.averageProcessingTime = 
      this.metrics.totalProcessingTime / this.metrics.totalJobs;
  }

  recordFailure(duration) {
    this.metrics.totalJobs++;
    this.metrics.failedJobs++;
    this.metrics.totalProcessingTime += duration;
    this.metrics.averageProcessingTime = 
      this.metrics.totalProcessingTime / this.metrics.totalJobs;
  }

  getMetrics() {
    return { ...this.metrics };
  }
}

const globalMetrics = new MetricsStore();

/**
 * Create metrics collector middleware
 * @param {Object} options - Collector options
 * @returns {Function} Middleware function
 */
export default function createMetricsCollector(options = {}) {
  return async (job, next) => {
    const startTime = Date.now();
    
    try {
      await next();
      const duration = Date.now() - startTime;
      globalMetrics.recordSuccess(duration);
    } catch (error) {
      const duration = Date.now() - startTime;
      globalMetrics.recordFailure(duration);
      throw error;
    }
  };
}

// Export metrics store for external access
createMetricsCollector.getMetrics = () => globalMetrics.getMetrics();
