/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/index
 */


import QueueFactory from './src/core/QueueFactory.js';
import { registerDriver } from './src/drivers/index.js';
import { loadConfig } from './src/utils/config-loader.js';
import { QueueError } from './src/utils/error-handler.js';
import createJobLogger from './src/middleware/job-logger.js';
import createRateLimiter from './src/middleware/rate-limiter.js';
import createRetryManager from './src/middleware/retry-manager.js';
import createMetricsCollector from './src/middleware/metrics-collector.js';
import createCircuitBreaker from './src/middleware/circuit-breaker.js';
import * as serializers from './src/utils/job-serializer.js';
import * as validators from './src/utils/validation.js';

// Export core components
export {
  QueueFactory,
  registerDriver,
  loadConfig,
  QueueError,
  createJobLogger,
  createRateLimiter,
  createRetryManager,
  createMetricsCollector,
  createCircuitBreaker,
  serializers,
  validators
};

// Default export for convenience
export default {
  QueueFactory,
  registerDriver,
  loadConfig,
  QueueError,
  createJobLogger,
  createRateLimiter,
  createRetryManager,
  createMetricsCollector,
  createCircuitBreaker,
  serializers,
  validators
};
