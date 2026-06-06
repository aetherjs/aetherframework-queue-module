/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/middleware/circuit-breaker
 */

class CircuitBreaker {
  /**
   * Create circuit breaker instance
   * @param {Object} options - Breaker options
   * @param {number} options.failureThreshold - Failure count to trip breaker
   * @param {number} options.resetTimeout - Time in ms before resetting breaker
   */
  constructor(options = {}) {
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeout = options.resetTimeout || 30000;
    this.failureCount = 0;
    this.lastFailureTime = null;
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF-OPEN
  }

  /**
   * Check if circuit is open
   * @returns {boolean}
   */
  isOpen() {
    if (this.state === 'OPEN') {
      const now = Date.now();
      if (now - this.lastFailureTime > this.resetTimeout) {
        this.state = 'HALF-OPEN';
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Record success
   */
  recordSuccess() {
    this.failureCount = 0;
    this.state = 'CLOSED';
  }

  /**
   * Record failure
   */
  recordFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    
    if (this.failureCount >= this.failureThreshold) {
      this.state = 'OPEN';
    }
  }
}

const globalBreaker = new CircuitBreaker();

/**
 * Create circuit breaker middleware
 * @param {Object} options - Breaker options
 * @returns {Function} Middleware function
 */
export default function createCircuitBreaker(options) {
  // Update global breaker config if provided
  if (options) {
    globalBreaker.failureThreshold = options.failureThreshold || globalBreaker.failureThreshold;
    globalBreaker.resetTimeout = options.resetTimeout || globalBreaker.resetTimeout;
  }

  return async (job, next) => {
    if (globalBreaker.isOpen()) {
      throw new Error('Circuit breaker is OPEN. Service temporarily unavailable.');
    }

    try {
      await next();
      globalBreaker.recordSuccess();
    } catch (error) {
      globalBreaker.recordFailure();
      throw error;
    }
  };
}
