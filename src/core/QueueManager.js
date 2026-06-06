/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/core/QueueManager
 */

import EventEmitter from 'events';

export default class QueueManager extends EventEmitter {
  /**
   * Create a new QueueManager instance
   * @param {Object} factory - QueueFactory instance for creating queues
   * @param {Object} options - Manager configuration options
   */
  constructor(factory, options = {}) {
    super();
    
    this.factory = factory;
    this.options = {
      autoStart: options.autoStart !== false,
      healthCheckInterval: options.healthCheckInterval || 30000,
      maxQueues: options.maxQueues || 100,
      ...options
    };
    
    this.queues = new Map(); // queueName -> queue instance
    this.workers = new Map(); // workerName -> worker instance
    this.schedulers = new Map(); // schedulerName -> scheduler instance
    this.healthCheckInterval = null;
    
    if (this.options.autoStart) {
      this.startHealthChecks();
    }
  }
  
  /**
   * Create and register a new queue
   * @param {string} name - Unique name for the queue
   * @param {Object} config - Queue configuration
   * @returns {Object} Created queue instance
   */
  createQueue(name, config = {}) {
    if (this.queues.has(name)) {
      throw new Error(`Queue "${name}" already exists`);
    }
    
    if (this.queues.size >= this.options.maxQueues) {
      throw new Error(`Maximum number of queues (${this.options.maxQueues}) reached`);
    }
    
    const queue = this.factory.createQueue({
      name,
      ...config
    });
    
    this.queues.set(name, queue);
    this.emit('queueCreated', { name, queue, config });
    
    return queue;
  }
  
  /**
   * Create and register a new worker
   * @param {string} name - Unique name for the worker
   * @param {Object} config - Worker configuration
   * @returns {Object} Created worker instance
   */
  createWorker(name, config = {}) {
    if (this.workers.has(name)) {
      throw new Error(`Worker "${name}" already exists`);
    }
    
    const worker = this.factory.createWorker({
      name,
      ...config
    });
    
    this.workers.set(name, worker);
    this.emit('workerCreated', { name, worker, config });
    
    return worker;
  }
  
  /**
   * Create and register a new scheduler
   * @param {string} name - Unique name for the scheduler
   * @param {Object} config - Scheduler configuration
   * @returns {Object} Created scheduler instance
   */
  createScheduler(name, config = {}) {
    if (this.schedulers.has(name)) {
      throw new Error(`Scheduler "${name}" already exists`);
    }
    
    const scheduler = this.factory.createScheduler({
      name,
      ...config
    });
    
    this.schedulers.set(name, scheduler);
    this.emit('schedulerCreated', { name, scheduler, config });
    
    return scheduler;
  }
  
  /**
   * Get queue by name
   * @param {string} name - Queue name
   * @returns {Object} Queue instance
   */
  getQueue(name) {
    if (!this.queues.has(name)) {
      throw new Error(`Queue "${name}" not found`);
    }
    return this.queues.get(name);
  }
  
  /**
   * Get worker by name
   * @param {string} name - Worker name
   * @returns {Object} Worker instance
   */
  getWorker(name) {
    if (!this.workers.has(name)) {
      throw new Error(`Worker "${name}" not found`);
    }
    return this.workers.get(name);
  }
  
  /**
   * Get scheduler by name
   * @param {string} name - Scheduler name
   * @returns {Object} Scheduler instance
   */
  getScheduler(name) {
    if (!this.schedulers.has(name)) {
      throw new Error(`Scheduler "${name}" not found`);
    }
    return this.schedulers.get(name);
  }
  
  /**
   * Remove queue by name
   * @param {string} name - Queue name
   * @returns {boolean} True if queue was removed
   */
  removeQueue(name) {
    if (!this.queues.has(name)) {
      return false;
    }
    
    const queue = this.queues.get(name);
    
    // Stop the queue if it's running
    if (typeof queue.stop === 'function') {
      queue.stop();
    }
    
    this.queues.delete(name);
    this.emit('queueRemoved', { name });
    
    return true;
  }
  
  /**
   * Remove worker by name
   * @param {string} name - Worker name
   * @returns {boolean} True if worker was removed
   */
  removeWorker(name) {
    if (!this.workers.has(name)) {
      return false;
    }
    
    const worker = this.workers.get(name);
    
    // Stop the worker if it's running
    if (typeof worker.stop === 'function') {
      worker.stop();
    }
    
    this.workers.delete(name);
    this.emit('workerRemoved', { name });
    
    return true;
  }
  
  /**
   * Remove scheduler by name
   * @param {string} name - Scheduler name
   * @returns {boolean} True if scheduler was removed
   */
  removeScheduler(name) {
    if (!this.schedulers.has(name)) {
      return false;
    }
    
    const scheduler = this.schedulers.get(name);
    
    // Stop the scheduler if it's running
    if (typeof scheduler.stop === 'function') {
      scheduler.stop();
    }
    
    this.schedulers.delete(name);
    this.emit('schedulerRemoved', { name });
    
    return true;
  }
  
  /**
   * Get all queue names
   * @returns {Array} Array of queue names
   */
  getQueueNames() {
    return Array.from(this.queues.keys());
  }
  
  /**
   * Get all worker names
   * @returns {Array} Array of worker names
   */
  getWorkerNames() {
    return Array.from(this.workers.keys());
  }
  
  /**
   * Get all scheduler names
   * @returns {Array} Array of scheduler names
   */
  getSchedulerNames() {
    return Array.from(this.schedulers.keys());
  }
  
  /**
   * Get statistics for all managed components
   * @returns {Object} Statistics object
   */
  getStats() {
    const stats = {
      queues: {},
      workers: {},
      schedulers: {},
      totals: {
        queues: this.queues.size,
        workers: this.workers.size,
        schedulers: this.schedulers.size
      }
    };
    
    // Collect queue statistics
    for (const [name, queue] of this.queues.entries()) {
      if (typeof queue.getStats === 'function') {
        stats.queues[name] = queue.getStats();
      } else {
        stats.queues[name] = { status: 'active' };
      }
    }
    
    // Collect worker statistics
    for (const [name, worker] of this.workers.entries()) {
      if (typeof worker.getStats === 'function') {
        stats.workers[name] = worker.getStats();
      } else {
        stats.workers[name] = { status: worker.isRunning ? 'running' : 'stopped' };
      }
    }
    
    // Collect scheduler statistics
    for (const [name, scheduler] of this.schedulers.entries()) {
      if (typeof scheduler.getStats === 'function') {
        stats.schedulers[name] = scheduler.getStats();
      } else {
        stats.schedulers[name] = { status: scheduler.isRunning ? 'running' : 'stopped' };
      }
    }
    
    return stats;
  }
  
  /**
   * Start health checks for all components
   */
  startHealthChecks() {
    if (this.healthCheckInterval) {
      return;
    }
    
    this.healthCheckInterval = setInterval(() => {
      this._performHealthChecks();
    }, this.options.healthCheckInterval);
    
    this.emit('healthChecksStarted');
  }
  
  /**
   * Stop health checks
   */
  stopHealthChecks() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
      this.emit('healthChecksStopped');
    }
  }
  
  /**
   * Perform health checks on all components
   * @private
   */
  _performHealthChecks() {
    const healthStatus = {
      timestamp: Date.now(),
      queues: {},
      workers: {},
      schedulers: {},
      healthy: true
    };
    
    // Check queues
    for (const [name, queue] of this.queues.entries()) {
      try {
        if (typeof queue.getStats === 'function') {
          const stats = queue.getStats();
          healthStatus.queues[name] = {
            healthy: true,
            stats
          };
        } else {
          healthStatus.queues[name] = {
            healthy: true,
            status: 'active'
          };
        }
      } catch (error) {
        healthStatus.queues[name] = {
          healthy: false,
          error: error.message
        };
        healthStatus.healthy = false;
      }
    }
    
    // Check workers
    for (const [name, worker] of this.workers.entries()) {
      try {
        if (typeof worker.getStatus === 'function') {
          const status = worker.getStatus();
          healthStatus.workers[name] = {
            healthy: status.isRunning !== false,
            status
          };
          if (!status.isRunning) healthStatus.healthy = false;
        } else {
          healthStatus.workers[name] = {
            healthy: true,
            status: 'active'
          };
        }
      } catch (error) {
        healthStatus.workers[name] = {
          healthy: false,
          error: error.message
        };
        healthStatus.healthy = false;
      }
    }
    
    // Check schedulers
    for (const [name, scheduler] of this.schedulers.entries()) {
      try {
        if (typeof scheduler.getStats === 'function') {
          const stats = scheduler.getStats();
          healthStatus.schedulers[name] = {
            healthy: stats.isRunning !== false,
            stats
          };
          if (!stats.isRunning) healthStatus.healthy = false;
        } else {
          healthStatus.schedulers[name] = {
            healthy: true,
            status: 'active'
          };
        }
      } catch (error) {
        healthStatus.schedulers[name] = {
          healthy: false,
          error: error.message
        };
        healthStatus.healthy = false;
      }
    }
    
    this.emit('healthCheck', healthStatus);
  }
  
  /**
   * Start all components
   */
  startAll() {
    // Start all queues
    for (const [name, queue] of this.queues.entries()) {
      if (typeof queue.start === 'function') {
        queue.start();
      }
    }
    
    // Start all workers
    for (const [name, worker] of this.workers.entries()) {
      if (typeof worker.start === 'function') {
        worker.start();
      }
    }
    
    // Start all schedulers
    for (const [name, scheduler] of this.schedulers.entries()) {
      if (typeof scheduler.start === 'function') {
        scheduler.start();
      }
    }
    
    this.emit('allStarted');
  }
  
  /**
   * Stop all components
   */
  stopAll() {
    // Stop all schedulers
    for (const [name, scheduler] of this.schedulers.entries()) {
      if (typeof scheduler.stop === 'function') {
        scheduler.stop();
      }
    }
    
    // Stop all workers
    for (const [name, worker] of this.workers.entries()) {
      if (typeof worker.stop === 'function') {
        worker.stop();
      }
    }
    
    // Stop all queues
    for (const [name, queue] of this.queues.entries()) {
      if (typeof queue.stop === 'function') {
        queue.stop();
      }
    }
    
    // Stop health checks
    this.stopHealthChecks();
    
    this.emit('allStopped');
  }
  
  /**
   * Pause all components
   */
  pauseAll() {
    for (const [name, queue] of this.queues.entries()) {
      if (typeof queue.pause === 'function') {
        queue.pause();
      }
    }
    
    for (const [name, worker] of this.workers.entries()) {
      if (typeof worker.pause === 'function') {
        worker.pause();
      }
    }
    
    for (const [name, scheduler] of this.schedulers.entries()) {
      if (typeof scheduler.pause === 'function') {
        scheduler.pause();
      }
    }
    
    this.emit('allPaused');
  }
  
  /**
   * Resume all components
   */
  resumeAll() {
    for (const [name, queue] of this.queues.entries()) {
      if (typeof queue.resume === 'function') {
        queue.resume();
      }
    }
    
    for (const [name, worker] of this.workers.entries()) {
      if (typeof worker.resume === 'function') {
        worker.resume();
      }
    }
    
    for (const [name, scheduler] of this.schedulers.entries()) {
      if (typeof scheduler.resume === 'function') {
        scheduler.resume();
      }
    }
    
    this.emit('allResumed');
  }
  
  /**
   * Shutdown the manager and all components
   * @returns {Promise<void>}
   */
  async shutdown() {
    this.emit('shutdownStarted');
    
    // Stop all components
    this.stopAll();
    
    // Clear all collections
    this.queues.clear();
    this.workers.clear();
    this.schedulers.clear();
    
    this.emit('shutdownCompleted');
  }
}
