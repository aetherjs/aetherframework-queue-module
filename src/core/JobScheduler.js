/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/core/JobSceheduler
 */

import crypto from 'crypto';
import EventEmitter from 'events';

export default class JobScheduler extends EventEmitter {
  /**
   * Create a new JobScheduler instance
   * @param {Object} options - Scheduler configuration
   * @param {number} options.concurrency - Maximum concurrent jobs
   * @param {number} options.pollInterval - Polling interval in ms
   * @param {number} options.maxRetries - Maximum retry attempts
   * @param {number} options.retryDelay - Base retry delay in ms
   * @param {Object} options.driver - Queue driver instance
   * @param {string} options.name - Scheduler name
   */
  constructor(options = {}) {
    super();
    
    this.options = {
      concurrency: options.concurrency || 5,
      pollInterval: options.pollInterval || 100,
      maxRetries: options.maxRetries || 3,
      retryDelay: options.retryDelay || 1000,
      name: options.name || `scheduler_${crypto.randomBytes(4).toString('hex')}`,
      ...options
    };
    
    this.driver = options.driver;
    this.workers = new Map();
    this.isRunning = false;
    this.activeJobs = new Map();
    this.processInterval = null;
    this.middleware = [];
    this.stats = {
      totalJobs: 0,
      completedJobs: 0,
      failedJobs: 0,
      retriedJobs: 0,
      avgProcessingTime: 0
    };
    
    if (!this.driver) {
      throw new Error('Driver instance is required for JobScheduler');
    }
    
    this._validateDriverInterface();
  }
  
  /**
   * Register a job handler
   * @param {string} name - Handler name
   * @param {Function} handler - Handler function
   * @param {Object} options - Handler options
   */
  registerHandler(name, handler, options = {}) {
    if (typeof handler !== 'function') {
      throw new Error(`Handler for "${name}" must be a function`);
    }
    
    const wrappedHandler = async (job) => {
      const startTime = Date.now();
      const jobId = job.id || crypto.randomUUID();
      
      this.emit('jobStarted', { jobId, job, handler: name });
      
      try {
        // Apply middleware before execution
        const processedJob = await this._applyMiddleware('beforeExecute', job);
        
        // Execute handler
        const result = await handler(processedJob.payload || processedJob.data);
        
        const duration = Date.now() - startTime;
        this._updateStats('completed', duration);
        
        this.emit('jobCompleted', {
          jobId,
          job: processedJob,
          result,
          duration,
          handler: name
        });
        
        // Apply middleware after successful execution
        await this._applyMiddleware('afterExecute', {
          job: processedJob,
          result,
          duration
        });
        
        return result;
      } catch (error) {
        const duration = Date.now() - startTime;
        this._updateStats('failed', duration);
        
        this.emit('jobFailed', {
          jobId,
          job,
          error,
          handler: name,
          duration
        });
        
        // Handle retry logic
        const shouldRetry = await this._handleRetry(job, error, name);
        
        if (!shouldRetry) {
          this.emit('jobDead', {
            jobId,
            job,
            error,
            handler: name
          });
        }
        
        throw error;
      }
    };
    
    this.workers.set(name, { handler: wrappedHandler, options });
    this.emit('handlerRegistered', { name, options });
    
    return () => this.unregisterHandler(name);
  }
  
  /**
   * Add a job to the queue
   * @param {Object} job - Job object
   * @param {string} job.handlerName - Handler name
   * @param {*} job.payload - Job payload
   * @param {number} job.priority - Job priority (higher = more important)
   * @param {number} job.delay - Delay in ms before execution
   * @param {number} job.maxRetries - Maximum retry attempts
   * @returns {string} Job ID
   */
  async add(job) {
    const jobId = job.id || crypto.randomUUID();
    const enhancedJob = {
      id: jobId,
      ...job,
      createdAt: Date.now(),
      status: 'pending',
      attempts: 0,
      executeAt: Date.now() + (job.delay || 0)
    };
    
    // Apply middleware before adding
    const processedJob = await this._applyMiddleware('beforeAdd', enhancedJob);
    
    // Add to driver
    await this.driver.enqueue(this.options.name, processedJob);
    
    this.stats.totalJobs++;
    this.emit('jobAdded', { jobId, job: processedJob });
    
    return jobId;
  }
  
  /**
   * Start the scheduler
   */
  start() {
    if (this.isRunning) {
      return;
    }
    
    this.isRunning = true;
    this.processInterval = setInterval(
      () => this._processTick(),
      this.options.pollInterval
    );
    
    this.emit('schedulerStarted', {
      name: this.options.name,
      timestamp: Date.now()
    });
  }
  
  /**
   * Stop the scheduler
   */
  stop() {
    if (!this.isRunning) {
      return;
    }
    
    this.isRunning = false;
    if (this.processInterval) {
      clearInterval(this.processInterval);
      this.processInterval = null;
    }
    
    this.emit('schedulerStopped', {
      name: this.options.name,
      timestamp: Date.now(),
      stats: this.getStats()
    });
  }
  
  /**
   * Process tick - checks for and executes pending jobs
   */
  async _processTick() {
    if (this.activeJobs.size >= this.options.concurrency) {
      return;
    }
    
    try {
      // Get next job from driver
      const job = await this.driver.dequeue(this.options.name, {
        limit: this.options.concurrency - this.activeJobs.size
      });
      
      if (!job) {
        return;
      }
      
      // Process job
      await this._processJob(job);
      
    } catch (error) {
      this.emit('processError', { error, timestamp: Date.now() });
    }
  }
  
  /**
   * Process a single job
   * @param {Object} job - Job object
   */
  async _processJob(job) {
    const jobId = job.id;
    const handlerName = job.handlerName;
    
    if (!this.workers.has(handlerName)) {
      this.emit('handlerNotFound', { jobId, handlerName });
      return;
    }
    
    const worker = this.workers.get(handlerName);
    this.activeJobs.set(jobId, {
      job,
      startTime: Date.now(),
      handler: handlerName
    });
    
    try {
      await worker.handler(job);
      this.activeJobs.delete(jobId);
    } catch (error) {
      this.activeJobs.delete(jobId);
      // Error already handled in wrapped handler
    }
  }
  
  /**
   * Handle job retry logic
   * @param {Object} job - Job object
   * @param {Error} error - Error that caused failure
   * @param {string} handlerName - Handler name
   * @returns {boolean} Whether job was retried
   */
  async _handleRetry(job, error, handlerName) {
    const maxRetries = job.maxRetries || this.options.maxRetries;
    const currentAttempts = job.attempts || 0;
    
    if (currentAttempts >= maxRetries) {
      return false;
    }
    
    // Calculate retry delay with exponential backoff
    const retryDelay = this.options.retryDelay * Math.pow(2, currentAttempts);
    const retryJob = {
      ...job,
      attempts: currentAttempts + 1,
      delay: retryDelay,
      lastError: error.message,
      retryAt: Date.now() + retryDelay
    };
    
    this.stats.retriedJobs++;
    this.emit('jobRetry', {
      jobId: job.id,
      job: retryJob,
      attempt: currentAttempts + 1,
      delay: retryDelay,
      handler: handlerName
    });
    
    // Re-enqueue with delay
    await this.driver.enqueue(this.options.name, retryJob);
    
    return true;
  }
  
  /**
   * Add middleware to the scheduler
   * @param {string} stage - Middleware stage (beforeAdd, beforeExecute, afterExecute)
   * @param {Function} middleware - Middleware function
   */
  use(stage, middleware) {
    if (!this.middleware[stage]) {
      this.middleware[stage] = [];
    }
    this.middleware[stage].push(middleware);
  }
  
  /**
   * Apply middleware for a specific stage
   * @param {string} stage - Middleware stage
   * @param {*} data - Data to process
   * @returns {*} Processed data
   */
  async _applyMiddleware(stage, data) {
    if (!this.middleware[stage] || this.middleware[stage].length === 0) {
      return data;
    }
    
    let result = data;
    for (const middleware of this.middleware[stage]) {
      result = await middleware(result);
    }
    return result;
  }
  
  /**
   * Update statistics
   * @param {string} type - Stat type (completed, failed)
   * @param {number} duration - Processing duration
   */
  _updateStats(type, duration) {
    if (type === 'completed') {
      this.stats.completedJobs++;
      // Update average processing time
      const totalTime = this.stats.avgProcessingTime * (this.stats.completedJobs - 1) + duration;
      this.stats.avgProcessingTime = totalTime / this.stats.completedJobs;
    } else if (type === 'failed') {
      this.stats.failedJobs++;
    }
  }
  
  /**
   * Get scheduler statistics
   * @returns {Object} Statistics object
   */
  getStats() {
    return {
      ...this.stats,
      activeJobs: this.activeJobs.size,
      registeredHandlers: this.workers.size,
      isRunning: this.isRunning,
      concurrency: this.options.concurrency,
      successRate: this.stats.totalJobs > 0 
        ? ((this.stats.completedJobs / this.stats.totalJobs) * 100).toFixed(2) + '%'
        : '0%'
    };
  }
  
  /**
   * Get active jobs
   * @returns {Array} Active jobs
   */
  getActiveJobs() {
    return Array.from(this.activeJobs.entries()).map(([jobId, jobInfo]) => ({
      jobId,
      ...jobInfo,
      duration: Date.now() - jobInfo.startTime
    }));
  }
  
  /**
   * Validate driver interface
   * @private
   */
  _validateDriverInterface() {
    const requiredMethods = ['enqueue', 'dequeue', 'ack', 'nack'];
    for (const method of requiredMethods) {
      if (typeof this.driver[method] !== 'function') {
        throw new Error(`Driver must implement ${method} method`);
      }
    }
  }
}
