/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/core/QueueWorker
 */
import EventEmitter from 'events';
import crypto from 'crypto';

export default class QueueWorker extends EventEmitter {
  /**
   * Create a new QueueWorker instance
   * @param {Object} options - Worker configuration options
   * @param {string} options.name - Worker name for identification
   * @param {number} options.concurrency - Maximum concurrent job processing
   * @param {number} options.maxRetries - Maximum retry attempts for failed jobs
   * @param {number} options.retryDelay - Base delay in milliseconds for retries
   * @param {number} options.timeout - Job timeout in milliseconds
   * @param {boolean} options.autoStart - Whether to auto-start the worker
   * @param {Object} options.queue - Queue instance to process jobs from
   */
  constructor(options = {}) {
    super();
    
    this.options = {
      name: options.name || `worker-${crypto.randomBytes(4).toString('hex')}`,
      concurrency: options.concurrency || 1,
      maxRetries: options.maxRetries || 3,
      retryDelay: options.retryDelay || 1000,
      timeout: options.timeout || 30000,
      autoStart: options.autoStart !== false,
      queue: options.queue,
      ...options
    };
    
    this.id = crypto.randomUUID();
    this.jobs = new Map(); // jobId -> { task, status, startTime, handler, retries }
    this.handlers = new Map(); // handlerName -> { handler: Function, options: Object }
    this.isRunning = false;
    this.activeJobs = 0;
    this.pollInterval = null;
    
    // Statistics
    this.stats = {
      processed: 0,
      succeeded: 0,
      failed: 0,
      retried: 0,
      totalTime: 0,
      startedAt: null
    };
    
    // Middleware chain
    this.middleware = {
      beforeProcess: [],
      afterProcess: [],
      onError: []
    };
    
    if (this.options.autoStart) {
      this.start();
    }
  }
  
  /**
   * Register a job handler
   * @param {string} name - Handler name
   * @param {Function} handler - Handler function
   * @param {Object} options - Handler options
   * @returns {Function} Unregister function
   */
  registerHandler(name, handler, options = {}) {
    if (typeof handler !== 'function') {
      throw new Error(`Handler for "${name}" must be a function`);
    }
    
    const wrappedHandler = this._createWrappedHandler(name, handler, options);
    this.handlers.set(name, { handler: wrappedHandler, options });
    
    this.emit('handlerRegistered', { 
      name, 
      options,
      timestamp: Date.now() 
    });
    
    // Return unregister function
    return () => this.unregisterHandler(name);
  }
  
  /**
   * Create wrapped handler with error handling and retry logic
   * @private
   */
  _createWrappedHandler(name, handler, options) {
    return async (task) => {
      const jobId = task.id || crypto.randomUUID();
      const startTime = Date.now();
      
      // Create job record
      const jobRecord = {
        id: jobId,
        task,
        handler: name,
        status: 'processing',
        startTime,
        retries: task.retries || 0,
        maxRetries: task.maxRetries || this.options.maxRetries
      };
      
      this.jobs.set(jobId, jobRecord);
      this.activeJobs++;
      
      this.emit('jobStarted', { 
        jobId, 
        task, 
        handler: name,
        timestamp: startTime 
      });
      
      try {
        // Apply before-process middleware
        const processedTask = await this._applyMiddleware('beforeProcess', task);
        
        // Set timeout
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error(`Job timeout after ${this.options.timeout}ms`)), 
            this.options.timeout);
        });
        
        // Execute handler with timeout
        const result = await Promise.race([
          handler(processedTask),
          timeoutPromise
        ]);
        
        const duration = Date.now() - startTime;
        
        // Update statistics
        this.stats.processed++;
        this.stats.succeeded++;
        this.stats.totalTime += duration;
        
        // Clean up job record
        this.jobs.delete(jobId);
        this.activeJobs--;
        
        this.emit('jobCompleted', { 
          jobId, 
          task: processedTask, 
          result, 
          duration,
          handler: name,
          timestamp: Date.now()
        });
        
        // Apply after-process middleware
        await this._applyMiddleware('afterProcess', {
          jobId,
          task: processedTask,
          result,
          duration
        });
        
        return result;
      } catch (error) {
        const jobRecord = this.jobs.get(jobId);
        if (!jobRecord) {
          throw error;
        }
        
        jobRecord.retries++;
        jobRecord.lastError = error.message;
        
        this.emit('jobFailed', { 
          jobId, 
          task, 
          error, 
          retries: jobRecord.retries,
          handler: name,
          timestamp: Date.now()
        });
        
        // Apply error middleware
        await this._applyMiddleware('onError', {
          jobId,
          task,
          error,
          retries: jobRecord.retries
        });
        
        // Check if retry is needed
        if (jobRecord.retries < jobRecord.maxRetries) {
          this.stats.retried++;
          
          // Exponential backoff retry
          const delay = this.options.retryDelay * Math.pow(2, jobRecord.retries - 1);
          
          this.emit('jobRetry', { 
            jobId, 
            task, 
            retryCount: jobRecord.retries,
            delay,
            handler: name,
            timestamp: Date.now()
          });
          
          // Schedule retry
          setTimeout(() => {
            if (this.jobs.has(jobId)) {
              this._executeJob(jobId, task, name, handler);
            }
          }, delay);
          
          return null;
        } else {
          // Max retries exceeded
          this.jobs.delete(jobId);
          this.activeJobs--;
          this.stats.processed++;
          this.stats.failed++;
          
          this.emit('jobDead', { 
            jobId, 
            task, 
            error, 
            retries: jobRecord.retries,
            handler: name,
            timestamp: Date.now()
          });
          
          throw error;
        }
      }
    };
  }
  
  /**
   * Execute a job with the given handler
   * @private
   */
  async _executeJob(jobId, task, handlerName, handler) {
    const startTime = Date.now();
    const jobRecord = this.jobs.get(jobId);
    
    if (!jobRecord) {
      return;
    }
    
    jobRecord.status = 'processing';
    jobRecord.startTime = startTime;
    
    this.emit('jobExecuting', { 
      jobId, 
      task, 
      handler: handlerName,
      timestamp: startTime 
    });
    
    try {
      const result = await handler(task);
      const duration = Date.now() - startTime;
      
      this.jobs.delete(jobId);
      this.activeJobs--;
      this.stats.processed++;
      this.stats.succeeded++;
      this.stats.totalTime += duration;
      
      this.emit('jobCompleted', { 
        jobId, 
        task, 
        result, 
        duration,
        handler: handlerName,
        timestamp: Date.now()
      });
      
      return result;
    } catch (error) {
      const jobRecord = this.jobs.get(jobId);
      if (!jobRecord) {
        throw error;
      }
      
      jobRecord.retries++;
      jobRecord.lastError = error.message;
      
      this.emit('jobFailed', { 
        jobId, 
        task, 
        error, 
        retries: jobRecord.retries,
        handler: handlerName,
        timestamp: Date.now()
      });
      
      // Apply error middleware
      await this._applyMiddleware('onError', {
        jobId,
        task,
        error,
        retries: jobRecord.retries
      });
      
      // Check if retry is needed
      if (jobRecord.retries < jobRecord.maxRetries) {
        this.stats.retried++;
        
        // Exponential backoff retry
        const delay = this.options.retryDelay * Math.pow(2, jobRecord.retries - 1);
        
        this.emit('jobRetry', { 
          jobId, 
          task, 
          retryCount: jobRecord.retries,
          delay,
          handler: handlerName,
          timestamp: Date.now()
        });
        
        // Schedule retry
        setTimeout(() => {
          if (this.jobs.has(jobId)) {
            this._executeJob(jobId, task, handlerName, handler);
          }
        }, delay);
        
        return null;
      } else {
        // Max retries exceeded
        this.jobs.delete(jobId);
        this.activeJobs--;
        this.stats.processed++;
        this.stats.failed++;
        
        this.emit('jobDead', { 
          jobId, 
          task, 
          error, 
          retries: jobRecord.retries,
          handler: handlerName,
          timestamp: Date.now()
        });
        
        throw error;
      }
    }
  }
  
  /**
   * Add middleware to the worker
   * @param {string} stage - Middleware stage ('beforeProcess', 'afterProcess', 'onError')
   * @param {Function} middleware - Middleware function
   */
  use(stage, middleware) {
    if (!this.middleware[stage]) {
      throw new Error(`Invalid middleware stage: ${stage}. Valid stages: beforeProcess, afterProcess, onError`);
    }
    
    this.middleware[stage].push(middleware);
  }
  
  /**
   * Apply middleware for a specific stage
   * @private
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
   * Start processing jobs from the queue
   */
  start() {
    if (this.isRunning) {
      return;
    }
    
    this.isRunning = true;
    this.stats.startedAt = Date.now();
    
    if (this.options.queue) {
      this.pollInterval = setInterval(() => {
        this._pollQueue();
      }, 100); // Poll every 100ms
    }
    
    this.emit('started', { 
      id: this.id, 
      name: this.options.name,
      timestamp: this.stats.startedAt 
    });
  }
  
  /**
   * Poll queue for new jobs
   * @private
   */
  async _pollQueue() {
    if (!this.isRunning || this.activeJobs >= this.options.concurrency) {
      return;
    }
    
    try {
      const availableSlots = this.options.concurrency - this.activeJobs;
      
      for (let i = 0; i < availableSlots; i++) {
        const job = await this.options.queue.dequeue();
        if (!job) {
          break;
        }
        
        const handlerName = job.handlerName || job.type;
        const handlerInfo = this.handlers.get(handlerName);
        
        if (!handlerInfo) {
          this.emit('handlerNotFound', { 
            jobId: job.id, 
            handlerName,
            timestamp: Date.now()
          });
          continue;
        }
        
        // Process the job
        handlerInfo.handler(job).catch(error => {
          this.emit('jobProcessingError', { 
            jobId: job.id, 
            error,
            timestamp: Date.now()
          });
        });
      }
    } catch (error) {
      this.emit('pollError', { 
        error,
        timestamp: Date.now()
      });
    }
  }
  
  /**
   * Process a single job directly
   * @param {Object} task - Job task
   * @returns {Promise} Job result
   */
  async process(task) {
    if (!this.isRunning) {
      throw new Error('Worker is not running');
    }
    
    const handlerName = task.handlerName || task.type;
    const handlerInfo = this.handlers.get(handlerName);
    
    if (!handlerInfo) {
      throw new Error(`No handler registered for ${handlerName}`);
    }
    
    return await handlerInfo.handler(task);
  }
  
  /**
   * Process multiple jobs in batch
   * @param {Array} tasks - Array of job tasks
   * @param {Object} options - Batch processing options
   * @returns {Promise} Batch results
   */
  async processBatch(tasks, options = {}) {
    if (!this.isRunning) {
      throw new Error('Worker is not running');
    }
    
    const { 
      batchSize = this.options.concurrency,
      stopOnError = false,
      parallel = true
    } = options;
    
    const results = [];
    const errors = [];
    
    if (parallel) {
      // Process in parallel batches
      for (let i = 0; i < tasks.length; i += batchSize) {
        const batch = tasks.slice(i, i + batchSize);
        const batchPromises = batch.map(task => 
          this.process(task).catch(err => {
            if (stopOnError) throw err;
            errors.push({ task, error: err });
            return null;
          })
        );
        
        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults.filter(r => r !== null));
        
        if (stopOnError && errors.length > 0) {
          break;
        }
      }
    } else {
      // Process sequentially
      for (const task of tasks) {
        try {
          const result = await this.process(task);
          results.push(result);
        } catch (error) {
          if (stopOnError) {
            throw error;
          }
          errors.push({ task, error });
        }
      }
    }
    
    return { results, errors };
  }
  
  /**
   * Stop the worker
   */
  stop() {
    if (!this.isRunning) {
      return;
    }
    
    this.isRunning = false;
    
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
    
    this.emit('stopped', { 
      id: this.id, 
      name: this.options.name,
      stats: this.getStats(),
      timestamp: Date.now() 
    });
  }
  
  /**
   * Pause the worker
   */
  pause() {
    this.isRunning = false;
    this.emit('paused', { 
      id: this.id, 
      name: this.options.name,
      timestamp: Date.now() 
    });
  }
  
  /**
   * Resume the worker
   */
  resume() {
    this.isRunning = true;
    this.emit('resumed', { 
      id: this.id, 
      name: this.options.name,
      timestamp: Date.now() 
    });
  }
  
  /**
   * Get worker status
   * @returns {Object} Status object
   */
  getStatus() {
    return {
      id: this.id,
      name: this.options.name,
      isRunning: this.isRunning,
      activeJobs: this.activeJobs,
      registeredHandlers: this.handlers.size,
      ...this.getStats()
    };
  }
  
  /**
   * Get worker statistics
   * @returns {Object} Statistics object
   */
  getStats() {
    const avgTime = this.stats.processed > 0 
      ? this.stats.totalTime / this.stats.processed 
      : 0;
    
    const successRate = this.stats.processed > 0
      ? (this.stats.succeeded / this.stats.processed) * 100
      : 0;
    
    const uptime = this.stats.startedAt ? Date.now() - this.stats.startedAt : 0;
    
    return {
      ...this.stats,
      avgTime: Math.round(avgTime),
      successRate: `${successRate.toFixed(2)}%`,
      uptime,
      activeJobs: this.activeJobs,
      concurrency: this.options.concurrency
    };
  }
  
  /**
   * Get active jobs
   * @returns {Array} Array of active jobs
   */
  getActiveJobs() {
    const jobs = [];
    for (const [jobId, job] of this.jobs.entries()) {
      jobs.push({
        jobId,
        ...job,
        duration: job.startTime ? Date.now() - job.startTime : 0
      });
    }
    return jobs;
  }
  
  /**
   * Cancel a job
   * @param {string} jobId - Job ID
   * @returns {boolean} True if job was cancelled
   */
  cancelJob(jobId) {
    const job = this.jobs.get(jobId);
    if (job) {
      this.jobs.delete(jobId);
      this.activeJobs--;
      this.emit('jobCancelled', { 
        jobId, 
        task: job.task,
        timestamp: Date.now()
      });
      return true;
    }
    return false;
  }
  
  /**
   * Cleanup completed jobs
   * @returns {number} Number of jobs cleaned up
   */
  cleanup() {
    const now = Date.now();
    let cleaned = 0;
    
    for (const [jobId, job] of this.jobs.entries()) {
      // Cleanup timed out jobs
      if (job.startTime && now - job.startTime > this.options.timeout * 2) {
        this.jobs.delete(jobId);
        this.activeJobs--;
        cleaned++;
        this.emit('jobTimeout', { 
          jobId, 
          task: job.task,
          timestamp: now
        });
      }
    }
    
    return cleaned;
  }
  
  /**
   * Unregister a handler
   * @param {string} name - Handler name
   * @returns {boolean} True if handler was unregistered
   */
  unregisterHandler(name) {
    const existed = this.handlers.delete(name);
    if (existed) {
      this.emit('handlerUnregistered', { 
        name,
        timestamp: Date.now()
      });
    }
    return existed;
  }
  
  /**
   * Get all registered handler names
   * @returns {Array} Array of handler names
   */
  getHandlerNames() {
    return Array.from(this.handlers.keys());
  }
  
  /**
   * Check if handler is registered
   * @param {string} name - Handler name
   * @returns {boolean} True if handler is registered
   */
  hasHandler(name) {
    return this.handlers.has(name);
  }
}
