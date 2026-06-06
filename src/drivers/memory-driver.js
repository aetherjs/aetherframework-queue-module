/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/drivers/memory-driver
 */

import crypto from 'crypto';
import fs from 'fs/promises';
import path from 'path';

import EventEmitter from 'events';

export default class MemoryDriver extends EventEmitter {

  /**
   * Create a new MemoryDriver instance
   * @param {Object} options - Driver configuration
   * @param {number} options.maxSize - Maximum queue size
   * @param {boolean} options.persist - Whether to persist to disk
   * @param {string} options.persistencePath - Path for persistence
   */
   constructor(options = {}) {


     super();
    this.options = {
      maxSize: options.maxSize || 10000,
      persist: options.persist || false,
      persistencePath: options.persistencePath || './queue-data',
      ...options
    };
    
    this.queues = new Map(); // queueName -> { jobs: [], processing: Set }
    this.jobs = new Map(); // jobId -> job data
    this.handlers = new Map(); // queueName -> handler function
    this.isRunning = false;
    this.processing = new Set();
    
    // Initialize persistence if enabled
    if (this.options.persist) {
      this._initPersistence();
    }
  }
  // 在 MemoryDriver 类的 constructor 方法后添加

/**
 * Start the memory driver and begin processing jobs
 * @returns {Promise<void>}
 */
async start() {
  this.isRunning = true;
  
  // Start automatic job processing
  this._startAutoProcessing();
  
  return Promise.resolve();
}

/**
 * Stop the memory driver
 * @returns {Promise<void>}
 */
async stop() {
  this.isRunning = false;
  
  // Stop auto processing
  if (this._processingInterval) {
    clearInterval(this._processingInterval);
    this._processingInterval = null;
  }
  
  return Promise.resolve();
}

/**
 * Start automatic job processing
 * @private
 */
_startAutoProcessing() {
  if (this._processingInterval) {
    clearInterval(this._processingInterval);
  }
  
  this._processingInterval = setInterval(async () => {
    if (!this.isRunning) return;
    
    // Process all queues
    for (const [queueName, queue] of this.queues.entries()) {
      const handler = this.handlers.get(queueName);
      if (handler && queue.jobs.length > 0) {
        try {
          const job = await this.dequeue(queueName);
          if (job) {
            // Emit job started event
            this.emit('jobStarted', { queueName, jobId: job.id, job });
            
            try {
              const result = await handler(job);
              await this.ack(job.id, result);
              // Emit job completed event
              this.emit('jobCompleted', { queueName, jobId: job.id, result });
            } catch (error) {
              await this.nack(job.id, error);
              // Emit job failed event
              this.emit('jobFailed', { queueName, jobId: job.id, error });
            }
          }
        } catch (error) {
          console.error(`Error processing queue ${queueName}:`, error);
        }
      }
    }
  }, 100); // Process every 100ms for better responsiveness
}

  /**
   * Enqueue a job
   * @param {string} queueName - Queue name
   * @param {Object} job - Job object
   * @returns {Promise<string>} Job ID
   */
  async enqueue(queueName, job) {
    if (!this.queues.has(queueName)) {
      this.queues.set(queueName, {
        jobs: [],
        processing: new Set(),
        stats: {
          enqueued: 0,
          dequeued: 0,
          completed: 0,
          failed: 0
        }
      });
    }
    
    const queue = this.queues.get(queueName);
    const jobId = job.id || `job_${crypto.randomUUID()}`;
    
    const jobData = {
      ...job,
      id: jobId,
      queue: queueName,
      status: 'pending',
      enqueuedAt: Date.now(),
      executeAt: job.executeAt || Date.now() + (job.delay || 0),
      attempts: job.attempts || 0
    };
    
    // Check queue size limit
    if (queue.jobs.length >= this.options.maxSize) {
      throw new Error(`Queue "${queueName}" has reached maximum size of ${this.options.maxSize}`);
    }
    
    // Add to queue (sorted by executeAt for delayed jobs)
    queue.jobs.push(jobData);
    queue.jobs.sort((a, b) => a.executeAt - b.executeAt);
    
    // Store in job map
    this.jobs.set(jobId, jobData);
    
    // Update statistics
    queue.stats.enqueued++;
    
    // Persist if enabled
    if (this.options.persist) {
      await this._persistJob(jobData);
    }
    
    return jobId;
  }
  
  /**
   * Dequeue a job
   * @param {string} queueName - Queue name
   * @param {Object} options - Dequeue options
   * @returns {Promise<Object|null>} Job or null
   */
  async dequeue(queueName, options = {}) {
    if (!this.queues.has(queueName)) {
      return null;
    }
    
    const queue = this.queues.get(queueName);
    const now = Date.now();
    
    // Find next available job
    const jobIndex = queue.jobs.findIndex(job => 
      job.status === 'pending' && job.executeAt <= now
    );
    
    if (jobIndex === -1) {
      return null;
    }
    
    const job = queue.jobs[jobIndex];
    
    // Move to processing set
    job.status = 'processing';
    job.startedAt = now;
    job.attempts = (job.attempts || 0) + 1;
    
    queue.processing.add(job.id);
    queue.jobs.splice(jobIndex, 1);
    
    // Update statistics
    queue.stats.dequeued++;
    
    // Persist if enabled
    if (this.options.persist) {
      await this._persistJob(job);
    }
    
    return job;
  }
  
  /**
   * Acknowledge job completion
   * @param {string} jobId - Job ID
   * @param {*} result - Job result
   * @returns {Promise<void>}
   */
  async ack(jobId, result) {
    const job = this.jobs.get(jobId);
    if (!job) {
      return;
    }
    
    const queueName = job.queue;
    if (this.queues.has(queueName)) {
      const queue = this.queues.get(queueName);
      queue.processing.delete(jobId);
      queue.stats.completed++;
    }
    
    job.status = 'completed';
    job.completedAt = Date.now();
    job.result = result;
    
    // Remove from memory after completion
    setTimeout(() => {
      this.jobs.delete(jobId);
    }, 60000); // Keep for 1 minute for debugging
    
    // Persist if enabled
    if (this.options.persist) {
      await this._persistJob(job);
    }
  }
  
  /**
   * Negative acknowledge job failure
   * @param {string} jobId - Job ID
   * @param {Error} error - Error object
   * @returns {Promise<void>}
   */
  async nack(jobId, error) {
    const job = this.jobs.get(jobId);
    if (!job) {
      return;
    }
    
    const queueName = job.queue;
    if (this.queues.has(queueName)) {
      const queue = this.queues.get(queueName);
      queue.processing.delete(jobId);
      queue.stats.failed++;
    }
    
    job.status = 'failed';
    job.failedAt = Date.now();
    job.error = error.message;
    
    // Persist if enabled
    if (this.options.persist) {
      await this._persistJob(job);
    }
  }
  
  /**
   * Register a handler for a queue
   * @param {string} queueName - Queue name
   * @param {Function} handler - Handler function
   */
  registerHandler(queueName, handler) {
    this.handlers.set(queueName, handler);
  }
  
  /**
   * Get queue statistics
   * @param {string} queueName - Queue name
   * @returns {Promise<Object>} Queue statistics
   */
  async getStats(queueName) {
    if (!this.queues.has(queueName)) {
      return null;
    }
    
    const queue = this.queues.get(queueName);
    const now = Date.now();
    
    return {
      queue: queueName,
      pending: queue.jobs.length,
      processing: queue.processing.size,
      ...queue.stats,
      memoryUsage: process.memoryUsage().heapUsed,
      uptime: process.uptime()
    };
  }
  
  /**
   * Clear a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<void>}
   */
  async clear(queueName) {
    if (this.queues.has(queueName)) {
      const queue = this.queues.get(queueName);
      
      // Remove all jobs from this queue
      queue.jobs.forEach(job => this.jobs.delete(job.id));
      queue.processing.forEach(jobId => this.jobs.delete(jobId));
      
      // Reset queue
      queue.jobs = [];
      queue.processing.clear();
      queue.stats = {
        enqueued: 0,
        dequeued: 0,
        completed: 0,
        failed: 0
      };
    }
  }
  
  /**
   * Pause a queue
   * @param {string} queueName - Queue name
   */
  pause(queueName) {
    // In memory driver, pausing is handled by the scheduler
    // This is a no-op for the driver itself
  }
  
  /**
   * Resume a queue
   * @param {string} queueName - Queue name
   */
  resume(queueName) {
    // In memory driver, resuming is handled by the scheduler
    // This is a no-op for the driver itself
  }
  
  /**
   * Initialize persistence
   * @private
   */
  _initPersistence() {
    // Create persistence directory if it doesn't exist
    if (!fs.existsSync(this.options.persistencePath)) {
      fs.mkdirSync(this.options.persistencePath, { recursive: true });
    }
    
    this.persistencePath = path.resolve(this.options.persistencePath);
  }
  
  /**
   * Persist job to disk
   * @param {Object} job - Job object
   * @private
   */
  async _persistJob(job) {
    if (!this.options.persist) {
      return;
    }
    
    const queueDir = path.join(this.persistencePath, job.queue);
    const jobFile = path.join(queueDir, `${job.id}.json`);
    
    try {
      // Create queue directory if it doesn't exist
      await fs.mkdir(queueDir, { recursive: true });
      
      // Write job to file
      await fs.writeFile(jobFile, JSON.stringify(job, null, 2));
    } catch (error) {
      console.error(`Failed to persist job ${job.id}:`, error);
    }
  }
  
  /**
   * Load persisted jobs
   * @param {string} queueName - Queue name
   * @returns {Promise<void>}
   */
  async loadPersistedJobs(queueName) {
    if (!this.options.persist) {
      return;
    }
    
    const queueDir = path.join(this.persistencePath, queueName);
    
    try {
      const files = await fs.readdir(queueDir);
      
      for (const file of files) {
        if (file.endsWith('.json')) {
          const filePath = path.join(queueDir, file);
          const data = await fs.readFile(filePath, 'utf8');
          const job = JSON.parse(data);
          
          // Add to memory
          this.jobs.set(job.id, job);
          
          if (!this.queues.has(queueName)) {
            this.queues.set(queueName, {
              jobs: [],
              processing: new Set(),
              stats: { enqueued: 0, dequeued: 0, completed: 0, failed: 0 }
            });
          }
          
          const queue = this.queues.get(queueName);
          if (job.status === 'pending') {
            queue.jobs.push(job);
          } else if (job.status === 'processing') {
            queue.processing.add(job.id);
          }
          
          // Update stats
          queue.stats.enqueued++;
          if (job.status === 'completed') queue.stats.completed++;
          if (job.status === 'failed') queue.stats.failed++;
        }
      }
      
      // Sort jobs by executeAt
      if (this.queues.has(queueName)) {
        const queue = this.queues.get(queueName);
        queue.jobs.sort((a, b) => a.executeAt - b.executeAt);
      }
      
    } catch (error) {
      // Directory might not exist yet
      if (error.code !== 'ENOENT') {
        console.error(`Failed to load persisted jobs for ${queueName}:`, error);
      }
    }
  }
}
