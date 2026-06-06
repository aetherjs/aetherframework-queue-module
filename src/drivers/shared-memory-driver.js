/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/drivers/shared-memory-driver
 */

import crypto from 'crypto';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';

export default class SharedMemoryDriver {
  /**
   * Create a new SharedMemoryDriver instance
   * @param {Object} options - Driver configuration
   * @param {string} options.sharedPath - Shared directory path for inter-process communication
   * @param {number} options.lockTimeout - Lock timeout in milliseconds (default: 5000)
   * @param {number} options.cleanupInterval - Cleanup interval in milliseconds (default: 60000)
   * @param {Object} options.clusterManager - Cluster manager instance for coordination
   * @param {string} options.nodeId - Unique node identifier
   * @param {number} options.maxSize - Maximum queue size per node
   * @param {boolean} options.persist - Whether to persist data to disk
   */
  constructor(options = {}) {
    this.options = {
      sharedPath: options.sharedPath || path.join(os.tmpdir(), 'queue-shared'),
      lockTimeout: options.lockTimeout || 5000,
      cleanupInterval: options.cleanupInterval || 60000,
      clusterManager: options.clusterManager,
      nodeId: options.nodeId || `node_${crypto.randomBytes(4).toString('hex')}`,
      maxSize: options.maxSize || 10000,
      persist: options.persist !== false,
      ...options
    };
    
    // Internal state management
    this.queues = new Map(); // queueName -> { jobs: [], processing: Set, stats: {...} }
    this.jobs = new Map(); // jobId -> job data
    this.locks = new Map(); // resource -> lock data
    this.cleanupInterval = null;
    this.nodeId = this.options.nodeId;
    this.isLeader = false;
    
    // Statistics
    this.stats = {
      enqueued: 0,
      dequeued: 0,
      completed: 0,
      failed: 0,
      nodeId: this.nodeId,
      lastSync: Date.now()
    };
    
    // Initialize shared directory and start cleanup process
    this._initSharedDirectory();
    this._startCleanup();
    
    // Register with cluster manager if available
    if (this.options.clusterManager) {
      this.options.clusterManager.registerNode(this.nodeId, this);
    }
  }
  
  /**
   * Initialize shared directory structure
   * @private
   */
  async _initSharedDirectory() {
    try {
      // Create main shared directory
      await fs.mkdir(this.options.sharedPath, { recursive: true });
      
      // Create subdirectories
      await fs.mkdir(path.join(this.options.sharedPath, 'queues'), { recursive: true });
      await fs.mkdir(path.join(this.options.sharedPath, 'locks'), { recursive: true });
      await fs.mkdir(path.join(this.options.sharedPath, 'state'), { recursive: true });
      await fs.mkdir(path.join(this.options.sharedPath, 'dead-letter'), { recursive: true });
    } catch (error) {
      console.error('Failed to initialize shared directory:', error);
      throw error;
    }
  }
  
  /**
   * Acquire a distributed lock for a resource
   * @param {string} resource - Resource name to lock
   * @returns {Promise<boolean>} True if lock acquired, false otherwise
   * @private
   */
  async _acquireLock(resource) {
    const lockFile = path.join(this.options.sharedPath, 'locks', `${resource}.lock`);
    const lockData = {
      nodeId: this.nodeId,
      timestamp: Date.now(),
      expiresAt: Date.now() + this.options.lockTimeout
    };
    
    try {
      // Try to create lock file
      await fs.writeFile(lockFile, JSON.stringify(lockData, null, 2), { flag: 'wx' });
      this.locks.set(resource, lockData);
      return true;
    } catch (error) {
      if (error.code === 'EEXIST') {
        // Lock file exists, check if expired
        try {
          const existingLock = JSON.parse(await fs.readFile(lockFile, 'utf8'));
          
          if (existingLock.expiresAt < Date.now()) {
            // Lock expired, try to acquire it
            await fs.writeFile(lockFile, JSON.stringify(lockData, null, 2));
            this.locks.set(resource, lockData);
            return true;
          }
          
          // Lock is still valid
          return false;
        } catch (readError) {
          // Lock file corrupted, try to acquire it
          await fs.writeFile(lockFile, JSON.stringify(lockData, null, 2));
          this.locks.set(resource, lockData);
          return true;
        }
      }
      
      // Other error
      console.error(`Failed to acquire lock for ${resource}:`, error);
      return false;
    }
  }
  
  /**
   * Release a distributed lock
   * @param {string} resource - Resource name to unlock
   * @returns {Promise<void>}
   * @private
   */
  async _releaseLock(resource) {
    const lockFile = path.join(this.options.sharedPath, 'locks', `${resource}.lock`);
    
    try {
      await fs.unlink(lockFile);
    } catch (error) {
      // Ignore if lock file doesn't exist
      if (error.code !== 'ENOENT') {
        console.error(`Failed to release lock for ${resource}:`, error);
      }
    }
    
    this.locks.delete(resource);
  }
  
  /**
   * Load shared state from disk for a specific queue
   * @param {string} queueName - Queue name
   * @returns {Promise<void>}
   * @private
   */
  async _loadQueueState(queueName) {
    const stateFile = path.join(this.options.sharedPath, 'state', `${queueName}.json`);
    
    try {
      const data = await fs.readFile(stateFile, 'utf8');
      const state = JSON.parse(data);
      
      // Merge with local state
      if (!this.queues.has(queueName)) {
        this.queues.set(queueName, {
          jobs: [],
          processing: new Set(),
          stats: { enqueued: 0, dequeued: 0, completed: 0, failed: 0 }
        });
      }
      
      const queue = this.queues.get(queueName);
      
      // Add jobs from shared state
      if (state.jobs && Array.isArray(state.jobs)) {
        state.jobs.forEach(job => {
          if (!this.jobs.has(job.id)) {
            this.jobs.set(job.id, job);
            
            if (job.status === 'pending') {
              queue.jobs.push(job);
            } else if (job.status === 'processing') {
              queue.processing.add(job.id);
            }
          }
        });
        
        // Sort by execution time
        queue.jobs.sort((a, b) => a.executeAt - b.executeAt);
      }
      
      // Merge statistics
      if (state.stats) {
        Object.keys(state.stats).forEach(key => {
          queue.stats[key] = (queue.stats[key] || 0) + state.stats[key];
        });
      }
      
    } catch (error) {
      // File doesn't exist or is corrupted, start fresh
      if (error.code !== 'ENOENT') {
        console.error(`Failed to load state for queue ${queueName}:`, error);
      }
    }
  }
  
  /**
   * Save queue state to disk
   * @param {string} queueName - Queue name
   * @returns {Promise<void>}
   * @private
   */
  async _saveQueueState(queueName) {
    if (!this.options.persist) {
      return;
    }
    
    const stateFile = path.join(this.options.sharedPath, 'state', `${queueName}.json`);
    
    try {
      const queue = this.queues.get(queueName);
      if (!queue) {
        return;
      }
      
      const state = {
        jobs: Array.from(this.jobs.values()).filter(job => job.queue === queueName),
        stats: queue.stats,
        lastUpdated: Date.now(),
        nodeId: this.nodeId
      };
      
      await fs.writeFile(stateFile, JSON.stringify(state, null, 2));
    } catch (error) {
      console.error(`Failed to save state for queue ${queueName}:`, error);
    }
  }
  
  /**
   * Start periodic cleanup process
   * @private
   */
  _startCleanup() {
    this.cleanupInterval = setInterval(async () => {
      await this._cleanupExpiredLocks();
      await this._cleanupStaleJobs();
    }, this.options.cleanupInterval);
  }
  
  /**
   * Cleanup expired locks
   * @private
   */
  async _cleanupExpiredLocks() {
    const locksDir = path.join(this.options.sharedPath, 'locks');
    
    try {
      const files = await fs.readdir(locksDir);
      
      for (const file of files) {
        if (file.endsWith('.lock')) {
          const lockFile = path.join(locksDir, file);
          try {
            const lockData = JSON.parse(await fs.readFile(lockFile, 'utf8'));
            
            if (lockData.expiresAt < Date.now()) {
              // Lock expired, remove it
              await fs.unlink(lockFile);
              const resource = file.replace('.lock', '');
              this.locks.delete(resource);
            }
          } catch (error) {
            // Corrupted lock file, remove it
            await fs.unlink(lockFile);
          }
        }
      }
    } catch (error) {
      // Directory might not exist yet
      if (error.code !== 'ENOENT') {
        console.error('Failed to cleanup expired locks:', error);
      }
    }
  }
  
  /**
   * Cleanup stale jobs (jobs stuck in processing for too long)
   * @private
   */
  async _cleanupStaleJobs() {
    const now = Date.now();
    const staleThreshold = 5 * 60 * 1000; // 5 minutes
    
    for (const [queueName, queue] of this.queues.entries()) {
      const staleJobs = [];
      
      for (const jobId of queue.processing) {
        const job = this.jobs.get(jobId);
        if (job && job.startedAt && (now - job.startedAt) > staleThreshold) {
          staleJobs.push(jobId);
        }
      }
      
      if (staleJobs.length > 0) {
        // Acquire lock for this queue
        if (await this._acquireLock(`queue-${queueName}-cleanup`)) {
          try {
            for (const jobId of staleJobs) {
              const job = this.jobs.get(jobId);
              if (job) {
                // Move to dead letter queue
                job.status = 'dead';
                job.deadReason = 'stale';
                job.deadAt = now;
                
                queue.processing.delete(jobId);
                queue.stats.failed++;
                
                // Save to dead letter queue
                await this._saveToDeadLetter(job);
                
              }
            }
          } finally {
            await this._releaseLock(`queue-${queueName}-cleanup`);
          }
        }
      }
    }
  }
  
  /**
   * Save job to dead letter queue
   * @param {Object} job - Job object
   * @private
   */
  async _saveToDeadLetter(job) {
    const dlqFile = path.join(
      this.options.sharedPath, 
      'dead-letter', 
      `${job.queue}_${Date.now()}_${job.id}.json`
    );
    
    try {
      await fs.writeFile(dlqFile, JSON.stringify(job, null, 2));
    } catch (error) {
      console.error(`Failed to save job ${job.id} to dead letter queue:`, error);
    }
  }
  
  /**
   * Enqueue a job with distributed locking
   * @param {string} queueName - Queue name
   * @param {Object} job - Job object
   * @param {Object} options - Enqueue options
   * @returns {Promise<string>} Job ID
   */
  async enqueue(queueName, job, options = {}) {
    // Acquire lock for this queue
    if (!(await this._acquireLock(`queue-${queueName}`))) {
      throw new Error(`Failed to acquire lock for queue ${queueName}`);
    }
    
    try {
      // Load current state
      await this._loadQueueState(queueName);
      
      if (!this.queues.has(queueName)) {
        this.queues.set(queueName, {
          jobs: [],
          processing: new Set(),
          stats: { enqueued: 0, dequeued: 0, completed: 0, failed: 0 }
        });
      }
      
      const queue = this.queues.get(queueName);
      
      // Check queue size limit
      if (queue.jobs.length >= this.options.maxSize) {
        throw new Error(`Queue "${queueName}" has reached maximum size of ${this.options.maxSize}`);
      }
      
      const jobId = job.id || `job_${crypto.randomUUID()}`;
      const jobData = {
        ...job,
        id: jobId,
        queue: queueName,
        nodeId: this.nodeId,
        status: 'pending',
        enqueuedAt: Date.now(),
        executeAt: Date.now() + (job.delay || 0),
        attempts: job.attempts || 0,
        priority: job.priority || 0
      };
      
      // Add to queue with priority sorting
      queue.jobs.push(jobData);
      queue.jobs.sort((a, b) => {
        // First by priority (higher number = higher priority)
        const priorityDiff = (b.priority || 0) - (a.priority || 0);
        if (priorityDiff !== 0) return priorityDiff;
        
        // Then by execution time
        return a.executeAt - b.executeAt;
      });
      
      // Store in job map
      this.jobs.set(jobId, jobData);
      
      // Update statistics
      queue.stats.enqueued++;
      this.stats.enqueued++;
      
      // Save state
      await this._saveQueueState(queueName);
      
      return jobId;
    } finally {
      await this._releaseLock(`queue-${queueName}`);
    }
  }
  
  /**
   * Dequeue a job with distributed locking
   * @param {string} queueName - Queue name
   * @param {Object} options - Dequeue options
   * @returns {Promise<Object|null>} Job or null
   */
  async dequeue(queueName, options = {}) {
    // Acquire lock for this queue
    if (!(await this._acquireLock(`queue-${queueName}`))) {
      throw new Error(`Failed to acquire lock for queue ${queueName}`);
    }
    
    try {
      // Load current state
      await this._loadQueueState(queueName);
      
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
      job.processingNode = this.nodeId;
      
      queue.processing.add(job.id);
      queue.jobs.splice(jobIndex, 1);
      
      // Update statistics
      queue.stats.dequeued++;
      this.stats.dequeued++;
      
      // Save state
      await this._saveQueueState(queueName);
      
      return job;
    } finally {
      await this._releaseLock(`queue-${queueName}`);
    }
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
    
    // Acquire lock for this queue
    if (!(await this._acquireLock(`queue-${queueName}`))) {
      throw new Error(`Failed to acquire lock for queue ${queueName}`);
    }
    
    try {
      if (this.queues.has(queueName)) {
        const queue = this.queues.get(queueName);
        queue.processing.delete(jobId);
        queue.stats.completed++;
        this.stats.completed++;
      }
      
      job.status = 'completed';
      job.completedAt = Date.now();
      job.result = result;
      job.completedNode = this.nodeId;
      
      // Save state
      await this._saveQueueState(queueName);
      
      // Remove from memory after some time
      setTimeout(() => {
        this.jobs.delete(jobId);
      }, 60000); // Keep for 1 minute for debugging
      
    } finally {
      await this._releaseLock(`queue-${queueName}`);
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
    
    // Acquire lock for this queue
    if (!(await this._acquireLock(`queue-${queueName}`))) {
      throw new Error(`Failed to acquire lock for queue ${queueName}`);
    }
    
    try {
      if (this.queues.has(queueName)) {
        const queue = this.queues.get(queueName);
        queue.processing.delete(jobId);
        queue.stats.failed++;
        this.stats.failed++;
      }
      
      job.status = 'failed';
      job.failedAt = Date.now();
      job.error = error.message;
      job.failedNode = this.nodeId;
      
      // Save state
      await this._saveQueueState(queueName);
      
    } finally {
      await this._releaseLock(`queue-${queueName}`);
    }
  }
  
  /**
   * Register a handler for a queue
   * @param {string} queueName - Queue name
   * @param {Function} handler - Handler function
   */
  registerHandler(queueName, handler) {
    // In shared memory driver, handlers are typically registered at the scheduler level
    // This method is kept for API compatibility
    console.log(`Handler registered for queue ${queueName} on node ${this.nodeId}`);
  }
  
  /**
   * Get queue statistics
   * @param {string} queueName - Queue name
   * @returns {Promise<Object>} Queue statistics
   */
  async getStats(queueName) {
    // Load current state
    await this._loadQueueState(queueName);
    
    if (!this.queues.has(queueName)) {
      return null;
    }
    
    const queue = this.queues.get(queueName);
    const now = Date.now();
    
    // Calculate additional metrics
    const pendingJobs = queue.jobs.filter(job => job.status === 'pending');
    const processingJobs = Array.from(queue.processing).map(id => this.jobs.get(id)).filter(Boolean);
    const completedJobs = Array.from(this.jobs.values())
      .filter(job => job.queue === queueName && job.status === 'completed');
    const failedJobs = Array.from(this.jobs.values())
      .filter(job => job.queue === queueName && job.status === 'failed');
    
    return {
      queue: queueName,
      nodeId: this.nodeId,
      pending: pendingJobs.length,
      processing: processingJobs.length,
      completed: completedJobs.length,
      failed: failedJobs.length,
      deadLetter: 0, // Would need to scan dead letter directory
      ...queue.stats,
      memoryUsage: process.memoryUsage().heapUsed,
      uptime: process.uptime(),
      lastSync: this.stats.lastSync,
      isLeader: this.isLeader
    };
  }
  
  /**
   * Clear a queue
   * @param {string} queueName - Queue name
   * @returns {Promise<void>}
   */
  async clear(queueName) {
    // Acquire lock for this queue
    if (!(await this._acquireLock(`queue-${queueName}-clear`))) {
      throw new Error(`Failed to acquire lock for clearing queue ${queueName}`);
    }
    
    try {
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
        
        // Remove state file
        const stateFile = path.join(this.options.sharedPath, 'state', `${queueName}.json`);
        try {
          await fs.unlink(stateFile);
        } catch (error) {
          // Ignore if file doesn't exist
        }
      }
    } finally {
      await this._releaseLock(`queue-${queueName}-clear`);
    }
  }
  
  /**
   * Pause a queue (mark as paused in shared state)
   * @param {string} queueName - Queue name
   */
  async pause(queueName) {
    const pauseFile = path.join(this.options.sharedPath, 'queues', `${queueName}.pause`);
    
    try {
      await fs.writeFile(pauseFile, JSON.stringify({
        paused: true,
        pausedAt: Date.now(),
        pausedBy: this.nodeId
      }, null, 2));
    } catch (error) {
      console.error(`Failed to pause queue ${queueName}:`, error);
    }
  }
  
  /**
   * Resume a queue (remove pause marker)
   * @param {string} queueName - Queue name
   */
  async resume(queueName) {
    const pauseFile = path.join(this.options.sharedPath, 'queues', `${queueName}.pause`);
    
    try {
      await fs.unlink(pauseFile);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        console.error(`Failed to resume queue ${queueName}:`, error);
      }
    }
  }
  
  /**
   * Check if queue is paused
   * @param {string} queueName - Queue name
   * @returns {Promise<boolean>}
   */
  async isPaused(queueName) {
    const pauseFile = path.join(this.options.sharedPath, 'queues', `${queueName}.pause`);
    
    try {
      await fs.access(pauseFile);
      return true;
    } catch (error) {
      return false;
    }
  }
  
  /**
   * Get all queues in the system
   * @returns {Promise<Array>} Array of queue names
   */
  async getQueues() {
    const queuesDir = path.join(this.options.sharedPath, 'state');
    
    try {
      const files = await fs.readdir(queuesDir);
      return files
        .filter(file => file.endsWith('.json'))
        .map(file => file.replace('.json', ''));
    } catch (error) {
      if (error.code === 'ENOENT') {
        return [];
      }
      throw error;
    }
  }
  
  /**
   * Promote this node to leader
   * @returns {Promise<boolean>} True if promoted to leader
   */
  async promoteToLeader() {
    const leaderFile = path.join(this.options.sharedPath, 'leader.lock');
    
    try {
      const leaderData = {
        nodeId: this.nodeId,
        timestamp: Date.now(),
        expiresAt: Date.now() + this.options.lockTimeout
      };
      
      await fs.writeFile(leaderFile, JSON.stringify(leaderData, null, 2), { flag: 'wx' });
      this.isLeader = true;
      return true;
    } catch (error) {
      if (error.code === 'EEXIST') {
        // Check if current leader is expired
        try {
          const existingLeader = JSON.parse(await fs.readFile(leaderFile, 'utf8'));
          
          if (existingLeader.expiresAt < Date.now()) {
            // Leader expired, take over
            await fs.writeFile(leaderFile, JSON.stringify({
              nodeId: this.nodeId,
              timestamp: Date.now(),
              expiresAt: Date.now() + this.options.lockTimeout
            }, null, 2));
            
            this.isLeader = true;
            return true;
          }
        } catch (readError) {
          // Corrupted leader file, take over
          await fs.writeFile(leaderFile, JSON.stringify({
            nodeId: this.nodeId,
            timestamp: Date.now(),
            expiresAt: Date.now() + this.options.lockTimeout
          }, null, 2));
          
          this.isLeader = true;
          return true;
        }
      }
      
      return false;
    }
  }
  
  /**
   * Release leadership
   * @returns {Promise<void>}
   */
  async releaseLeadership() {
    const leaderFile = path.join(this.options.sharedPath, 'leader.lock');
    
    try {
      await fs.unlink(leaderFile);
    } catch (error) {
      // Ignore if file doesn't exist
    }
    
    this.isLeader = false;
  }
  
  /**
   * Get current leader node
   * @returns {Promise<string|null>} Leader node ID or null
   */
  async getLeader() {
    const leaderFile = path.join(this.options.sharedPath, 'leader.lock');
    
    try {
      const leaderData = JSON.parse(await fs.readFile(leaderFile, 'utf8'));
      
      if (leaderData.expiresAt < Date.now()) {
        return null; // Leader expired
      }
      
      return leaderData.nodeId;
    } catch (error) {
      return null; // No leader or error reading
    }
  }
  
  /**
   * Cleanup and shutdown driver
   * @returns {Promise<void>}
   */
  async shutdown() {
    // Stop cleanup interval
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    
    // Release all locks
    for (const [resource] of this.locks) {
      await this._releaseLock(resource);
    }
    
    // Release leadership if we are leader
    if (this.isLeader) {
      await this.releaseLeadership();
    }
    
    // Unregister from cluster manager
    if (this.options.clusterManager) {
      this.options.clusterManager.unregisterNode(this.nodeId);
    }

  }

  /**
 * Start the shared memory driver and begin processing jobs
 * @returns {Promise<void>}
 */
async start() {
  this.isRunning = true;
  
  // Start automatic job processing for leader node
  if (await this.promoteToLeader()) {
    this._startAutoProcessing();
  } else {
    this._startFollowerPolling();
  }
  
  return Promise.resolve();
}

/**
 * Stop the shared memory driver
 * @returns {Promise<void>}
 */
async stop() {
  this.isRunning = false;
  
  // Stop auto processing
  if (this._processingInterval) {
    clearInterval(this._processingInterval);
    this._processingInterval = null;
  }
  
  // Stop follower polling
  if (this._pollingInterval) {
    clearInterval(this._pollingInterval);
    this._pollingInterval = null;
  }
  
  // Release leadership if we are leader
  if (this.isLeader) {
    await this.releaseLeadership();
  }
  
  return Promise.resolve();
}

/**
 * Start automatic job processing (leader node)
 * @private
 */
_startAutoProcessing() {
  if (this._processingInterval) {
    clearInterval(this._processingInterval);
  }
  
  this._processingInterval = setInterval(async () => {
    if (!this.isRunning || !this.isLeader) return;
    
    // Process all queues
    const queues = await this.getQueues();
    
    for (const queueName of queues) {
      try {
        // Check if queue is paused
        if (await this.isPaused(queueName)) {
          continue;
        }
        
        // Process jobs in this queue
        await this._processQueue(queueName);
      } catch (error) {
        console.error(`Error processing queue ${queueName}:`, error);
      }
    }
    
    // Update last sync time
    this.stats.lastSync = Date.now();
    
  }, 100); // Process every 100ms for better responsiveness
}

/**
 * Process jobs in a specific queue
 * @param {string} queueName - Queue name
 * @private
 */
async _processQueue(queueName) {
  // Load current state
  await this._loadQueueState(queueName);
  
  if (!this.queues.has(queueName)) {
    return;
  }
  
  const queue = this.queues.get(queueName);
  const handler = this.handlers && this.handlers.get(queueName);
  
  // If no handler registered, skip processing
  if (!handler) {
    return;
  }
  
  // Process up to 10 jobs at a time
  for (let i = 0; i < 10 && queue.jobs.length > 0; i++) {
    const job = await this.dequeue(queueName);
    if (job) {
      try {
        const result = await handler(job);
        await this.ack(job.id, result);
      } catch (error) {
        await this.nack(job.id, error);
        console.error(`❌ Job ${job.id} failed on node ${this.nodeId}:`, error.message);
      }
    }
  }
}

/**
 * Start follower polling (non-leader nodes)
 * @private
 */
_startFollowerPolling() {
  if (this._pollingInterval) {
    clearInterval(this._pollingInterval);
  }
  
  this._pollingInterval = setInterval(async () => {
    if (!this.isRunning) return;
    
    // Check if we should become leader
    const currentLeader = await this.getLeader();
    if (!currentLeader) {
      // No leader, try to become leader
      if (await this.promoteToLeader()) {
        this._startAutoProcessing();
        if (this._pollingInterval) {
          clearInterval(this._pollingInterval);
          this._pollingInterval = null;
        }
      }
    } else if (currentLeader !== this.nodeId) {
      // We are follower, just sync state periodically
      await this._syncWithLeader();
    }
    
  }, 5000); // Poll every 5 seconds
}

/**
 * Sync state with leader node
 * @private
 */
async _syncWithLeader() {
  // In a real implementation, this would sync state with the leader
  // For now, just update last sync time
  this.stats.lastSync = Date.now();
}

/**
 * Register a handler for a queue
 * @param {string} queueName - Queue name
 * @param {Function} handler - Handler function
 */
registerHandler(queueName, handler) {
  if (!this.handlers) {
    this.handlers = new Map();
  }
  this.handlers.set(queueName, handler);
}

}
