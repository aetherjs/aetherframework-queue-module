/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/drivers/file-driver
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

export default class FileDriver {
  /**
   * Create a new FileDriver instance
   * @param {Object} config - Driver configuration
   * @param {string} config.dataDir - Directory to store queue data
   * @param {string} config.queueName - Name of the queue
   */
  constructor(config) {
    this.config = config;
    this.dataDir = config.dataDir || './data/queues';
    this.queueName = config.queueName;
    this.filePath = path.join(this.dataDir, `${this.queueName}.jsonl`);
    this.lockFilePath = path.join(this.dataDir, `${this.queueName}.lock`);
    
    // Ensure directory exists
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
    
    // Initialize file if not exists
    if (!fs.existsSync(this.filePath)) {
      fs.writeFileSync(this.filePath, '');
    }
  }

  /**
   * Push a job to the queue
   * @param {Object} job - Job data
   * @returns {Promise<string>} Job ID
   */
  async push(job) {
    const jobId = job.id || `job_${crypto.randomBytes(8).toString('hex')}`;
    const entry = {
      id: jobId,
      data: job.data,
      timestamp: Date.now(),
      status: 'pending',
      ...job
    };
    
    const line = JSON.stringify(entry) + '\n';
    
    // Simple file locking mechanism
    await this._acquireLock();
    try {
      fs.appendFileSync(this.filePath, line);
    } finally {
      this._releaseLock();
    }
    
    return jobId;
  }

  /**
   * Pop a job from the queue
   * @returns {Promise<Object|null>} Job data or null if empty
   */
  async pop() {
    await this._acquireLock();
    try {
      const content = fs.readFileSync(this.filePath, 'utf8');
      if (!content.trim()) return null;
      
      const lines = content.split('\n').filter(line => line.trim());
      if (lines.length === 0) return null;
      
      // Get first pending job
      let jobIndex = -1;
      let job = null;
      
      for (let i = 0; i < lines.length; i++) {
        try {
          const parsed = JSON.parse(lines[i]);
          if (parsed.status === 'pending') {
            job = parsed;
            jobIndex = i;
            break;
          }
        } catch (e) {
          // Skip invalid lines
          continue;
        }
      }
      
      if (!job) return null;
      
      // Mark as processed by removing the line
      lines.splice(jobIndex, 1);
      fs.writeFileSync(this.filePath, lines.join('\n') + (lines.length > 0 ? '\n' : ''));
      
      return job;
    } finally {
      this._releaseLock();
    }
  }

  /**
   * Get queue length
   * @returns {Promise<number>}
   */
  async length() {
    const content = fs.readFileSync(this.filePath, 'utf8');
    if (!content.trim()) return 0;
    
    const lines = content.split('\n').filter(line => line.trim());
    let count = 0;
    
    for (const line of lines) {
      try {
        const parsed = JSON.parse(line);
        if (parsed.status === 'pending') {
          count++;
        }
      } catch (e) {
        // Skip invalid lines
      }
    }
    
    return count;
  }

  /**
   * Clear the queue
   * @returns {Promise<void>}
   */
  async clear() {
    await this._acquireLock();
    try {
      fs.writeFileSync(this.filePath, '');
    } finally {
      this._releaseLock();
    }
  }

  /**
   * Acquire file lock
   * @private
   */
  async _acquireLock() {
    // Simple spin-lock implementation
    const maxRetries = 100;
    let retries = 0;
    
    while (retries < maxRetries) {
      try {
        fs.writeFileSync(this.lockFilePath, process.pid.toString(), { flag: 'wx' });
        return;
      } catch (err) {
        if (err.code === 'EEXIST') {
          // Check if lock is stale
          try {
            const lockPid = parseInt(fs.readFileSync(this.lockFilePath, 'utf8'));
            if (!this._isProcessRunning(lockPid)) {
              // Stale lock, remove it
              fs.unlinkSync(this.lockFilePath);
              continue;
            }
          } catch (e) {
            // Lock file corrupted, remove it
            fs.unlinkSync(this.lockFilePath);
            continue;
          }
          
          // Wait and retry
          await new Promise(resolve => setTimeout(resolve, 10));
          retries++;
        } else {
          throw err;
        }
      }
    }
    
    throw new Error('Could not acquire file lock after maximum retries');
  }

  /**
   * Release file lock
   * @private
   */
  _releaseLock() {
    try {
      if (fs.existsSync(this.lockFilePath)) {
        fs.unlinkSync(this.lockFilePath);
      }
    } catch (err) {
      // Ignore errors during unlock
    }
  }

  /**
   * Check if a process is running
   * @param {number} pid - Process ID
   * @returns {boolean}
   * @private
   */
  _isProcessRunning(pid) {
    try {
      process.kill(pid, 0);
      return true;
    } catch (err) {
      return false;
    }
  }
}
