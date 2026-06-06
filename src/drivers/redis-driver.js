/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/drivers/redis-driver
 */

import Redis from 'ioredis';
import EventEmitter from 'events';

// ================= Lua Scripts =================

const LUA_DEQUEUE = `
  local pendingKey    = KEYS[1]
  local lockPrefix    = KEYS[2]
  local jobPrefix     = KEYS[3]
  local processingKey = KEYS[4]
  local lockTtl       = tonumber(ARGV[1])
  local now           = ARGV[2]
  local nodeId        = ARGV[3]

  local jobs = redis.call('ZRANGE', pendingKey, 0, 0)
  if #jobs == 0 then return nil end

  local jobId = jobs[1]
  local lockKey = lockPrefix .. jobId
  local locked = redis.call('SET', lockKey, 'processing', 'NX', 'PX', lockTtl)
  if not locked then return nil end

  redis.call('ZREM', pendingKey, jobId)
  redis.call('SADD', processingKey, jobId)

  local jobKey = jobPrefix .. jobId
  redis.call('HSET', jobKey, 'status', 'processing')
  redis.call('HSET', jobKey, 'startedAt', now)
  redis.call('HSET', jobKey, 'processingNode', nodeId)
  redis.call('HINCRBY', jobKey, 'attempts', 1)

  return jobId
`;

const LUA_PROMOTE_DELAYED = `
  local delayedKey  = KEYS[1]
  local pendingKey  = KEYS[2]
  local jobPrefix   = KEYS[3]
  local now         = tonumber(ARGV[1])
  local limit       = tonumber(ARGV[2])

  local jobs = redis.call('ZRANGEBYSCORE', delayedKey, 0, now, 'LIMIT', 0, limit)
  if #jobs == 0 then return 0 end

  for i, jobId in ipairs(jobs) do
    redis.call('ZREM', delayedKey, jobId)
    local priority = redis.call('HGET', jobPrefix .. jobId, 'priority') or '0'
    redis.call('ZADD', pendingKey, priority, jobId)
    redis.call('HSET', jobPrefix .. jobId, 'status', 'pending')
  end
  return #jobs
`;

// 🔥 Rate Limiter: Atomic token bucket / fixed window check
const LUA_RATE_LIMIT = `
  local limiterKey = KEYS[1]
  local max        = tonumber(ARGV[1])
  local duration   = tonumber(ARGV[2])

  local current = redis.call('GET', limiterKey)
  if current and tonumber(current) >= max then
    local ttl = redis.call('PTTL', limiterKey)
    return ttl > 0 and ttl or duration
  end

  redis.call('INCR', limiterKey)
  if redis.call('PTTL', limiterKey) < 0 then
    redis.call('PEXPIRE', limiterKey, duration)
  end
  return 0
`;

export default class RedisDriver extends EventEmitter {
  constructor(config = {}) {
    super();
    this.config = {
      url: config.url || 'redis://localhost:6379',
      password: config.password,
      db: config.db || 0,
      keyPrefix: config.keyPrefix || 'queue:',
      maxRetriesPerRequest: config.maxRetriesPerRequest || 5,
      connectTimeout: config.connectTimeout || 15000,
      enableReadyCheck: config.enableReadyCheck !== false,
      pollInterval: config.pollInterval || 100, 
      concurrency: config.concurrency || 20,    
      limiter: config.limiter || null, // 🔥 NEW: Global rate limiter { max: 10, duration: 1000 }
      retryStrategy: (times) => Math.min(times * 50, 2000),
      reconnectOnError: () => true,
      enableOfflineQueue: true,
      ...config
    };

    this.redis = null;
    this.queues = new Map();
    this.handlers = new Map();
    this.isRunning = false;
    this.workerLoops = new Map(); 
    
    // 🔥 NEW: Zero-RTT local state for Pause/Resume
    this.pausedQueues = new Set(); 

    this.stats = { enqueued: 0, dequeued: 0, completed: 0, failed: 0, redisConnected: false };
  }

  async init() {
    try {
      const url = new URL(this.config.url);
      if (this.redis) { try { await this.redis.quit(); } catch (_) {} this.redis = null; }

      this.redis = new Redis({
        host: url.hostname, port: parseInt(url.port) || 6379,
        password: url.password || this.config.password, db: this.config.db,
        keyPrefix: this.config.keyPrefix, maxRetriesPerRequest: this.config.maxRetriesPerRequest,
        connectTimeout: this.config.connectTimeout, enableReadyCheck: this.config.enableReadyCheck,
        retryStrategy: this.config.retryStrategy, reconnectOnError: this.config.reconnectOnError,
        enableOfflineQueue: this.config.enableOfflineQueue
      });

      // 🔥 Cache Lua scripts via EVALSHA
      this.redis.defineCommand('dqJob', { numberOfKeys: 4, lua: LUA_DEQUEUE });
      this.redis.defineCommand('promoteDelayed', { numberOfKeys: 3, lua: LUA_PROMOTE_DELAYED });
      this.redis.defineCommand('checkRateLimit', { numberOfKeys: 1, lua: LUA_RATE_LIMIT });

      this.redis.on('connect', () => { this.stats.redisConnected = true; });
      this.redis.on('error', (err) => { console.error('❌ Redis error:', err.message); this.stats.redisConnected = false; });
      this.redis.on('ready', () => { this.stats.redisConnected = true; });
      this.redis.on('end', () => { this.stats.redisConnected = false; });

      await this.redis.ping();
    } catch (error) {
      console.error('❌ Failed to connect to Redis:', error.message);
      throw error;
    }
  }

  async start() {
    if (!this.redis) await this.init();
    this.isRunning = true;
    this._startWorkerLoops();
  }

  async stop() {
    this.isRunning = false;
    await Promise.all(Array.from(this.workerLoops.values()));
    this.workerLoops.clear();
    if (this.redis) { await this.redis.quit(); this.redis = null; }
  }

  _startWorkerLoops() {
    for (const [queueName] of this.queues) {
      if (this.workerLoops.has(queueName)) continue;
      this.workerLoops.set(queueName, this._runWorkerLoop(queueName));
    }
  }

  async _runWorkerLoop(queueName) {
    const handler = this.handlers.get(queueName);
    const queueConfig = this.queues.get(queueName)?.config || {};
    const concurrency = queueConfig.concurrency || this.config.concurrency;
    const limiter = queueConfig.limiter || this.config.limiter;
    const activeJobs = new Set();

    while (this.isRunning && this.stats.redisConnected) {
      // 🔥 FEATURE: Pause/Resume (Zero-RTT Local Check)
      if (this.pausedQueues.has(queueName)) {
        await new Promise(r => setTimeout(r, this.config.pollInterval));
        continue;
      }

      // 🔥 Promote delayed jobs
      await this.redis.promoteDelayed(
        `${queueName}:delayed`, `${queueName}:pending`, 'job:',
        Date.now().toString(), '50'
      ).catch(() => {});

      while (activeJobs.size < concurrency && this.isRunning && !this.pausedQueues.has(queueName)) {
        // 🔥 FEATURE: Rate Limiting (Atomic Lua Check)
        if (limiter && limiter.max && limiter.duration) {
          const ttl = await this.redis.checkRateLimit(
            `${queueName}:limiter`, limiter.max, limiter.duration
          ).catch(() => 0);
          
          if (ttl > 0) {
            // Rate limited! Sleep for the exact remaining TTL of the window
            await new Promise(r => setTimeout(r, ttl));
            break; // Break inner loop to re-evaluate pause/status
          }
        }

        const job = await this.dequeue(queueName);
        if (!job) break; 

        const jobPromise = this._processJob(queueName, job, handler)
          .catch(err => console.error(`❌ Job ${job.id} failed:`, err.message))
          .finally(() => activeJobs.delete(jobPromise));
        
        activeJobs.add(jobPromise);
      }

      if (activeJobs.size === 0) {
        await new Promise(r => setTimeout(r, this.config.pollInterval));
      } else {
        await Promise.race(activeJobs);
      }
    }
  }

  async _processJob(queueName, job, handler) {
    const queueConfig = this.queues.get(queueName);
    try {
      const result = await handler(job);
      await this.ack(job.id, result, queueName);
      if (queueConfig?.onJobCompleted) queueConfig.onJobCompleted(job, result);
      this.emit('completed', job, result);
      return result;
    } catch (error) {
      await this.nack(job.id, error, queueName);
      if (queueConfig?.onJobFailed) queueConfig.onJobFailed(job, error);
      this.emit('failed', job, error);
      throw error;
    }
  }

  async enqueue(queueName, job) {
    if (!this.redis) await this.init();
    const jobId = job.id || `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    const priority = job.priority || 0; 
    const delay = job.delay || 0;
    const executeAt = job.executeAt || (Date.now() + delay);
    
    const jobData = { 
      ...job, id: jobId, queue: queueName, status: 'pending', 
      enqueuedAt: Date.now(), attempts: 0, priority, executeAt, progress: 0,
      backoffType: job.backoff?.type || 'exponential',
      backoffDelay: job.backoff?.delay || 1000
    };

    if (job.children && job.children.length > 0) {
      jobData.pendingChildren = job.children.length;
      for (const child of job.children) {
        child.parent = { id: jobId, queue: queueName };
        await this.enqueue(child.queue || queueName, child);
      }
    }

    try {
      const multi = this.redis.multi();
      for (const [key, value] of Object.entries(jobData)) {
        if (value === undefined || value === null || key === 'children') continue;
        multi.hset(`job:${jobId}`, key, typeof value === 'object' ? JSON.stringify(value) : String(value));
      }

      if (delay > 0 || executeAt > Date.now()) {
        multi.zadd(`${queueName}:delayed`, executeAt, jobId);
        multi.hset(`job:${jobId}`, 'status', 'delayed');
      } else {
        multi.zadd(`${queueName}:pending`, priority, jobId);
      }

      if (jobData.pendingChildren > 0) {
        multi.zadd(`${queueName}:waiting-children`, priority, jobId);
        multi.hset(`job:${jobId}`, 'status', 'waiting-children');
      }

      multi.hincrby(`${queueName}:stats`, 'enqueued', 1);
      if (job.ttl) multi.expire(`job:${jobId}`, Math.ceil(job.ttl / 1000));
      
      await multi.exec();
      this.stats.enqueued++;
      return jobId;
    } catch (error) {
      console.error(`❌ Enqueue failed:`, error.message);
      throw error;
    }
  }

  async dequeue(queueName) {
    if (!this.redis || !this.stats.redisConnected) return null;
    try {
      const jobId = await this.redis.dqJob(
        `${queueName}:pending`, 'lock:', 'job:', `${queueName}:processing`,
        30000, Date.now().toString(), process.pid.toString()
      );
      if (!jobId) return null;

      const jobData = await this.redis.hgetall(`job:${jobId}`);
      if (!jobData || Object.keys(jobData).length === 0) {
        await this.redis.del(`lock:${jobId}`);
        return null;
      }

      const parsedJobData = {};
      for (const [key, value] of Object.entries(jobData)) {
        try { parsedJobData[key] = JSON.parse(value); } catch (_) { parsedJobData[key] = value; }
      }

      this.stats.dequeued++;
      
      // 🔥 FEATURE: Inject Progress Reporting Method
      const jobObj = { ...parsedJobData, id: jobId, queue: queueName };
      jobObj.updateProgress = async (progress) => {
        jobObj.progress = progress;
        if (this.redis) {
          await this.redis.hset(`job:${jobId}`, 'progress', JSON.stringify(progress));
        }
        const queueConfig = this.queues.get(queueName);
        if (queueConfig?.onJobProgress) queueConfig.onJobProgress(jobObj, progress);
        this.emit('progress', jobObj, progress);
      };
      
      return jobObj;
    } catch (error) {
      return null;
    }
  }

  async ack(jobId, result, queueName) {
    if (!this.redis) return;
    try {
      const jobData = await this.redis.hgetall(`job:${jobId}`);
      const qName = queueName || jobData.queue;
      if (!qName) return;

      const multi = this.redis.multi();
      multi.hset(`job:${jobId}`, 'status', 'completed');
      multi.hset(`job:${jobId}`, 'completedAt', Date.now().toString());
      multi.hset(`job:${jobId}`, 'result', JSON.stringify(result));
      multi.hset(`job:${jobId}`, 'progress', '100'); // 🔥 Auto-set 100% on completion
      multi.srem(`${qName}:processing`, jobId);
      multi.zadd(`${qName}:completed`, Date.now(), jobId);
      multi.del(`lock:${jobId}`);
      multi.hincrby(`${qName}:stats`, 'completed', 1);
      await multi.exec();
      this.stats.completed++;

      if (jobData.parentId && jobData.parentQueue) {
        const parentData = await this.redis.hgetall(`job:${jobData.parentId}`);
        if (parentData) {
          const pendingChildren = parseInt(parentData.pendingChildren || 1, 10) - 1;
          const pMulti = this.redis.multi();
          pMulti.hset(`job:${jobData.parentId}`, 'pendingChildren', pendingChildren);
          if (pendingChildren <= 0) {
            pMulti.zrem(`${parentData.queue}:waiting-children`, jobData.parentId);
            pMulti.zadd(`${parentData.queue}:pending`, parentData.priority || 0, jobData.parentId);
            pMulti.hset(`job:${jobData.parentId}`, 'status', 'pending');
          }
          await pMulti.exec();
        }
      }

      setTimeout(() => {
        this.redis.del(`job:${jobId}`).catch(() => {});
        this.redis.zrem(`${qName}:completed`, jobId).catch(() => {});
      }, 3600000);
    } catch (error) {
      console.error(`❌ ACK failed:`, error.message);
    }
  }

  async nack(jobId, error, queueName) {
    if (!this.redis) return;
    try {
      const jobData = await this.redis.hgetall(`job:${jobId}`);
      const qName = queueName || jobData.queue;
      const maxRetries = parseInt(jobData.maxRetries || 3, 10);
      const attempts = parseInt(jobData.attempts || 0, 10);
      if (!qName) return;

      const multi = this.redis.multi();
      multi.srem(`${qName}:processing`, jobId);
      multi.del(`lock:${jobId}`);
      multi.hincrby(`${qName}:stats`, 'failed', 1);

      if (attempts >= maxRetries) {
        multi.hset(`job:${jobId}`, 'status', 'dead');
        multi.hset(`job:${jobId}`, 'error', error.message);
        multi.zadd(`${qName}:dlq`, Date.now(), jobId); 
      } else {
        const backoffType = jobData.backoffType || 'exponential';
        const backoffDelay = parseInt(jobData.backoffDelay || 1000, 10);
        let retryDelay = backoffDelay;
        
        if (backoffType === 'exponential') {
          retryDelay = backoffDelay * Math.pow(2, attempts - 1);
        } else if (typeof backoffType === 'function') {
          retryDelay = backoffType(attempts, error); 
        }

        const retryAt = Date.now() + retryDelay;
        multi.hset(`job:${jobId}`, 'status', 'delayed');
        multi.hset(`job:${jobId}`, 'error', error.message);
        multi.zadd(`${qName}:delayed`, retryAt, jobId); 
      }

      await multi.exec();
      this.stats.failed++;
    } catch (err) {
      console.error(`❌ NACK failed:`, err.message);
    }
  }

  // ================= 🎛️ Queue Control API =================

  /**
   * 🔥 FEATURE: Pause Queue (Zero-RTT local cache + Redis sync)
   */
  async pause(queueName) {
    this.pausedQueues.add(queueName);
    if (this.redis) await this.redis.set(`${queueName}:paused`, '1');
    this.emit('paused', queueName);
  }

  /**
   * 🔥 FEATURE: Resume Queue
   */
  async resume(queueName) {
    this.pausedQueues.delete(queueName);
    if (this.redis) await this.redis.del(`${queueName}:paused`);
    this.emit('resumed', queueName);
  }

  async isPaused(queueName) {
    return this.pausedQueues.has(queueName);
  }

  registerHandler(queueName, handler, options = {}) {
    this.handlers.set(queueName, handler);
    this.queues.set(queueName, { name: queueName, config: options, handler, ...options });
    if (this.isRunning && !this.workerLoops.has(queueName)) {
      this.workerLoops.set(queueName, this._runWorkerLoop(queueName));
    }
  }

  // ================= 📊 Monitoring & UI API =================

  async getJobs(queueName, status = 'pending', start = 0, end = 20) {
    if (!this.redis) return [];
    const keyMap = {
      pending: `${queueName}:pending`, delayed: `${queueName}:delayed`,
      completed: `${queueName}:completed`, dead: `${queueName}:dlq`,
      processing: `${queueName}:processing`
    };
    
    const key = keyMap[status];
    if (!key) return [];

    let jobIds = [];
    if (status === 'processing') {
      jobIds = await this.redis.smembers(key);
    } else {
      jobIds = await this.redis.zrange(key, start, end);
    }

    const pipeline = this.redis.pipeline();
    jobIds.forEach(id => pipeline.hgetall(`job:${id}`));
    const results = await pipeline.exec();

    return results.map(([err, data], i) => {
      if (err || !data) return null;
      const parsed = {};
      for (const [k, v] of Object.entries(data)) {
        try { parsed[k] = JSON.parse(v); } catch (_) { parsed[k] = v; }
      }
      return { id: jobIds[i], ...parsed };
    }).filter(Boolean);
  }

  async retryDeadLetterJob(queueName, jobId) {
    const multi = this.redis.multi();
    multi.zrem(`${queueName}:dlq`, jobId);
    multi.hset(`job:${jobId}`, 'status', 'pending');
    multi.hset(`job:${jobId}`, 'attempts', 0);
    multi.zadd(`${queueName}:pending`, 0, jobId);
    await multi.exec();
  }

  async getStats(queueName) {
    if (!this.redis) throw new Error('Redis not initialized');
    const [pending, delayed, processing, completed, dead, stats, isPaused] = await Promise.all([
      this.redis.zcard(`${queueName}:pending`),
      this.redis.zcard(`${queueName}:delayed`),
      this.redis.scard(`${queueName}:processing`),
      this.redis.zcard(`${queueName}:completed`),
      this.redis.zcard(`${queueName}:dlq`),
      this.redis.hgetall(`${queueName}:stats`),
      this.redis.exists(`${queueName}:paused`)
    ]);
    return {
      queue: queueName, 
      pending: +pending || 0, delayed: +delayed || 0, processing: +processing || 0,
      completed: +completed || 0, dead: +dead || 0,
      enqueued: +(stats?.enqueued) || 0, dequeued: +(stats?.dequeued) || 0, failed: +(stats?.failed) || 0,
      isPaused: !!isPaused,
      redisConnected: this.stats.redisConnected
    };
  }

  async clear(queueName) {
    if (!this.redis) return;
    const keys = [
      `${queueName}:pending`, `${queueName}:delayed`, `${queueName}:processing`, 
      `${queueName}:completed`, `${queueName}:dlq`, `${queueName}:waiting-children`, 
      `${queueName}:stats`, `${queueName}:paused`, `${queueName}:limiter`
    ];
    if (keys.length > 0) await this.redis.del(...keys);
  }
}
