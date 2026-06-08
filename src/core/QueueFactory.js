/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/core/QueueFactory
 */
import { DRIVERS, getDriver } from '../drivers/index.js';
import ClusterManager from './ClusterManager.js';
import EventEmitter from 'events';

//  OPTIMIZATION: Define Queue as a proper class extending EventEmitter.
// This allows V8 to optimize the object shape and avoids the massive overhead 
// of Object.assign + Object.create + EventEmitter.call.
class Queue extends EventEmitter {
  constructor(name, driverInstance, driverType, config, factory) {
    super();
    this.name = name;
    this.driver = driverInstance;
    this.driverType = driverType;
    this.config = config;
    this.status = 'created';
    this.createdAt = Date.now();
    this._handler = null;
    this._pollingInterval = null;
    this._factory = factory;
  }

  add(job) {
    return this.driver.enqueue(this.name, job);
  }

  process(handler) {
    this._handler = handler;
    if (typeof this.driver.registerHandler === 'function') {
      return this.driver.registerHandler(this.name, handler);
    }
  }

  async start() {
    if (typeof this.driver.start === 'function') {
      return this.driver.start();
    }
    return this._factory._startPolling(this.name, this.driver, this);
  }

  async stop() {
    if (typeof this.driver.stop === 'function') {
      return this.driver.stop();
    }
    this.status = 'stopped';
    if (this._pollingInterval) {
      clearInterval(this._pollingInterval);
      this._pollingInterval = null;
    }
  }

  getStats() {
    return this.driver.getStats 
      ? this.driver.getStats(this.name) 
      : { status: this.status, name: this.name, driver: this.driverType };
  }

  pause() {
    return this.driver.pause ? this.driver.pause(this.name) : Promise.resolve();
  }

  resume() {
    return this.driver.resume ? this.driver.resume(this.name) : Promise.resolve();
  }

  clear() {
    return this.driver.clear ? this.driver.clear(this.name) : Promise.resolve();
  }
}

// 🔥 OPTIMIZATION: Define Worker as a proper class.
class Worker extends EventEmitter {
  constructor(name, queueName, config) {
    super();
    this.name = name;
    this.queue = queueName;
    this.config = config;
    this.status = 'created';
    this.createdAt = Date.now();
  }

  start() {
    this.status = 'running';
    this.emit('workerStarted', { name: this.name, queue: this.queue });
  }

  stop() {
    this.status = 'stopped';
    this.emit('workerStopped', { name: this.name, queue: this.queue });
  }

  getStats() {
    return {
      name: this.name,
      queue: this.queue,
      status: this.status,
      uptime: Date.now() - this.createdAt
    };
  }
}

export default class QueueFactory extends EventEmitter {
  constructor(config = {}) {
    super();
    
    this.config = {
      defaultDriver: 'memory',
      cluster: {
        enabled: false,
        nodes: [],
        electionTimeout: 5000,
        heartbeatInterval: 1000
      },
      middleware: [],
      ...config
    };
    
    this.drivers = new Map();
    this.queues = new Map();
    this.clusterManager = null;
    
    if (this.config.cluster.enabled) {
      this.clusterManager = new ClusterManager(this.config.cluster);
      this.clusterManager.on('leaderChange', (leader) => {
        this.emit('clusterLeaderChange', leader);
      });
    }
    
    this._registerBuiltinDrivers();
  }

  createQueue(options = {}) {
    const queueName = options.name || `queue_${Date.now()}`;
    const driverType = options.driver || this.config.defaultDriver;
    
    if (this.queues.has(queueName)) {
      throw new Error(`Queue "${queueName}" already exists`);
    }
    
    const DriverFactory = this._getDriverFactory(driverType);
    if (!DriverFactory) {
      throw new Error(`Driver "${driverType}" not found. Available: ${Array.from(this.drivers.keys()).join(', ')}`);
    }
    
    return this._instantiateQueue(queueName, driverType, DriverFactory, options);
  }

  async createQueueAsync(options = {}) {
    const queueName = options.name || `queue_${Date.now()}`;
    const driverType = options.driver || this.config.defaultDriver;
    
    if (this.queues.has(queueName)) {
      throw new Error(`Queue "${queueName}" already exists`);
    }
    
    let DriverFactory;
    
    // 🔥 OPTIMIZATION: Check against simple conditions for async drivers
    if (driverType === 'redis' || driverType === 'kafka' || driverType === 'mq' || driverType === 'bullmq') {
      DriverFactory = await getDriver(driverType);
    } else {
      DriverFactory = this._getDriverFactory(driverType);
      if (!DriverFactory) {
        throw new Error(`Driver "${driverType}" not found. Available: ${Array.from(this.drivers.keys()).join(', ')}`);
      }
    }
    
    return this._instantiateQueue(queueName, driverType, DriverFactory, options);
  }

  /**
   * 🔥 OPTIMIZATION: Centralized instantiation logic.
   * Eliminates massive code duplication between sync and async creation.
   */
  _instantiateQueue(queueName, driverType, DriverFactory, options) {
    const driverConfig = {
      ...options.driverConfig,
      clusterManager: this.clusterManager,
      queueName
    };
    
    const driverInstance = new DriverFactory(driverConfig);
    
    // 🔥 OPTIMIZATION: Use the optimized Queue class instead of Object.assign
    const queueInstance = new Queue(queueName, driverInstance, driverType, options, this);
    
    // 🔥 OPTIMIZATION: Zero-allocation middleware application (no spread syntax)
    if (this.config.middleware.length > 0) {
      this._applyMiddleware(queueInstance, this.config.middleware);
    }
    if (options.middleware && options.middleware.length > 0) {
      this._applyMiddleware(queueInstance, options.middleware);
    }
    
    this.queues.set(queueName, queueInstance);
    
    if (this.clusterManager) {
      this.clusterManager.registerQueue(queueName, queueInstance);
    }
    
    this.emit('queueCreated', { name: queueName, queue: queueInstance });
    return queueInstance;
  }

  createWorker(options = {}) {
    const workerName = options.name || `worker_${Date.now()}`;
    const queueName = options.queue;
    
    if (!queueName || !this.queues.has(queueName)) {
      throw new Error(`Queue "${queueName}" not found`);
    }
    
    const queue = this.queues.get(queueName);
    
    // 🔥 OPTIMIZATION: Use the optimized Worker class
    const workerInstance = new Worker(workerName, queueName, options);
    
    if (options.handlers) {
      // 🔥 OPTIMIZATION: for...in is faster than Object.entries().forEach()
      for (const jobType in options.handlers) {
        queue.process(jobType, options.handlers[jobType]);
      }
    }
    
    this.emit('workerCreated', { name: workerName, worker: workerInstance });
    return workerInstance;
  }
  
  registerDriver(name, DriverClass) {
    if (typeof DriverClass !== 'function') {
      throw new Error('Driver must be a constructor function');
    }
    this.drivers.set(name, DriverClass);
    this.emit('driverRegistered', { name });
  }
  
  getQueues() {
    return Object.fromEntries(this.queues);
  }
  
  getQueue(name) {
    const queue = this.queues.get(name);
    if (!queue) throw new Error(`Queue "${name}" not found`);
    return queue;
  }
  
  getClusterManager() {
    return this.clusterManager;
  }
  
  getStats() {
    return {
      queues: this.queues.size,
      drivers: this.drivers.size,
      clusterEnabled: !!this.clusterManager,
      clusterLeader: this.clusterManager ? this.clusterManager.getLeader() : null
    };
  }
  
  async shutdown() {
    this.emit('shutdownStarted');
    
    // 🔥 OPTIMIZATION: Promise.all for concurrent shutdown
    const stopPromises = [];
    for (const [name, queue] of this.queues.entries()) {
      stopPromises.push(
        queue.stop()
          .then(() => this.emit('queueStopped', { name }))
          .catch((error) => this.emit('queueStopError', { name, error }))
      );
    }
    
    await Promise.all(stopPromises);
    
    if (this.clusterManager) {
      await this.clusterManager.stop();
    }
    
    this.queues.clear();
    this.emit('shutdownCompleted');
  }
  
  _getDriverFactory(driverType) {
    return this.drivers.get(driverType) || DRIVERS[driverType];
  }
  
  _registerBuiltinDrivers() {
    // 🔥 OPTIMIZATION: for...in avoids intermediate [key, value] array allocations
    for (const name in DRIVERS) {
      if (DRIVERS[name] !== null) {
        this.drivers.set(name, DRIVERS[name]);
      }
    }
  }
  
  _applyMiddleware(queueInstance, middlewareConfig) {
    const useFn = queueInstance.driver.use;
    if (typeof useFn !== 'function') return;

    // 🔥 OPTIMIZATION: Standard for-loop is the fastest iteration method in V8
    for (let i = 0; i < middlewareConfig.length; i++) {
      const middleware = middlewareConfig[i];
      if (typeof middleware === 'function') {
        useFn.call(queueInstance.driver, middleware);
      } else if (middleware && typeof middleware.execute === 'function') {
        useFn.call(queueInstance.driver, middleware.execute.bind(middleware));
      }
    }
  }
  
  _startPolling(queueName, driverInstance, queueInstance) {
    queueInstance.status = 'running';
    
    queueInstance._pollingInterval = setInterval(async () => {
      if (!queueInstance._handler) return;
      
      try {
        const job = await driverInstance.dequeue(queueName);
        if (job) {
          try {
            const result = await queueInstance._handler(job);
            if (typeof driverInstance.ack === 'function') {
              await driverInstance.ack(job.id, result);
            }
          } catch (error) {
            console.error(`Polling: Job ${job.id} failed:`, error.message);
            if (typeof driverInstance.nack === 'function') {
              await driverInstance.nack(job.id, error);
            }
          }
        }
      } catch (error) {
        console.error(`Error polling queue ${queueName}:`, error);
      }
    }, 1000);
    
    return Promise.resolve();
  }
}
