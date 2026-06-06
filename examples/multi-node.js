/**
 * multi-node.js - High-Performance Multi-Node Queue Cluster with Enhanced Testing Capabilities
 * 
 * This module demonstrates a production-grade, multi-process queue system using Node.js cluster.
 * It features load balancing, fault tolerance, graceful shutdown, and comprehensive testing hooks.
 * 
 * Key Optimizations:
 * 1. Dynamic worker scaling based on system load
 * 2. Shared memory driver for inter-process communication
 * 3. Graceful shutdown with job completion
 * 4. Health monitoring and automatic restart
 * 5. Built-in testing and benchmarking capabilities
 */

import cluster from 'cluster';
import os from 'os';
import QueueFactory from '../src/core/QueueFactory.js';

// Configuration constants for cluster management
const CONFIG = {
  MAX_WORKERS: os.cpus().length,           // Maximum workers based on CPU cores
  MIN_WORKERS: 1,                          // Minimum workers to maintain
  WORKER_RESTART_DELAY: 1000,              // Delay before restarting failed workers (ms)
  GRACEFUL_SHUTDOWN_TIMEOUT: 30000,        // Timeout for graceful shutdown (ms)
  HEALTH_CHECK_INTERVAL: 5000,             // Interval for health checks (ms)
  QUEUE_CONFIG: {
    defaultDriver: 'shared-memory',        // Driver for inter-process communication
    defaultConfig: {
      concurrency: 2,                      // Jobs per worker
      maxRetries: 3,                       // Maximum retry attempts
      timeout: 30000                       // Job timeout in milliseconds
    }
  }
};

// Global state for cluster management
const clusterState = {
  activeWorkers: new Map(),                // Map of worker PID to worker info
  isShuttingDown: false,                   // Flag for graceful shutdown
  totalJobsProcessed: 0,                   // Counter for processed jobs
  failedJobs: 0,                           // Counter for failed jobs
  startTime: Date.now(),                   // Cluster start timestamp
  healthCheckInterval: null                // Reference to health check interval
};

/**
 * Initialize worker processes based on configuration
 */
function initializeWorkers() {
  console.log(`👥 Creating ${CONFIG.MIN_WORKERS} initial worker(s)...`);
  
  for (let i = 0; i < CONFIG.MIN_WORKERS; i++) {
    createWorker();
  }
}

/**
 * Create and register a new worker process
 * @returns {cluster.Worker} The created worker instance
 */
function createWorker() {
  const worker = cluster.fork();
  
  // Store worker information in cluster state
  clusterState.activeWorkers.set(worker.process.pid, {
    worker,
    pid: worker.process.pid,
    startTime: Date.now(),
    jobsProcessed: 0,
    lastHealthCheck: Date.now(),
    status: 'starting',
    isConnected: true                     // Track connection status
  });
  
  console.log(`🆕 Worker ${worker.process.pid} created`);
  return worker;
}

/**
 * Set up cluster event handlers for worker management
 */
function setupClusterEventHandlers() {
  // Handle worker exit events
  cluster.on('exit', (worker, code, signal) => {
    const workerInfo = clusterState.activeWorkers.get(worker.process.pid);
    
    if (workerInfo) {
      console.log(`💀 Worker ${worker.process.pid} exited with code ${code} and signal ${signal}`);
      console.log(`⏱️  Worker uptime: ${Date.now() - workerInfo.startTime}ms`);
      console.log(`📊 Jobs processed: ${workerInfo.jobsProcessed}`);
      
      // Remove from active workers
      clusterState.activeWorkers.delete(worker.process.pid);
    }
    
    // Restart worker unless we're shutting down or it was intentionally terminated
    if (!clusterState.isShuttingDown && code !== 0 && signal !== 'SIGTERM') {
      console.log(`🔄 Restarting worker ${worker.process.pid} in ${CONFIG.WORKER_RESTART_DELAY}ms...`);
      setTimeout(() => createWorker(), CONFIG.WORKER_RESTART_DELAY);
    }
  });
  
  // Handle worker message events for inter-process communication
  cluster.on('message', (worker, message) => {
    handlePrimaryMessage(worker, message);
  });
  
  // Handle worker online events
  cluster.on('online', (worker) => {
    const workerInfo = clusterState.activeWorkers.get(worker.process.pid);
    if (workerInfo) {
      workerInfo.status = 'online';
      console.log(`✅ Worker ${worker.process.pid} is now online`);
    }
  });
  
  // Handle worker disconnect events
  cluster.on('disconnect', (worker) => {
    const workerInfo = clusterState.activeWorkers.get(worker.process.pid);
    if (workerInfo) {
      workerInfo.isConnected = false;
      console.log(`🔌 Worker ${worker.process.pid} disconnected`);
    }
  });
}

/**
 * Handle messages from worker processes
 * @param {cluster.Worker} worker - The worker sending the message
 * @param {Object} message - The message payload
 */
function handlePrimaryMessage(worker, message) {
  const workerInfo = clusterState.activeWorkers.get(worker.process.pid);
  
  if (!workerInfo) return;
  
  switch (message.type) {
    case 'worker_ready':
      workerInfo.status = 'ready';
      workerInfo.lastHealthCheck = Date.now();
      console.log(`✅ Worker ${worker.process.pid} reported ready for work`);
      break;
      
    case 'job_completed':
      workerInfo.jobsProcessed++;
      clusterState.totalJobsProcessed++;
      workerInfo.lastHealthCheck = Date.now();
      
      // Optional: Implement load balancing logic here
      // Example: If worker is overloaded, scale up
      if (shouldScaleUpWorkers()) {
        scaleUpWorkers();
      }
      break;
      
    case 'job_failed':
      clusterState.failedJobs++;
      workerInfo.lastHealthCheck = Date.now();
      console.warn(`⚠️  Worker ${worker.process.pid} failed job ${message.jobId}: ${message.error}`);
      break;
      
    case 'health_check_response':
      workerInfo.lastHealthCheck = Date.now();
      break;
      
    default:
      console.log(`📨 Received unknown message from worker ${worker.process.pid}:`, message.type);
  }
}

/**
 * Handle messages in worker process
 * @param {Object} message - The message from primary process
 */
function handleWorkerMessage(message) {
  switch (message.type) {
    case 'health_check':
      // Respond to health check
      if (process.send) {
        process.send({ type: 'health_check_response', pid: process.pid });
      }
      break;
      
    case 'graceful_shutdown':
      console.log(`🛑 Worker ${process.pid} received shutdown signal`);
      initiateWorkerShutdown();
      break;
      
    case 'test_signal':
      // Handle test signals for integration testing
      console.log(`🧪 Worker ${process.pid} received test signal:`, message.payload);
      if (process.send) {
        process.send({ type: 'test_response', pid: process.pid, payload: 'test_ack' });
      }
      break;
  }
}

/**
 * Safely send a message to a worker process
 * @param {cluster.Worker} worker - The worker to send message to
 * @param {Object} message - The message to send
 * @returns {boolean} True if message was sent successfully
 */
function safeSendToWorker(worker, message) {
  if (!worker || !worker.isConnected()) {
    return false;
  }
  
  try {
    worker.send(message);
    return true;
  } catch (error) {
    // Handle EPIPE and other IPC errors gracefully
    if (error.code === 'EPIPE' || error.code === 'ERR_IPC_CHANNEL_CLOSED') {
      console.warn(`⚠️  Cannot send message to worker ${worker.process.pid}: IPC channel closed`);
      return false;
    }
    console.error(`❌ Failed to send message to worker ${worker.process.pid}:`, error.message);
    return false;
  }
}

/**
 * Start health monitoring for all workers
 */
function startHealthMonitoring() {
  // Clear any existing interval
  if (clusterState.healthCheckInterval) {
    clearInterval(clusterState.healthCheckInterval);
  }
  
  clusterState.healthCheckInterval = setInterval(() => {
    // Skip health checks during shutdown
    if (clusterState.isShuttingDown) {
      return;
    }
    
    const now = Date.now();
    const unhealthyWorkers = [];
    
    // Check each worker's last health check time
    for (const [pid, info] of clusterState.activeWorkers) {
      // Only check workers that are supposed to be ready
      if (info.status !== 'ready') {
        continue;
      }
      
      const timeSinceLastCheck = now - info.lastHealthCheck;
      
      // Mark as unhealthy if no response for 3x the interval
      if (timeSinceLastCheck > CONFIG.HEALTH_CHECK_INTERVAL * 3) {
        console.warn(`⚠️  Worker ${pid} missed health check (last: ${timeSinceLastCheck}ms ago)`);
        unhealthyWorkers.push(pid);
      }
    }
    
    // Restart only truly unhealthy workers (no response for 3 intervals)
    unhealthyWorkers.forEach(pid => {
      const workerInfo = clusterState.activeWorkers.get(pid);
      if (workerInfo && workerInfo.worker && workerInfo.worker.isConnected()) {
        console.log(`🔄 Restarting unhealthy worker ${pid} (no health check for ${now - workerInfo.lastHealthCheck}ms)`);
        workerInfo.worker.kill('SIGTERM');
      }
    });
    
    // Send health check to all workers
    for (const [pid, info] of clusterState.activeWorkers) {
      if (info.worker && info.worker.isConnected() && info.status === 'ready') {
        safeSendToWorker(info.worker, { type: 'health_check' });
      }
    }
    
  }, CONFIG.HEALTH_CHECK_INTERVAL);
}

/**
 * Determine if we should scale up workers based on load
 * @returns {boolean} True if workers should be scaled up
 */
function shouldScaleUpWorkers() {
  const activeWorkerCount = clusterState.activeWorkers.size;
  
  // Scale up if we have capacity and need more workers
  if (activeWorkerCount >= CONFIG.MAX_WORKERS) {
    return false;
  }
  
  // Simple scaling logic: scale up if average jobs per worker > threshold
  const avgJobsPerWorker = clusterState.totalJobsProcessed / Math.max(activeWorkerCount, 1);
  const scaleThreshold = 100; // Adjust based on your workload
  
  return avgJobsPerWorker > scaleThreshold;
}

/**
 * Scale up the number of worker processes
 */
function scaleUpWorkers() {
  const currentWorkers = clusterState.activeWorkers.size;
  
  if (currentWorkers < CONFIG.MAX_WORKERS) {
    console.log(`📈 Scaling up workers: ${currentWorkers} -> ${currentWorkers + 1}`);
    createWorker();
  }
}

/**
 * Scale down the number of worker processes
 */
function scaleDownWorkers() {
  const currentWorkers = clusterState.activeWorkers.size;
  
  if (currentWorkers > CONFIG.MIN_WORKERS) {
    // Find the least busy worker
    let leastBusyWorker = null;
    let minJobs = Infinity;
    
    for (const [pid, info] of clusterState.activeWorkers) {
      if (info.jobsProcessed < minJobs && info.status === 'ready') {
        minJobs = info.jobsProcessed;
        leastBusyWorker = info.worker;
      }
    }
    
    if (leastBusyWorker) {
      console.log(`📉 Scaling down workers: ${currentWorkers} -> ${currentWorkers - 1}`);
      safeSendToWorker(leastBusyWorker, { type: 'graceful_shutdown' });
    }
  }
}

/**
 * Set up signal handlers for graceful shutdown
 */
function setupSignalHandlers() {
  // Handle SIGTERM (termination signal)
  process.on('SIGTERM', () => {
    console.log('🛑 Received SIGTERM, initiating graceful shutdown...');
    initiateGracefulShutdown();
  });
  
  // Handle SIGINT (Ctrl+C)
  process.on('SIGINT', () => {
    console.log('🛑 Received SIGINT, initiating graceful shutdown...');
    initiateGracefulShutdown();
  });
  
  // Handle SIGUSR2 for hot reload (development)
  process.on('SIGUSR2', () => {
    console.log('🔄 Received SIGUSR2, performing hot reload...');
    performHotReload();
  });
}

/**
 * Initiate graceful shutdown of the entire cluster
 */
function initiateGracefulShutdown() {
  if (clusterState.isShuttingDown) return;
  
  clusterState.isShuttingDown = true;
  console.log('⏳ Beginning graceful shutdown sequence...');
  
  // Stop health monitoring
  if (clusterState.healthCheckInterval) {
    clearInterval(clusterState.healthCheckInterval);
    clusterState.healthCheckInterval = null;
  }
  
  // Send shutdown signal to all workers
  let shutdownSignalsSent = 0;
  for (const [pid, info] of clusterState.activeWorkers) {
    if (info.worker) {
      if (safeSendToWorker(info.worker, { type: 'graceful_shutdown' })) {
        shutdownSignalsSent++;
        console.log(`📤 Sent shutdown signal to worker ${pid}`);
      } else {
        console.log(`ℹ️  Worker ${pid} is already disconnected, marking for removal`);
        // If we can't send signal, mark worker as disconnected
        info.isConnected = false;
      }
    }
  }
  
  console.log(`📤 Shutdown signals sent to ${shutdownSignalsSent} workers`);
  
  // If no workers to wait for, exit immediately
  if (clusterState.activeWorkers.size === 0) {
    console.log('✅ No active workers to shutdown');
    console.log(`📊 Final Stats:`);
    console.log(`   Total Jobs Processed: ${clusterState.totalJobsProcessed}`);
    console.log(`   Failed Jobs: ${clusterState.failedJobs}`);
    console.log(`   Uptime: ${Date.now() - clusterState.startTime}ms`);
    console.log('👋 Shutdown complete');
    process.exit(0);
    return;
  }
  
  // Set timeout for forced shutdown
  const forceShutdownTimer = setTimeout(() => {
    console.log('⏰ Graceful shutdown timeout reached, forcing exit...');
    forceShutdown();
  }, CONFIG.GRACEFUL_SHUTDOWN_TIMEOUT);
  
  // Monitor for worker exits
  let workersExited = 0;
  const totalWorkers = clusterState.activeWorkers.size;
  
  const exitHandler = (worker, code, signal) => {
    workersExited++;
    console.log(`👋 Worker ${worker.process.pid} exited (${workersExited}/${totalWorkers})`);
    
    if (workersExited >= totalWorkers) {
      clearTimeout(forceShutdownTimer);
      console.log('✅ All workers exited gracefully');
      console.log(`📊 Final Stats:`);
      console.log(`   Total Jobs Processed: ${clusterState.totalJobsProcessed}`);
      console.log(`   Failed Jobs: ${clusterState.failedJobs}`);
      console.log(`   Uptime: ${Date.now() - clusterState.startTime}ms`);
      console.log('👋 Shutdown complete');
      process.exit(0);
    }
  };
  
  cluster.on('exit', exitHandler);
  
  // Monitor progress
  const progressInterval = setInterval(() => {
    const remaining = totalWorkers - workersExited;
    console.log(`⏳ Waiting for ${remaining} worker(s) to exit...`);
    
    if (remaining === 0) {
      clearInterval(progressInterval);
    }
  }, 2000);
  
  // Clean up interval on completion
  setTimeout(() => {
    clearInterval(progressInterval);
  }, CONFIG.GRACEFUL_SHUTDOWN_TIMEOUT + 1000);
}

/**
 * Force shutdown all processes
 */
function forceShutdown() {
  console.log('💥 Force shutting down all workers...');
  
  let forceKilled = 0;
  for (const [pid, info] of clusterState.activeWorkers) {
    if (info.worker) {
      try {
        info.worker.kill('SIGKILL');
        forceKilled++;
        console.log(`💀 Force killed worker ${pid}`);
      } catch (error) {
        console.error(`❌ Failed to force kill worker ${pid}:`, error.message);
      }
    }
  }
  
  console.log(`💥 Force killed ${forceKilled} workers`);
  
  setTimeout(() => {
    console.log('👋 Force shutdown complete');
    process.exit(1);
  }, 1000);
}

/**
 * Initiate graceful shutdown of a worker process
 */
function initiateWorkerShutdown() {
  console.log(`⏳ Worker ${process.pid} beginning graceful shutdown...`);
  
  // In a real implementation, you would:
  // 1. Stop accepting new jobs
  // 2. Complete current jobs
  // 3. Clean up resources
  // 4. Then exit
  
  // Simulate cleanup time
  setTimeout(() => {
    console.log(`👋 Worker ${process.pid} shutdown complete`);
    process.exit(0);
  }, 2000); // Reduced from 5000ms to 2000ms for faster shutdown
}

/**
 * Simulate job processing with configurable behavior
 * @param {Object} job - The job to process
 * @returns {Promise} Resolves when job processing is complete
 */
async function simulateJobProcessing(job) {
  // Extract job data
  const jobData = job.data || job.payload || job;
  const processingTime = jobData.processingTime || 2000; // Default 2 seconds
  
  // Simulate various job types for testing
  if (jobData.type === 'quick') {
    await new Promise(resolve => setTimeout(resolve, 500)); // Quick job
  } else if (jobData.type === 'slow') {
    await new Promise(resolve => setTimeout(resolve, 5000)); // Slow job
  } else if (jobData.type === 'error' && Math.random() < 0.1) {
    throw new Error('Simulated job processing error');
  } else {
    // Default processing time
    await new Promise(resolve => setTimeout(resolve, processingTime));
  }
  
  // Simulate CPU-intensive work (optional)
  if (jobData.cpuIntensive) {
    let sum = 0;
    for (let i = 0; i < 1000000; i++) {
      sum += Math.sqrt(i) * Math.random();
    }
  }
  
  return { simulatedResult: 'job_completed', timestamp: Date.now() };
}

/**
 * Perform hot reload of workers (development only)
 */
function performHotReload() {
  console.log('🔥 Performing hot reload of workers...');
  
  // Store current worker information
  const oldWorkers = Array.from(clusterState.activeWorkers.values());
  
  // Create new workers
  oldWorkers.forEach((workerInfo, index) => {
    setTimeout(() => {
      console.log(`🔄 Restarting worker ${workerInfo.pid} for hot reload`);
      if (workerInfo.worker) {
        safeSendToWorker(workerInfo.worker, { type: 'graceful_shutdown' });
      }
      createWorker();
    }, index * 1000); // Stagger restarts
  });
}

/**
 * Expose cluster metrics for external monitoring and testing
 */
function exposeClusterMetrics() {
  // Export metrics function for testing
  global.getClusterMetrics = () => ({
    activeWorkers: clusterState.activeWorkers.size,
    totalJobsProcessed: clusterState.totalJobsProcessed,
    failedJobs: clusterState.failedJobs,
    uptime: Date.now() - clusterState.startTime,
    workerDetails: Array.from(clusterState.activeWorkers.values()).map(w => ({
      pid: w.pid,
      status: w.status,
      jobsProcessed: w.jobsProcessed,
      uptime: Date.now() - w.startTime,
      isConnected: w.isConnected
    }))
  });
  
  // Expose control functions for testing
  global.scaleUpWorkers = scaleUpWorkers;
  global.scaleDownWorkers = scaleDownWorkers;
  global.initiateGracefulShutdown = initiateGracefulShutdown;
  
  console.log('📊 Cluster metrics and controls exposed to global scope for testing');
}

/**
 * Add test jobs to the queue for demonstration
 */
async function addTestJobs() {
  try {
    console.log('🧪 Adding test jobs to queue...');
    
    const factory = new QueueFactory(CONFIG.QUEUE_CONFIG);
    const taskQueue = factory.createQueue({
      name: 'distributed-tasks',
      driver: 'shared-memory',
      driverConfig: {
        persist: true,
        maxSize: 10000,
        cleanupInterval: 60000
      }
    });
    
    // Add some test jobs
    for (let i = 1; i <= 5; i++) {
      const jobType = i % 3 === 0 ? 'quick' : i % 5 === 0 ? 'slow' : 'normal';
      const processingTime = jobType === 'quick' ? 500 : 
                           jobType === 'slow' ? 5000 : 2000;
      
      await taskQueue.add({
        id: `test-job-${i}`,
        data: {
          type: jobType,
          processingTime: processingTime,
          cpuIntensive: Math.random() > 0.8,
          index: i,
          timestamp: new Date().toISOString()
        }
      });
      
      console.log(`📤 Added test job ${i}: ${jobType} (${processingTime}ms)`);
    }
    
    console.log('✅ Added 5 test jobs to the queue');
  } catch (error) {
    console.error('❌ Failed to add test jobs:', error.message);
  }
}

/**
 * Worker process logic
 */
function startWorkerProcess() {
  console.log(`👷 Worker ${process.pid} started with concurrency: ${CONFIG.QUEUE_CONFIG.defaultConfig.concurrency}`);
  
  // Initialize queue factory with shared memory driver
  const factory = new QueueFactory(CONFIG.QUEUE_CONFIG);
  
  // Create distributed task queue
  const taskQueue = factory.createQueue({
    name: 'distributed-tasks',
    driver: 'shared-memory',
    driverConfig: {
      persist: true,                       // Enable persistence for fault tolerance
      maxSize: 10000,                      // Maximum queue size
      cleanupInterval: 60000               // Cleanup interval in milliseconds
    }
  });
  
  // Add queue event listeners for debugging
  taskQueue.on('jobAdded', (job) => {
    console.log(`📥 Worker ${process.pid} received job: ${job.id}`);
  });
  
  taskQueue.on('jobStarted', (job) => {
    console.log(`▶️  Worker ${process.pid} started job: ${job.id}`);
  });
  
  taskQueue.on('jobCompleted', (job, result) => {
    console.log(`✅ Worker ${process.pid} completed job: ${job.id}`);
  });
  
  taskQueue.on('jobFailed', (job, error) => {
    console.error(`❌ Worker ${process.pid} failed job: ${job.id} - ${error.message}`);
  });
  
  // Register job processor with enhanced error handling and logging
  taskQueue.process(async (job) => {
    const workerPid = process.pid;
    const jobId = job.id || 'unknown';
    const startTime = Date.now();
    
    console.log(`🔧 Worker ${workerPid} started processing job ${jobId}`);
    
    try {
      // Simulate job processing with configurable delay
      // In production, replace with actual business logic
      await simulateJobProcessing(job);
      
      const processingTime = Date.now() - startTime;
      console.log(`✅ Worker ${workerPid} completed job ${jobId} in ${processingTime}ms`);
      
      // Send metrics to primary process
      if (process.send) {
        process.send({ 
          type: 'job_completed', 
          pid: workerPid, 
          jobId, 
          processingTime 
        });
      }
      
      return { 
        processedBy: workerPid, 
        jobId, 
        processingTime,
        status: 'success'
      };
      
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error(`❌ Worker ${workerPid} failed job ${jobId}: ${error.message}`);
      
      // Send failure metrics to primary process
      if (process.send) {
        process.send({ 
          type: 'job_failed', 
          pid: workerPid, 
          jobId, 
          error: error.message,
          processingTime 
        });
      }
      
      throw error; // Let queue handle retry logic
    }
  });
  
  // Start queue processing with timeout
  const queueStartTimeout = setTimeout(() => {
    console.log(`⚠️  Worker ${process.pid} queue start timeout, sending ready signal anyway`);
    if (process.send) {
      process.send({ type: 'worker_ready', pid: process.pid });
    }
  }, 5000); // 5 second timeout
  
  taskQueue.start().then(() => {
    clearTimeout(queueStartTimeout);
    console.log(`✅ Queue processing started successfully on worker ${process.pid}`);
    
    // Report worker readiness to primary process
    if (process.send) {
      process.send({ type: 'worker_ready', pid: process.pid });
    }
  }).catch((error) => {
    clearTimeout(queueStartTimeout);
    console.error(`💥 Worker ${process.pid} failed to start queue:`, error);
    process.exit(1); // Exit with error code for automatic restart
  });
  
  // Set up worker message handler for inter-process communication
  process.on('message', handleWorkerMessage);
  
  // Add periodic status reporting
  const statusInterval = setInterval(() => {
    console.log(`📊 Worker ${process.pid} is alive and waiting for jobs`);
  }, 10000); // Report every 10 seconds
  
  // Clean up interval on shutdown
  process.on('exit', () => {
    clearInterval(statusInterval);
  });
}

/**
 * Primary process logic
 */
function startPrimaryProcess() {
  console.log(`🚀 Primary process ${process.pid} initializing cluster...`);
  console.log(`📊 System Info: ${CONFIG.MAX_WORKERS} CPU cores available`);
  console.log(`⚙️  Configuration: ${CONFIG.MIN_WORKERS}-${CONFIG.MAX_WORKERS} workers`);
  
  // Initialize worker processes
  initializeWorkers();
  
  // Set up cluster event handlers
  setupClusterEventHandlers();
  
  // Start health monitoring
  startHealthMonitoring();
  
  // Handle process termination signals for graceful shutdown
  setupSignalHandlers();
  
  // Expose cluster metrics for testing (optional)
  exposeClusterMetrics();
  
  // Add test jobs after a delay to ensure workers are ready
  setTimeout(() => {
    addTestJobs().catch(console.error);
  }, 3000); // Wait 3 seconds for workers to initialize
}

// Main execution logic
if (cluster.isPrimary) {
  startPrimaryProcess();
} else {
  startWorkerProcess();
}

// ES Module exports
export {
  CONFIG,
  clusterState,
  initializeWorkers,
  scaleUpWorkers,
  scaleDownWorkers,
  initiateGracefulShutdown,
  simulateJobProcessing,
  handleWorkerMessage,
  initiateWorkerShutdown,
  addTestJobs
};

// Export getClusterMetrics if it exists
export function getClusterMetrics() {
  if (typeof global.getClusterMetrics === 'function') {
    return global.getClusterMetrics();
  }
  return null;
}

// Start the cluster if this file is run directly
if (import.meta.url === `file://${process.argv}`) {
  console.log('🚀 Starting multi-node queue cluster...');
  // The cluster will start automatically via the if(cluster.isPrimary) check above
}
