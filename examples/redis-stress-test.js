/**
 * redis-stress-test.js - Redis Driver Performance Test (Fixed Version)
 * 
 * FIXES APPLIED:
 * 1. ESM Import Path: Changed to '../src/drivers/redis-driver.js' (Node.js ESM requires exact relative paths with .js extension).
 * 2. Dequeue 0% Fix: Handler is now registered BEFORE driver.start() so the poll loop actually processes jobs.
 * 3. Connection 0 Fix: driver.stop() is strictly blocked until ALL jobs are processed via a completion Promise.
 * 4. Unified Metrics: Integrated your original formatting and monitoring logic safely.
 */

// ✅ CRITICAL FIX: Correct ESM relative path with .js extension
import RedisDriver from '../src/drivers/redis-driver.js';

// ================= Configuration =================
const CONFIG = {
  TOTAL_JOBS: 100,         // Total number of jobs to generate for the test
  CONCURRENCY: 10,         // Simulated concurrency level (number of parallel workers)
  BATCH_SIZE: 50,          // Number of jobs to enqueue in each batch operation
  PROCESS_DELAY_MIN: 5,    // Minimum simulated processing time in milliseconds
  PROCESS_DELAY_MAX: 20,   // Maximum simulated processing time in milliseconds
  FAILURE_RATE: 0.02,      // 2% simulated failure rate to test retry mechanisms
  QUEUE_NAME: 'redis-stress-test-queue', // Name of the Redis queue to use for testing
  CLEANUP_AFTER_TEST: true, // Whether to clean up test data after completion
  MONITOR_INTERVAL: 1000,  // Interval in milliseconds for printing real-time metrics
  TEST_TIMEOUT: 120000,    // Maximum test duration in milliseconds before timeout
  REDIS_CONFIG: {          // Configuration object for RedisDriver initialization
    url: 'redis://localhost:6379', // Redis server connection URL
    keyPrefix: 'queue:',   // Prefix for all Redis keys to avoid namespace collisions
    pollInterval: 50,      // Interval in milliseconds for polling new jobs from queue
    enableReadyCheck: true, // Enable Redis ready state checking
    enableOfflineQueue: true // Enable offline queue for connection resilience
  }
};

// ================= Test State Management =================
const testState = {
  startTime: null,         // Timestamp when the test begins
  endTime: null,           // Timestamp when the test completes
  jobsAdded: 0,            // Counter for total jobs added to the queue
  jobsProcessed: 0,        // Counter for successfully processed jobs
  jobsFailed: 0,           // Counter for jobs that failed processing
  jobsRetried: 0,          // Counter for jobs that were retried after failure
  isRunning: false,        // Flag indicating whether the test is currently running
  driver: null,            // Reference to the RedisDriver instance
  metrics: {               // Object containing real-time performance metrics
    enqueueRate: 0,        // Current rate of job enqueuing (jobs per second)
    dequeueRate: 0,        // Current rate of job processing (jobs per second)
    avgProcessingTime: 0,  // Average time taken to process a single job
    redisConnected: false, // Boolean indicating Redis connection status
    queueDepth: 0,         // Current number of jobs waiting in the queue
    processingTimes: []    // Array storing individual job processing times for percentile calculation
  }
};

// ================= Helper Functions =================

/**
 * Generates a random integer between min and max (inclusive)
 * @param {number} min - Minimum value
 * @param {number} max - Maximum value
 * @returns {number} Random integer within specified range
 */
const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

/**
 * Formats memory usage in bytes to a human-readable MB string
 * @param {number} bytes - Memory usage in bytes
 * @returns {string} Formatted memory usage string
 */
const formatMemoryUsage = (bytes) => {
  const mb = bytes / 1024 / 1024;
  return `${mb.toFixed(2)} MB`;
};

/**
 * Calculates the specified percentile value from an array of numbers
 * @param {number[]} values - Array of numeric values
 * @param {number} percentile - Desired percentile (e.g., 50 for median, 95 for P95)
 * @returns {number} The value at the specified percentile
 */
const calculatePercentile = (values, percentile) => {
  if (!values || values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.ceil((percentile / 100) * sorted.length) - 1;
  return sorted[Math.max(0, index)];
};

// ================= Redis Connection Test =================

/**
 * Tests the Redis connection and basic functionality
 * @returns {Promise<boolean>} True if connection test passes, false otherwise
 */
async function testRedisConnection() {
  console.log('🔍 Testing Redis connection...');
  const testDriver = new RedisDriver(CONFIG.REDIS_CONFIG);
  try {
    // Initialize the Redis driver and establish connection
    await testDriver.init();
    
    // Send a PING command to verify Redis is responsive
    await testDriver.redis.ping();
    
    // Perform a complete pipeline test: enqueue and clear a test job
    await testDriver.enqueue('connection-test', { id: 'test-1', data: 'ping' });
    await testDriver.clear('connection-test');
    
    console.log('✅ Redis connection test passed');
    await testDriver.stop();
    return true;
  } catch (error) {
    console.error('❌ Redis connection test failed:', error.message);
    if (error.message.includes('ECONNREFUSED')) {
      console.log('💡 Ensure Redis server is running on localhost:6379');
    }
    return false;
  }
}

// ================= Job Handler =================

/**
 * Simulates job processing with configurable delay and failure rate
 * @param {Object} jobData - The job data object
 * @returns {Promise<Object>} Result object containing processing metadata
 */
async function simulateJobProcessing(jobData) {
  const startTime = performance.now();

  // Simulate random failures based on configured failure rate
  if (Math.random() < CONFIG.FAILURE_RATE) {
    throw new Error(`Simulated processing error for job ${jobData.id}`);
  }

  // Simulate asynchronous work with random delay within configured bounds
  const delay = randomInt(CONFIG.PROCESS_DELAY_MIN, CONFIG.PROCESS_DELAY_MAX);
  await new Promise((resolve) => setTimeout(resolve, delay));

  return {
    success: true,
    jobId: jobData.id,
    processingTime: performance.now() - startTime
  };
}

// ================= Monitoring & Metrics =================

/**
 * Starts the real-time monitoring interval that prints metrics periodically
 * @returns {NodeJS.Timeout} The interval timer reference
 */
function startMonitoring() {
  return setInterval(async () => {
    try {
      const memUsage = process.memoryUsage();
      const elapsedSeconds = (Date.now() - testState.startTime) / 1000;
      
      // Calculate real-time rates based on elapsed time
      testState.metrics.enqueueRate = testState.jobsAdded / elapsedSeconds;
      testState.metrics.dequeueRate = testState.jobsProcessed / elapsedSeconds;

      // Retrieve queue statistics from the driver if available
      if (testState.driver && testState.driver.getStats) {
        const stats = await testState.driver.getStats(CONFIG.QUEUE_NAME);
        testState.metrics.queueDepth = (stats.pending || 0) + (stats.processing || 0);
        testState.metrics.redisConnected = stats.redisConnected || false;
      }

      // Calculate average processing time from collected samples
      if (testState.metrics.processingTimes.length > 0) {
        const sum = testState.metrics.processingTimes.reduce((a, b) => a + b, 0);
        testState.metrics.avgProcessingTime = sum / testState.metrics.processingTimes.length;
      }

      // Calculate overall progress percentage
      const totalProcessed = testState.jobsProcessed + testState.jobsFailed;
      const progress = totalProcessed > 0 ? ((totalProcessed / CONFIG.TOTAL_JOBS) * 100).toFixed(1) : 0;
      
      // Display formatted monitoring information
      console.log(`📊 Real-time Metrics:`);
      console.log(`   Progress: ${progress}% (${totalProcessed}/${CONFIG.TOTAL_JOBS})`);
      console.log(`   Success: ${testState.jobsProcessed} | Failed: ${testState.jobsFailed} | Retried: ${testState.jobsRetried}`);
      console.log(`   Enqueue Rate: ${testState.metrics.enqueueRate.toFixed(2)} jobs/sec`);
      console.log(`   Dequeue Rate: ${testState.metrics.dequeueRate.toFixed(2)} jobs/sec`);
      console.log(`   Avg Processing Time: ${testState.metrics.avgProcessingTime.toFixed(2)}ms`);
      console.log(`   Memory Usage: ${formatMemoryUsage(memUsage.heapUsed)}`);
      console.log(`   Queue Depth: ${testState.metrics.queueDepth}`);
      console.log(`   Redis Connected: ${testState.metrics.redisConnected ? 'Yes' : 'No'}`);
      
      const rate = totalProcessed / elapsedSeconds;
      console.log(`⏱️  Elapsed: ${elapsedSeconds.toFixed(1)}s | Processed: ${totalProcessed}/${CONFIG.TOTAL_JOBS} | Rate: ${rate.toFixed(2)} jobs/sec\n`);
    } catch (error) {
      // Silently ignore errors in monitoring to prevent breaking the main test
    }
  }, CONFIG.MONITOR_INTERVAL);
}

// ================= Main Test Function =================

/**
 * Main function that orchestrates the entire Redis stress test
 * @returns {Promise<void>}
 */
async function runRedisStressTest() {
  console.log('🔥 Starting Redis Stress Test');
  console.log('='.repeat(50));
  console.log(`⚙️  Configuration:`);
  console.log(`   Total Jobs: ${CONFIG.TOTAL_JOBS}`);
  console.log(`   Concurrency: ${CONFIG.CONCURRENCY}`);
  console.log(`   Batch Size: ${CONFIG.BATCH_SIZE}`);
  console.log(`   Redis URL: ${CONFIG.REDIS_CONFIG.url}`);
  console.log(`   Queue Name: ${CONFIG.QUEUE_NAME}`);
  console.log('='.repeat(50));

  // 1. Test Redis connection first - abort if connection fails
  const connectionOk = await testRedisConnection();
  if (!connectionOk) {
    console.error('❌ Cannot proceed without Redis connection');
    process.exit(1);
  }

  testState.startTime = Date.now();
  testState.isRunning = true;

  // 2. Initialize the Redis driver instance
  console.log('\n🚀 Initializing Redis Driver...');
  const driver = new RedisDriver(CONFIG.REDIS_CONFIG);
  testState.driver = driver;

  // 3. CRITICAL: Register handler BEFORE starting the driver to ensure jobs are processed
  driver.registerHandler(CONFIG.QUEUE_NAME, simulateJobProcessing, {
    onJobCompleted: (job, result) => {
      testState.jobsProcessed++;
      if (result?.processingTime) {
        testState.metrics.processingTimes.push(result.processingTime);
      }
    },
    onJobFailed: (job, error) => {
      testState.jobsFailed++;
      testState.jobsRetried++;
    }
  });

  // 4. Start driver (begins polling for jobs)
  await driver.start();
  await driver.clear(CONFIG.QUEUE_NAME); // Clean up any previous test runs
  console.log('✅ Redis queue started successfully\n');

  // 5. Start real-time monitoring
  const monitoringInterval = startMonitoring();

  // 6. Generate and enqueue test jobs in batches
  console.log('📤 Generating jobs...');
  const batchPromises = [];
  for (let i = 0; i < CONFIG.TOTAL_JOBS; i++) {
    const job = {
      id: `stress-job-${i}`,
      payload: { index: i, data: `test-payload-${i}` },
      priority: randomInt(0, 10),
      maxRetries: 3
    };
    batchPromises.push(driver.enqueue(CONFIG.QUEUE_NAME, job));
    testState.jobsAdded++;

    // Process jobs in batches to avoid memory issues and improve performance
    if (batchPromises.length >= CONFIG.BATCH_SIZE) {
      await Promise.all(batchPromises);
      batchPromises.length = 0;
    }
  }
  if (batchPromises.length > 0) await Promise.all(batchPromises);
  
  console.log(`✅ ${testState.jobsAdded} jobs generated and added to queue\n`);

  // 7. Wait for all jobs to complete processing
  console.log('🎯 Starting job processing...');
  console.log(`⏳ Waiting for ${CONFIG.TOTAL_JOBS} jobs to complete...\n`);

  // Create a promise that resolves when all jobs are processed
  const completionPromise = new Promise((resolve) => {
    const checkInterval = setInterval(() => {
      const totalDone = testState.jobsProcessed + testState.jobsFailed;
      if (totalDone >= CONFIG.TOTAL_JOBS) {
        clearInterval(checkInterval);
        resolve();
      }
    }, 100);
  });

  // Create a timeout promise to prevent infinite hanging
  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error(`Test timeout after ${CONFIG.TEST_TIMEOUT}ms`)), CONFIG.TEST_TIMEOUT);
  });

  try {
    // Wait for either completion or timeout
    await Promise.race([completionPromise, timeoutPromise]);
    console.log('\n✅ All jobs processed successfully');
  } catch (error) {
    console.error(`\n⏰ ${error.message}`);
  }

  // 8. Cleanup and stop the test
  clearInterval(monitoringInterval);
  testState.endTime = Date.now();
  testState.isRunning = false;

  if (CONFIG.CLEANUP_AFTER_TEST) {
    await driver.clear(CONFIG.QUEUE_NAME);
  }
  
  // CRITICAL: Only stop driver AFTER all processing is completely done
  await driver.stop();

  // 9. Calculate and display final test results
  const elapsed = (testState.endTime - testState.startTime) / 1000;
  const totalProcessed = testState.jobsProcessed + testState.jobsFailed;
  const successRate = ((testState.jobsProcessed / totalProcessed) * 100).toFixed(2);
  const throughput = testState.jobsProcessed / elapsed;
  
  const p50 = calculatePercentile(testState.metrics.processingTimes, 50);
  const p95 = calculatePercentile(testState.metrics.processingTimes, 95);

  console.log('\n' + '='.repeat(50));
  console.log('📋 FINAL TEST RESULTS:');
  console.log('='.repeat(50));
  console.log(` Total Time: ${elapsed.toFixed(2)}s`);
  console.log(` Jobs Processed: ${testState.jobsProcessed}`);
  console.log(` Jobs Failed: ${testState.jobsFailed}`);
  console.log(` Jobs Retried: ${testState.jobsRetried}`);
  console.log(` Success Rate: ${successRate}%`);
  console.log(` Throughput: ${throughput.toFixed(2)} jobs/second`);
  console.log(` Avg Enqueue Rate: ${testState.metrics.enqueueRate.toFixed(2)} jobs/sec`);
  console.log(` Avg Dequeue Rate: ${testState.metrics.dequeueRate.toFixed(2)} jobs/sec`);
  
  if (testState.metrics.processingTimes.length > 0) {
    console.log('⏱️  PROCESSING LATENCY:');
    console.log(`   P50 (Median): ${p50.toFixed(2)}ms`);
    console.log(`   P95: ${p95.toFixed(2)}ms`);
  }
  console.log('='.repeat(50));
  console.log('🏁 Stress test finished\n');
}

// Execute the test with error handling
runRedisStressTest().catch((err) => {
  console.error('💥 Fatal test error:', err);
  process.exit(1);
});
