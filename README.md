Aetherframework queue-module - Modern Node.js Queue System

📦 Installation

Install the queue module via npm:

```bash
npm install @aetherframework/queue-module
```

🚀 Quick Start

Basic Usage with ES Module Syntax

```javascript
// Import the QueueFactory using ES module syntax
import QueueFactory from '@aetherframework/queue-module';

// Initialize with memory driver (zero dependencies, fastest setup)
const factory = new QueueFactory({
  defaultDriver: 'memory',
  defaultConfig: {
    concurrency: 5,      // Number of concurrent jobs
    maxRetries: 3,       // Maximum retry attempts
    timeout: 30000       // Job timeout in milliseconds
  }
});

// Create a queue instance
const queue = factory.createQueue({ 
  name: 'my-first-queue' 
});

// Register a job handler
queue.process('greet', async (job) => {
  console.log(`Hello ${job.data.name}!`);
  return { 
    greeted: true, 
    timestamp: Date.now() 
  };
});

// Add a job to the queue
await queue.add({
  handlerName: 'greet',
  data: { name: 'World' }
});

// Start processing jobs
queue.start();
```

📚 API Reference

Core Exports

The module provides the following exports:

```javascript
// Main factory class
import QueueFactory from '@aetherframework/queue-module';

// Individual component imports
import { 
  QueueFactory,
  registerDriver,
  loadConfig,
  QueueError,
  createJobLogger,
  createRateLimiter,
  createRetryManager,
  createMetricsCollector,
  createCircuitBreaker,
  serializers,
  validators
} from '@aetherframework/queue-module';
```

Available Components

| Component | Description | Import Path |
|-----------|-------------|-------------|
| QueueFactory | Main factory class for creating queues | `@aetherframework/queue-module` |
| registerDriver | Register custom queue drivers | `@aetherframework/queue-module` |
| loadConfig | Configuration loader utility | `@aetherframework/queue-module` |
| QueueError | Custom error class for queue operations | `@aetherframework/queue-module` |
| createJobLogger | Middleware for job logging | `@aetherframework/queue-module` |
| createRateLimiter | Middleware for rate limiting | `@aetherframework/queue-module` |
| createRetryManager | Middleware for retry logic | `@aetherframework/queue-module` |
| createMetricsCollector | Middleware for metrics collection | `@aetherframework/queue-module` |
| createCircuitBreaker | Middleware for circuit breaking | `@aetherframework/queue-module` |
| serializers | Job serialization utilities | `@aetherframework/queue-module` |
| validators | Validation utilities | `@aetherframework/queue-module` |

🛠️ Driver Configuration Examples

1. Memory Driver (Development/Testing)

```javascript
import QueueFactory from '@aetherframework/queue-module';

const factory = new QueueFactory({ 
  defaultDriver: 'memory'
});

const memoryQueue = factory.createQueue({
  name: 'user-activity-tracker',
  driver: 'memory',
  driverConfig: {
    maxSize: 10000,     // Maximum queue size
    persist: false      // Transient (clears on restart)
  }
});

// Performance: 250,000+ TPS
// Perfect for: Analytics events, user session data, click tracking
```

2. Shared-Memory Driver (Multi-Process)

```javascript
import QueueFactory from '@aetherframework/queue-module';
import os from 'os';

const factory = new QueueFactory({ 
  defaultDriver: 'shared-memory' 
});

const sharedMemoryQueue = factory.createQueue({
  name: 'pdf-processing-queue',
  driver: 'shared-memory',
  driverConfig: {
    sharedPath: '/tmp/pdf-queue',  // Shared directory across processes
    persist: true,                 // Persist across restarts
    maxSize: 50000,                // Larger queue size
    lockTimeout: 5000              // 5-second lock timeout
  }
});

// Use with Node.js cluster for maximum CPU utilization
const cpuCount = os.cpus().length;
for (let i = 0; i < cpuCount; i++) {
  // Each worker processes jobs from shared memory
}

// Performance: 200,000+ TPS per cluster
// Perfect for: Image processing, PDF generation, real-time notifications
```

3. Redis Driver (Production)

```javascript
import QueueFactory from '@aetherframework/queue-module';

const factory = new QueueFactory({ 
  defaultDriver: 'redis' 
});

const redisQueue = factory.createQueue({
  name: 'email-campaign-queue',
  driver: 'redis',
  driverConfig: {
    url: 'redis://production-redis-cluster:6379',
    password: process.env.REDIS_PASSWORD,  // Use environment variables
    db: 1,                                 // Separate queue databases
    keyPrefix: 'campaign:',                // Namespace your keys
    connectionTimeout: 10000               // 10-second timeout
  }
});

// Features: Automatic failover, replication, persistence
// Perfect for: Multi-region deployments, containerized environments
// Performance: 50,000-100,000+ TPS
```

4. File Driver (Persistent Storage)

```javascript
import QueueFactory from '@aetherframework/queue-module';

const factory = new QueueFactory({ 
  defaultDriver: 'file' 
});

const fileQueue = factory.createQueue({
  name: 'data-backup-queue',
  driver: 'file',
  driverConfig: {
    dataDir: './data/queue-storage',  // Directory for queue files
    maxSize: 100000,                   // Larger capacity for storage
    flushInterval: 1000                 // Flush to disk every second
  }
});

// Features: Built-in persistence, human-readable files, easy debugging
// Perfect for: Cold storage, auditing, regulatory compliance backups
```

🔧 Advanced Usage

Middleware Integration

```javascript
import QueueFactory, { 
  createJobLogger, 
  createRateLimiter, 
  createRetryManager 
} from '@aetherframework/queue-module';

const factory = new QueueFactory({
  defaultDriver: 'redis',
  middleware: [
    createJobLogger({ level: 'info' }),      // Log all job operations
    createRateLimiter({ maxJobsPerSecond: 100 }), // Rate limiting
    createRetryManager({ maxRetries: 3, backoff: 'exponential' }) // Retry logic
  ]
});

const queue = factory.createQueue({ name: 'production-queue' });
```

Error Handling

```javascript
import QueueFactory, { QueueError } from '@aetherframework/queue-module';

const factory = new QueueFactory();
const queue = factory.createQueue({ name: 'error-handling-queue' });

queue.process('critical-task', async (job) => {
  try {
    return await processCriticalTask(job.data);
  } catch (error) {
    // Custom error handling
    if (error instanceof QueueError) {
      console.error('Queue error:', error.message);
    }
    throw error; // Will trigger retry logic if configured
  }
});
```

Event Handling

```javascript
import QueueFactory from '@aetherframework/queue-module';

const factory = new QueueFactory();
const queue = factory.createQueue({ name: 'event-driven-queue' });

// Listen to queue events
queue.on('jobAdded', (job) => {
  console.log(`Job ${job.id} added to queue`);
});

queue.on('jobStarted', (job) => {
  console.log(`Job ${job.id} started processing`);
});

queue.on('jobCompleted', (job, result) => {
  console.log(`Job ${job.id} completed with result:`, result);
});

queue.on('jobFailed', (job, error) => {
  console.error(`Job ${job.id} failed:`, error.message);
});

queue.on('queueEmpty', () => {
  console.log('Queue is empty');
});
```

📊 Performance Comparison

| Driver | TPS Range | Use Case | Setup Complexity |
|--------|-----------|----------|-----------------|
| Memory | 250,000+ TPS | Development, Single Process | ⭐ |
| Shared-Memory | 200,000+ TPS per cluster | Multi-Process Single Server | ⭐⭐ |
| Redis | 50,000-100,000+ TPS | Distributed Production | ⭐⭐⭐ |
| File | 10,000-50,000 TPS | Persistence/Backup | ⭐⭐ |

🎯 Best Practices

Development Environment

```javascript
// .env file for development
QUEUE_DRIVER=memory
QUEUE_CONCURRENCY=5
MEMORY_DRIVER_MAX_SIZE=10000
MEMORY_DRIVER_PERSIST=false
```

Production Environment

```javascript
// .env file for production
QUEUE_DRIVER=redis
REDIS_URL=redis://production-redis-cluster:6379
REDIS_DB=0
REDIS_PASSWORD=${REDIS_PASSWORD}
REDIS_KEY_PREFIX=queue:
QUEUE_CONCURRENCY=10
QUEUE_MAX_RETRIES=5
QUEUE_TIMEOUT=60000
```

Hybrid Architecture

```javascript
import QueueFactory from '@aetherframework/queue-module';

// Use different drivers for different purposes
const factory = new QueueFactory({
  defaultDriver: 'redis',                // Production default
  drivers: {
    redis: { url: process.env.REDIS_URL },
    memory: { maxSize: 5000 },          // In-memory cache queue
    file: { dataDir: './queue-backup' } // Backup storage
  }
});

// High-speed real-time events
const analyticsQueue = factory.createQueue({
  name: 'analytics-events',
  driver: 'memory',                      // Memory for speed
  maxSize: 1000                          // Keep recent events only
});

// Reliable email delivery
const emailQueue = factory.createQueue({
  name: 'email-queue',
  driver: 'redis'                        // Redis for persistence
});

// Monthly report generation
const reportsQueue = factory.createQueue({
  name: 'monthly-reports',
  driver: 'file'                         // File for large, infrequent jobs
});
```

🔍 Monitoring and Metrics

```javascript
import QueueFactory, { createMetricsCollector } from '@aetherframework/queue-module';

const factory = new QueueFactory({
  defaultDriver: 'redis',
  middleware: [
    createMetricsCollector({
      enabled: true,
      interval: 60000, // Collect metrics every minute
      metrics: ['throughput', 'latency', 'queueSize', 'errorRate']
    })
  ]
});

const queue = factory.createQueue({ name: 'monitored-queue' });

// Get queue statistics
const stats = queue.getStats();
console.log('Queue Statistics:', {
  activeJobs: stats.activeJobs,
  pendingJobs: stats.pendingJobs,
  completedJobs: stats.completedJobs,
  failedJobs: stats.failedJobs,
  throughput: stats.throughput,
  averageLatency: stats.averageLatency
});
```

🚀 Scalability Examples

Multi-Process Cluster

```javascript
import QueueFactory from '@aetherframework/queue-module';
import cluster from 'cluster';
import os from 'os';

if (cluster.isPrimary) {
  const cpuCount = os.cpus().length;
  console.log(`Starting ${cpuCount} workers`);
  for (let i = 0; i < cpuCount; i++) {
    cluster.fork();
  }
} else {
  const factory = new QueueFactory({ defaultDriver: 'shared-memory' });
  const queue = factory.createQueue({ name: 'cpu-intensive-tasks' });
  queue.start();
  // Process ~200,000 jobs per second across all cores
}
```

Auto-scaling with Redis

```javascript
import QueueFactory from '@aetherframework/queue-module';

const factory = new QueueFactory({
  defaultDriver: 'redis',
  defaultConfig: {
    redis: { url: process.env.REDIS_CLUSTER_URL },
    autoScale: true,      // Enable auto-scaling
    minWorkers: 2,        // Minimum number of workers
    maxWorkers: 10,       // Maximum number of workers
    scaleThreshold: 100    // Scale up when queue depth > 100
  }
});

const queue = factory.createQueue({ name: 'auto-scaling-queue' });
// Automatically scale workers based on queue depth
```

📄 License

MIT © Aether Framework

---

This updated README provides comprehensive documentation for installing and using the `@aetherframework/queue-module` with proper ES module syntax and English comments. The module offers a high-performance, scalable queue system with multiple driver options for different use cases, from development to production environments.