/**
 * enterprise-usage.js - Enterprise-grade usage example (Fixed Version)
 * Fixed job data access issue and middleware execution order
 */

import QueueFactory from '../src/core/QueueFactory.js';

// 中间件函数定义（不使用动态导入）
const createJobLogger = (options = {}) => async (job, next) => {
  console.log(`[${options.logLevel || 'info'}] Job started: ${job.id}`);
  if (options.logData) {
    // 正确访问作业数据：job.data 或 job 对象本身
    const jobData = job.data || job;
    console.log(`[${options.logLevel || 'info'}] Job data:`, jobData);
  }
  
  try {
    const result = await next();
    console.log(`[${options.logLevel || 'info'}] Job completed: ${job.id}`);
    return result;
  } catch (error) {
    console.error(`[error] Job failed: ${job.id}`, error.message);
    throw error;
  }
};

const createRetryManager = (options = {}) => async (job, next) => {
  const { maxRetries = 3, baseDelay = 1000 } = options;
  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await next();
    } catch (error) {
      lastError = error;
      if (attempt < maxRetries) {
        const delay = baseDelay * attempt;
        console.log(`Retry ${attempt}/${maxRetries} for job ${job.id}, waiting ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
};

const createMetricsCollector = () => async (job, next) => {
  const startTime = Date.now();
  try {
    const result = await next();
    const duration = Date.now() - startTime;
    console.log(`Job ${job.id} processed in ${duration}ms`);
    return result;
  } catch (error) {
    console.error(`Job ${job.id} failed after ${Date.now() - startTime}ms`);
    throw error;
  }
};

const createCircuitBreaker = (options = {}) => {
  const { failureThreshold = 3 } = options;
  let failureCount = 0;
  let isOpen = false;
  
  return async (job, next) => {
    if (isOpen) {
      throw new Error('Circuit breaker is OPEN - service unavailable');
    }
    
    try {
      const result = await next();
      failureCount = 0;
      return result;
    } catch (error) {
      failureCount++;
      if (failureCount >= failureThreshold) {
        isOpen = true;
        console.log('Circuit breaker OPENED');
        setTimeout(() => {
          isOpen = false;
          failureCount = 0;
          console.log('Circuit breaker CLOSED');
        }, 30000);
      }
      throw error;
    }
  };
};

async function main() {
  console.log('🚀 Starting enterprise queue example...');
  
  try {
    // Create queue factory with enterprise configuration
    const factory = new QueueFactory({
      defaultDriver: 'memory',
      defaultConfig: {
        concurrency: 5,
        maxRetries: 3,
        timeout: 30000
      }
    });
    
    console.log('✅ Queue factory created with enterprise configuration');
    
    // Create order processing queue - DISABLE PERSISTENCE to avoid fs.existsSync error
    const orderQueue = factory.createQueue({
      name: 'order-processing',
      driver: 'memory',
      driverConfig: {
        persist: false,  // Disable persistence to avoid fs module issues
        maxSize: 10000
      }
    });
    
    console.log('✅ Order queue created:', orderQueue.name);
    
    // 创建中间件链
    const middlewares = [];
    
    // 1. Job logger middleware
    const jobLogger = createJobLogger({ 
      logData: true,
      logLevel: 'info'
    });
    middlewares.push(jobLogger);
    
    // 2. Metrics collector middleware
    const metricsCollector = createMetricsCollector();
    middlewares.push(metricsCollector);
    
    // 3. Circuit breaker middleware
    const circuitBreaker = createCircuitBreaker({ 
      failureThreshold: 3
    });
    middlewares.push(circuitBreaker);
    
    // 4. Retry manager middleware
    const retryManager = createRetryManager({ 
      maxRetries: 2,
      baseDelay: 500
    });
    middlewares.push(retryManager);
    
    console.log('✅ Middleware functions created');
    
    // Define order processor with business logic and middleware
    const processOrder = async (job) => {
      console.log(`\n🎯 Processing job: ${job.id}`);
      
      // 调试输出作业数据
      console.log('🔍 Job object keys:', Object.keys(job));
      
      // 关键修复：正确访问作业数据
      // 作业数据可能存储在 job.data 或直接是 job 对象本身
      const orderData = job.data || job;
      
      console.log('🔍 Extracted order data:', {
        orderId: orderData.orderId,
        amount: orderData.amount,
        customerId: orderData.customerId,
        items: orderData.items
      });
      
      if (!orderData) {
        console.error('❌ Job data is undefined or null');
        throw new Error('Job data is missing');
      }
      
      // 检查必要的字段
      if (!orderData.orderId) {
        console.error('❌ orderId is missing in job data');
        console.error('❌ Available data:', orderData);
        throw new Error('orderId is required in job data');
      }
      
      if (typeof orderData.amount === 'undefined') {
        console.error('❌ amount is missing in job data');
        throw new Error('amount is required in job data');
      }
      
      // Apply middlewares manually
      let currentIndex = 0;
      
      const next = async () => {
        if (currentIndex < middlewares.length) {
          const middleware = middlewares[currentIndex];
          currentIndex++;
          return await middleware(job, next);
        } else {
          // Execute the actual processing logic
          console.log(`🔧 Processing order: ${orderData.orderId}`);
          console.log(`💰 Order amount: $${orderData.amount}`);
          
          // Business rule validation
          if (orderData.amount < 0) {
            throw new Error('Invalid order amount: Amount cannot be negative');
          }
          
          if (orderData.amount > 10000) {
            throw new Error('Order amount exceeds limit: Maximum $10,000 allowed');
          }
          
          // Simulate order processing (payment, inventory, etc.)
          console.log(`⏳ Processing order ${orderData.orderId}...`);
          await new Promise(resolve => setTimeout(resolve, 100));
          
          // Simulate random failures for testing
          if (Math.random() < 0.1) {
            throw new Error('Payment gateway temporarily unavailable');
          }
          
          console.log(`✅ Order ${orderData.orderId} processed successfully`);
          return { 
            status: 'processed', 
            orderId: orderData.orderId,
            amount: orderData.amount,
            timestamp: Date.now(),
            transactionId: `txn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
          };
        }
      };
      
      return await next();
    };
    
    // Register the processor
    console.log('📝 Registering order processor...');
    if (typeof orderQueue.process === 'function') {
      orderQueue.process(processOrder);
      console.log('✅ Order processor registered with middleware chain');
    } else {
      console.log('❌ Queue does not have process() method');
      throw new Error('Queue instance missing process() method');
    }
    
    // Add sample orders to the queue
    console.log('\n📤 Adding orders to queue...');
    
    // 关键修复：确保作业数据格式正确
    // 根据日志，作业数据直接存储在作业对象中，而不是 job.data 中
    // 所以直接传递数据对象即可
    
    // Valid orders
    await orderQueue.add({ 
      orderId: 'ORD-1001', 
      amount: 100,
      customerId: 'CUST-001',
      items: ['Product A', 'Product B']
    });
    console.log('📝 Added order: ORD-1001 ($100)');
    
    await orderQueue.add({ 
      orderId: 'ORD-1002', 
      amount: 250.50,
      customerId: 'CUST-002',
      items: ['Product C']
    });
    console.log('📝 Added order: ORD-1002 ($250.50)');
    
    // Order that will fail validation (negative amount)
    await orderQueue.add({ 
      orderId: 'ORD-1003', 
      amount: -50,
      customerId: 'CUST-003',
      items: ['Product D']
    });
    console.log('📝 Added order: ORD-1003 (-$50) - Will fail validation');
    
    // Large order that might trigger circuit breaker
    await orderQueue.add({ 
      orderId: 'ORD-1004', 
      amount: 15000,
      customerId: 'CUST-004',
      items: ['Product E', 'Product F', 'Product G']
    });
    console.log('📝 Added order: ORD-1004 ($15,000) - Exceeds limit');
    
    console.log('✅ All orders added to queue');
    
    // Start processing orders
    console.log('\n▶️  Starting order queue processing...');
    
    // 检查队列是否有 start 方法
    if (typeof orderQueue.start === 'function') {
      await orderQueue.start();
      console.log('✅ Order queue processing started');
    } else {
      console.log('⚠️  Queue does not have start() method, using manual processing...');
      
      // 手动处理队列中的任务
      console.log('\n🔧 Starting manual job processing...');
      for (let i = 0; i < 4; i++) {
        try {
          // 检查 driver 是否有 dequeue 方法
          if (orderQueue.driver && typeof orderQueue.driver.dequeue === 'function') {
            const job = await orderQueue.driver.dequeue('order-processing');
            if (job) {
              console.log(`\n🎯 Processing job ${i + 1}/4: ${job.id}`);
              console.log('🔍 Job object from dequeue:', job);
              const result = await processOrder(job);
              
              // 检查 driver 是否有 ack 方法
              if (typeof orderQueue.driver.ack === 'function') {
                await orderQueue.driver.ack(job.id, result);
                console.log(`✅ Job ${job.id} completed`);
              } else {
                console.log(`⚠️  Driver does not have ack() method, job ${job.id} processed but not acknowledged`);
              }
            } else {
              console.log(`ℹ️  No job available for processing ${i + 1}/4`);
            }
          } else {
            console.log('❌ Driver does not have dequeue() method');
            break;
          }
        } catch (error) {
          console.error(`❌ Error processing job:`, error.message);
        }
      }
    }
    
    // Wait for processing to complete
    console.log('\n⏳ Waiting for orders to be processed (10 seconds)...');
    await new Promise(resolve => setTimeout(resolve, 10000));
    
    // Get queue statistics
    console.log('\n📊 Queue Statistics:');
    try {
      if (typeof orderQueue.getStats === 'function') {
        const stats = await orderQueue.getStats();
        console.log('Queue stats:', stats);
      } else {
        console.log('⚠️  Queue does not have getStats() method');
      }
    } catch (error) {
      console.error('❌ Error getting queue stats:', error.message);
    }
    
    // Stop the queue
    console.log('\n⏹️  Stopping order queue...');
    if (typeof orderQueue.stop === 'function') {
      await orderQueue.stop();
      console.log('✅ Order queue stopped');
    } else {
      console.log('⚠️  Queue does not have stop() method');
    }
    
    console.log('\n🎉 Enterprise example completed successfully!');
    
  } catch (error) {
    console.error('\n💥 Error in enterprise example:', error.message);
    console.error('🔍 Error details:', error);
    console.error('📋 Stack trace:', error.stack);
    process.exit(1);
  }
}

// 直接调用 main 函数
console.log('🚀 Running enterprise queue example...\n');
main().catch(error => {
  console.error('💥 Unhandled error in main:', error);
  process.exit(1);
});

// Export for testing or module usage
export default main;
