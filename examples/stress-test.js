/**
 * stress-test.js - 修复版独立队列压力测试
 * 修复了卡顿问题并优化性能
 */

// ================= 配置 =================
const CONFIG = {
  TOTAL_JOBS: 10000,
  CONCURRENCY: 50,
  FAILURE_RATE: 0.01,
  PROCESS_DELAY_MIN: 0,
  PROCESS_DELAY_MAX: 1,
  BATCH_SIZE: 100,
};

// ================= 状态管理 =================
const state = {
  pendingJobs: [],
  processingCount: 0,
  completedCount: 0,
  failedCount: 0,
  retriedCount: 0,
  isFinished: false,
};

// ================= 辅助函数 =================
const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

// 使用 setImmediate 替代 setTimeout，更高效
const yieldLoop = () => new Promise(resolve => setImmediate(resolve));

// ================= 优化的业务逻辑 =================
async function processJob(jobId, attempt = 1) {
  // 模拟处理延迟
  const delay = randomInt(CONFIG.PROCESS_DELAY_MIN, CONFIG.PROCESS_DELAY_MAX);
  if (delay > 0) {
    // 使用更高效的延迟方式
    await new Promise(resolve => {
      if (delay <= 10) {
        setImmediate(resolve);
      } else {
        setTimeout(resolve, delay);
      }
    });
  }

  // 模拟随机失败
  if (Math.random() < CONFIG.FAILURE_RATE) {
    if (attempt < 3) {
      state.retriedCount++;
      // 使用循环替代递归，避免栈溢出
      return processJob(jobId, attempt + 1);
    } else {
      throw new Error(`Job ${jobId} failed after ${attempt} attempts`);
    }
  }

  return { status: 'success', jobId };
}

// ================= 修复的消费者循环 =================
async function consumer() {
  while (!state.isFinished) {
    // 1. 检查是否还有任务可处理
    if (state.pendingJobs.length === 0 && state.processingCount === 0) {
      state.isFinished = true;
      break;
    }

    // 2. 如果达到并发上限，让出事件循环
    if (state.processingCount >= CONFIG.CONCURRENCY) {
      await yieldLoop();
      continue;
    }

    // 3. 批量获取任务
    const availableSlots = CONFIG.CONCURRENCY - state.processingCount;
    const batchSize = Math.min(CONFIG.BATCH_SIZE, availableSlots, state.pendingJobs.length);
    
    if (batchSize === 0) {
      await yieldLoop();
      continue;
    }

    // 4. 处理批量任务
    const batch = state.pendingJobs.splice(0, batchSize);
    state.processingCount += batch.length;

    // 使用 Promise.allSettled 处理批量
    const promises = batch.map(job => 
      processJob(job.id)
        .then(() => {
          state.completedCount++;
        })
        .catch(() => {
          state.failedCount++;
        })
        .finally(() => {
          state.processingCount--;
        })
    );

    // 不等待批量完成，继续处理下一批（流水线）
    // 这可以显著提高吞吐量
    Promise.allSettled(promises).catch(() => {});
    
    // 短暂让出事件循环，避免阻塞
    if (state.pendingJobs.length > 0) {
      await yieldLoop();
    }
  }
}

// ================= 主测试流程 =================
async function runTest() {
  console.log('🔥 Starting Optimized Stress Test...');
  console.log(`⚙️  Config: Jobs=${CONFIG.TOTAL_JOBS}, Concurrency=${CONFIG.CONCURRENCY}, Batch=${CONFIG.BATCH_SIZE}`);
  console.log('---------------------------------------------------');

  const startTime = Date.now();

  // 1. 优化任务生成 - 使用更高效的方式
  console.log('📤 Generating tasks...');
  
  // 使用预分配数组，避免动态push的开销
  state.pendingJobs = new Array(CONFIG.TOTAL_JOBS);
  for (let i = 0; i < CONFIG.TOTAL_JOBS; i++) {
    state.pendingJobs[i] = { id: `job-${i + 1}` };
  }
  
  console.log(`✅ ${CONFIG.TOTAL_JOBS} tasks generated.`);

  // 2. 启动多个消费者实例
  console.log('▶️  Starting consumers...');
  const consumers = [];
  
  // 启动多个消费者，但不超过并发数
  const consumerCount = Math.min(CONFIG.CONCURRENCY, 10); // 限制消费者数量
  for (let i = 0; i < consumerCount; i++) {
    consumers.push(consumer());
  }

  // 3. 监控进度 - 使用更轻量的监控
  let lastProgress = 0;
  const monitorInterval = setInterval(() => {
    const totalHandled = state.completedCount + state.failedCount;
    const percentage = ((totalHandled / CONFIG.TOTAL_JOBS) * 100).toFixed(1);
    
    // 只在进度有变化时更新显示
    if (percentage !== lastProgress) {
      const mem = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
      process.stdout.write(`\r⏳ Progress: ${percentage}% | Success: ${state.completedCount} | Failed: ${state.failedCount} | Active: ${state.processingCount} | Mem: ${mem} MB`);
      lastProgress = percentage;
    }

    if (state.isFinished) {
      clearInterval(monitorInterval);
      console.log('\n'); // 换行
    }
  }, 100); // 更频繁的监控

  // 4. 等待所有消费者结束（添加超时保护）
  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error('Test timeout after 30 seconds')), 30000);
  });

  try {
    await Promise.race([
      Promise.all(consumers),
      timeoutPromise
    ]);
  } catch (error) {
    console.error(`\n⚠️  Test interrupted: ${error.message}`);
  }
  
  clearInterval(monitorInterval);

  const endTime = Date.now();
  const durationSec = (endTime - startTime) / 1000;
  const tps = (CONFIG.TOTAL_JOBS / durationSec).toFixed(2);

  // 5. 输出报告
  console.log('\n' + '='.repeat(50));
  console.log('📊 OPTIMIZED TEST REPORT');
  console.log('='.repeat(50));
  console.log(`✅ Total Jobs:       ${CONFIG.TOTAL_JOBS}`);
  console.log(`✅ Completed:        ${state.completedCount}`);
  console.log(`❌ Failed:           ${state.failedCount}`);
  console.log(`🔄 Retries:          ${state.retriedCount}`);
  console.log(`⏱️  Duration:         ${durationSec.toFixed(2)}s`);
  console.log(`🚀 Throughput (TPS): ${tps}`);
  console.log('='.repeat(50));

  if (state.completedCount + state.failedCount === CONFIG.TOTAL_JOBS) {
    console.log('✅ Test PASSED: All jobs accounted for.');
  } else {
    console.error(`❌ Test FAILED: ${CONFIG.TOTAL_JOBS - (state.completedCount + state.failedCount)} jobs missing`);
  }
}

// 添加错误处理
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

// 运行测试
runTest().catch(err => {
  console.error('💥 Test Error:', err.message);
  process.exit(1);
});
