/**
 * basic-usage.js - Simplified basic usage example for the queue system
 */

import QueueFactory from '../src/core/QueueFactory.js';
import { loadConfig } from '../src/utils/config-loader.js';

async function main() {
  console.log('🚀 Starting simplified queue example...');
  
  try {
    // 1. Load configuration
    const config = loadConfig({
      defaults: {
        QUEUE_DRIVER: 'memory',
        QUEUE_MAX_RETRIES: 3,
        QUEUE_CONCURRENCY: 5
      }
    });
    
    console.log('✅ Configuration loaded');
    
    // 2. Create queue factory
    const factory = new QueueFactory({
      defaultDriver: config.QUEUE_DRIVER,
      defaultConfig: {
        maxRetries: config.QUEUE_MAX_RETRIES,
        concurrency: config.QUEUE_CONCURRENCY
      }
    });
    
    console.log('✅ Queue factory created');
    
    // 3. Create a queue
    const emailQueue = factory.createQueue({
      name: 'email-queue',
      driver: 'memory'
    });
    
    console.log('✅ Email queue created:', emailQueue.name);
    
    // 4. Check available methods
    console.log('📋 Queue methods:', Object.keys(emailQueue));
    console.log('📋 Driver methods:', Object.keys(emailQueue.driver || {}));
    
    // 5. Define job processor
    const processEmail = async (job) => {
      console.log(`🔧 Processing email job: ${job.id}`);
      console.log(`📧 Sending email to: ${job.data.to}`);
      console.log(`📝 Subject: ${job.data.subject}`);
      
      // Simulate work
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      if (Math.random() > 0.8) {
        throw new Error('Random failure simulation');
      }
      
      return { sent: true, timestamp: Date.now() };
    };
    
    // 6. Register processor
    emailQueue.process(processEmail);
    console.log('✅ Job processor registered');
    
    // 7. Add jobs
    console.log('📤 Adding jobs to queue...');
    
    const job1 = await emailQueue.add({
      data: {
        to: 'user1@example.com',
        subject: 'Welcome!'
      }
    });
    console.log(`📝 Added job 1: ${job1}`);
    
    const job2 = await emailQueue.add({
      data: {
        to: 'user2@example.com',
        subject: 'Newsletter'
      }
    });
    console.log(`📝 Added job 2: ${job2}`);
    
    const job3 = await emailQueue.add({
      data: {
        to: 'user3@example.com',
        subject: 'Password Reset'
      }
    });
    console.log(`📝 Added job 3: ${job3}`);
    
    console.log(`✅ All jobs added: ${job1}, ${job2}, ${job3}`);
    
    // 8. Start queue (if method exists)
    if (typeof emailQueue.start === 'function') {
      console.log('▶️  Starting queue worker...');
      await emailQueue.start();
      console.log('✅ Queue worker started');
    } else {
      console.log('⚠️  Queue does not have start() method, processing manually...');
      
      // Manual processing
      for (let i = 0; i < 3; i++) {
        try {
          const job = await emailQueue.driver.dequeue('email-queue');
          if (job) {
            console.log(`🔧 Processing job: ${job.id}`);
            const result = await processEmail(job);
            await emailQueue.driver.ack(job.id, result);
            console.log(`✅ Job ${job.id} completed`);
          }
        } catch (error) {
          console.error(`❌ Error processing job:`, error.message);
        }
      }
    }
    
    // 9. Get statistics
    console.log('\n📊 Queue Statistics:');
    const stats = await emailQueue.getStats();
    console.log(stats);
    
    // 10. Stop queue (if method exists)
    if (typeof emailQueue.stop === 'function') {
      console.log('\n⏹️  Stopping queue...');
      await emailQueue.stop();
      console.log('✅ Queue stopped');
    }
    
    console.log('\n🎉 Example completed!');
    
  } catch (error) {
    console.error('💥 Error:', error.message);
    console.error('📋 Stack trace:', error.stack);
  }
}

// Run directly
main().catch(console.error);
