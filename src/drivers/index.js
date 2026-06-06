/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/drivers/index
 */

import MemoryDriver from './memory-driver.js';
import SharedMemoryDriver from './shared-memory-driver.js';
import FileDriver from './file-driver.js';

/**
 * Driver registry mapping driver names to their classes.
 * External drivers are initialized as null to enable lazy loading.
 */
const DRIVERS = {
  memory: MemoryDriver,
  'shared-memory': SharedMemoryDriver,
  file: FileDriver,
  redis: null,
  kafka: null,
  mq: null,
  bullmq: null
};

// Pre-compute available driver lists to avoid recreating arrays on every call
const AVAILABLE_DRIVERS = Object.freeze(['memory', 'shared-memory', 'file']);
const DEPENDENT_DRIVERS = Object.freeze(['redis', 'kafka', 'mq', 'bullmq']);

/**
 * Get a driver class by name with ultra-fast lazy loading.
 * Optimized for the Hot Path (when driver is already cached).
 * 
 * @param {string} name - Driver name
 * @returns {Promise<Class>} Driver class
 */
export async function getDriver(name) {
  // 🔥 OPTIMIZATION: `in` operator is heavily optimized by V8 Inline Caches
  if (!(name in DRIVERS)) {
    throw new Error(`Unknown driver: "${name}". Available: ${Object.keys(DRIVERS).join(', ')}`);
  }
  
  let DriverClass = DRIVERS[name];
  
  // 🔥 OPTIMIZATION: Fast truthy check. If loaded, return immediately (Hot Path)
  if (DriverClass) return DriverClass;

  // --- COLD PATH: Dynamic Loading ---
  
  if (name === 'redis') {
    try {
      const module = await import('./redis-driver.js');
      DriverClass = module.default || module;
      DRIVERS.redis = DriverClass; // Cache for subsequent calls
      return DriverClass;
    } catch (error) {
      // 🔥 OPTIMIZATION: Expensive URL resolution only happens on failure
      if (error.code === 'ERR_MODULE_NOT_FOUND') {
        const filePath = new URL('./redis-driver.js', import.meta.url).href;
        if (error.message.includes('ioredis')) {
          throw new Error('Redis driver requires ioredis. Install: npm install ioredis');
        }
        throw new Error(`Redis driver not found at: ${filePath}\nCurrent dir: ${process.cwd()}`);
      }
      throw error;
    }
  }

  // Fallback for other unimplemented lazy drivers (mq, bullmq)
  throw new Error(`Driver "${name}" is registered but not yet implemented or missing dependencies.`);
}

/**
 * Synchronous driver retrieval (Ultra-fast Hot Path only).
 * Will throw if the driver requires lazy loading and hasn't been preloaded.
 * 
 * @param {string} name - Driver name
 * @returns {Class} Driver class
 */
export function getDriverSync(name) {
  // 🔥 OPTIMIZATION: Direct property access + truthy check
  const DriverClass = DRIVERS[name];
  
  if (!DriverClass) {
    if (!(name in DRIVERS)) {
      throw new Error(`Unknown driver: ${name}. Available: ${Object.keys(DRIVERS).join(', ')}`);
    }
    // It's a dependent driver that hasn't been loaded yet
    const pkg = name === 'redis' ? 'ioredis' : name === 'kafka' ? 'kafkajs' : 'unknown';
    throw new Error(
      `${name} driver not loaded synchronously. Use await getDriver('${name}') or preload it. ` +
      `Requires: npm install ${pkg}`
    );
  }
  
  return DriverClass;
}

/**
 * Register a custom driver implementation.
 * 
 * @param {string} name - Driver name
 * @param {Class} DriverClass - Driver class
 */
export function registerDriver(name, DriverClass) {
  DRIVERS[name] = DriverClass;
}

/**
 * Preload a single driver to warm up the cache.
 * 
 * @param {string} name - Driver name to preload
 * @returns {Promise<void>}
 */
export async function preloadDriver(name) {
  await getDriver(name);
}

/**
 * 🔥 TOP-TIER OPTIMIZATION: Concurrent Preloading
 * Loads multiple heavy drivers in parallel using Promise.all.
 * Use this at application startup to eliminate sequential I/O latency.
 * 
 * @param {string[]} names - Array of driver names to preload
 * @returns {Promise<void>}
 */
export async function preloadDrivers(names) {
  if (!Array.isArray(names) || names.length === 0) return;
  
  // Deduplicate names to avoid redundant imports
  const uniqueNames = [...new Set(names)];
  
  // Execute all dynamic imports concurrently
  await Promise.all(uniqueNames.map(name => getDriver(name).catch(err => {
    console.warn(`⚠️ Failed to preload driver "${name}": ${err.message}`);
    return null; // Swallow error to prevent one failure from blocking others
  })));
}

/**
 * Check if a driver is available without throwing errors.
 * 
 * @param {string} name - Driver name
 * @returns {Promise<boolean>}
 */
export async function isDriverAvailable(name) {
  try {
    await getDriver(name);
    return true;
  } catch {
    // 🔥 OPTIMIZATION: Empty catch block is faster than capturing unused error variable
    return false;
  }
}

/**
 * Get list of built-in drivers (no external dependencies).
 * Returns a frozen array to prevent accidental mutation.
 * 
 * @returns {ReadonlyArray<string>}
 */
export function getAvailableDrivers() {
  return AVAILABLE_DRIVERS;
}

/**
 * Get list of drivers requiring external npm packages.
 * 
 * @returns {ReadonlyArray<string>}
 */
export function getDependentDrivers() {
  return DEPENDENT_DRIVERS;
}

/**
 * Get detailed metadata and dependency info for a driver.
 * 
 * @param {string} name - Driver name
 * @returns {Promise<Object>}
 */
export async function getDriverInfo(name) {
  const info = {
    name,
    available: false,
    requiresDependencies: DEPENDENT_DRIVERS.includes(name),
    dependencies: []
  };
  
  if (name === 'redis') info.dependencies = ['ioredis'];
  else if (name === 'kafka') info.dependencies = ['kafkajs'];
  
  if (AVAILABLE_DRIVERS.includes(name)) {
    info.available = true;
  } else {
    info.available = await isDriverAvailable(name);
  }
  
  return info;
}

export { DRIVERS };
