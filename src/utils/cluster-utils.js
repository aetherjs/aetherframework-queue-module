/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/utils/cluster-utils
 */


import os from 'os';
import crypto from 'crypto';
import cluster from 'cluster';

/**
 * Generate a unique node ID for cluster identification
 * @returns {string} Unique node ID
 */
export function generateNodeId() {
  const hostname = os.hostname();
  const pid = process.pid;
  const random = crypto.randomBytes(4).toString('hex');
  return `${hostname}-${pid}-${random}`;
}

/**
 * Check if current process is the primary node in cluster
 * @returns {boolean}
 */
export function isPrimaryNode() {
  // In Node.js cluster module, primary has specific properties
  // For standalone usage, we assume single node is primary
  if (cluster.isPrimary !== undefined) {
    return cluster.isPrimary;
  }
  return true; // Default to true for non-clustered environments
}

/**
 * Get number of available CPU cores
 * @returns {number}
 */
export function getCpuCount() {
  return os.cpus().length;
}
