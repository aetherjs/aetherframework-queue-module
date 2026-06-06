/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/utils/config-loader
 */


import fs from 'fs';
import path from 'path';

/**
 * Load configuration from various sources
 * @param {Object} options - Loading options
 * @param {string} options.envFile - Path to .env file
 * @param {Object} options.defaults - Default configuration values
 * @returns {Object} Merged configuration
 */
export function loadConfig(options = {}) {
  const {
    envFile = '.env',
    defaults = {}
  } = options;
  
  let envConfig = {};
  
  // Load .env file if exists
  const envPath = path.resolve(process.cwd(), envFile);
  if (fs.existsSync(envPath)) {
    const envContent = fs.readFileSync(envPath, 'utf8');
    envConfig = parseEnvFile(envContent);
  }
  
  // Merge with process.env
  const processEnv = {};
  for (const key in process.env) {
    if (key.startsWith('QUEUE_')) {
      processEnv[key] = process.env[key];
    }
  }
  
  // Merge configurations: defaults < .env < process.env
  const config = {
    ...defaults,
    ...envConfig,
    ...processEnv
  };
  
  // Type conversion for common types
  return convertTypes(config);
}

/**
 * Parse .env file content
 * @param {string} content - .env file content
 * @returns {Object} Parsed environment variables
 * @private
 */
function parseEnvFile(content) {
  const result = {};
  const lines = content.split('\n');
  
  for (const line of lines) {
    const trimmed = line.trim();
    // Skip empty lines and comments
    if (!trimmed || trimmed.startsWith('#')) continue;
    
    // Match key=value pattern
    // Group 1: Key, Group 2: Value
    const match = trimmed.match(/^([^=]+)=(.*)$/);
    
    if (match) {
      // Extract key and value from capture groups
      const key = match[1].trim();
      let value = match[2].trim();
      
      // Remove quotes if present (single or double)
      if ((value.startsWith('"') && value.endsWith('"')) || 
          (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      
      result[key] = value;
    }
  }
  
  return result;
}

/**
 * Convert string values to appropriate types
 * @param {Object} config - Configuration object
 * @returns {Object} Config with converted types
 * @private
 */
function convertTypes(config) {
  const converted = { ...config };
  
  for (const key in converted) {
    const value = converted[key];
    
    // Convert boolean strings
    if (value === 'true') converted[key] = true;
    else if (value === 'false') converted[key] = false;
    
    // Convert numeric strings
    else if (!isNaN(value) && value !== '') {
      converted[key] = Number(value);
    }
  }
  
  return converted;
}
