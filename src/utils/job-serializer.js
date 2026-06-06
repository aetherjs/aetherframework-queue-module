/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/utils/job-serializer
 */


/**
 * Serialize job data for storage/transmission
 * @param {Object} job - Job object
 * @returns {string} Serialized JSON string
 */
export function serializeJob(job) {
  try {
    // Handle special types like Date, RegExp, etc. if needed
    return JSON.stringify(job, (key, value) => {
      if (value instanceof Date) {
        return { __type: 'Date', value: value.toISOString() };
      }
      if (value instanceof RegExp) {
        return { __type: 'RegExp', value: value.source, flags: value.flags };
      }
      return value;
    });
  } catch (error) {
    throw new Error(`Failed to serialize job: ${error.message}`);
  }
}

/**
 * Deserialize job data from storage/transmission
 * @param {string} serialized - Serialized JSON string
 * @returns {Object} Deserialized job object
 */
export function deserializeJob(serialized) {
  try {
    return JSON.parse(serialized, (key, value) => {
      if (value && typeof value === 'object') {
        if (value.__type === 'Date') {
          return new Date(value.value);
        }
        if (value.__type === 'RegExp') {
          return new RegExp(value.value, value.flags);
        }
      }
      return value;
    });
  } catch (error) {
    throw new Error(`Failed to deserialize job: ${error.message}`);
  }
}
