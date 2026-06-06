/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/core/EventEmitter
 */
export default class EnhancedEventEmitter {
  constructor() {
    this.listeners = new Map();
    this.maxListeners = 10;
  }

  /**
   * Add an event listener
   * @param {string} event - Event name
   * @param {Function} listener - Listener function
   * @returns {EnhancedEventEmitter} this
   */
  on(event, listener) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }
    
    const listeners = this.listeners.get(event);
    if (listeners.length >= this.maxListeners) {
      console.warn(`Max listeners (${this.maxListeners}) exceeded for event: ${event}`);
    }
    
    listeners.push(listener);
    return this;
  }

  /**
   * Add a one-time event listener
   * @param {string} event - Event name
   * @param {Function} listener - Listener function
   * @returns {EnhancedEventEmitter} this
   */
  once(event, listener) {
    const onceWrapper = (...args) => {
      this.off(event, onceWrapper);
      listener.apply(this, args);
    };
    onceWrapper.listener = listener;
    this.on(event, onceWrapper);
    return this;
  }

  /**
   * Remove an event listener
   * @param {string} event - Event name
   * @param {Function} listener - Listener function
   * @returns {EnhancedEventEmitter} this
   */
  off(event, listener) {
    if (!this.listeners.has(event)) return this;
    
    const listeners = this.listeners.get(event);
    const filtered = listeners.filter(l => l !== listener && l.listener !== listener);
    this.listeners.set(event, filtered);
    return this;
  }

  /**
   * Emit an event
   * @param {string} event - Event name
   * @param {...*} args - Arguments to pass to listeners
   * @returns {Promise<void>}
   */
  async emit(event, ...args) {
    if (!this.listeners.has(event)) return;
    
    const listeners = [...this.listeners.get(event)];
    
    for (const listener of listeners) {
      try {
        await listener.apply(this, args);
      } catch (error) {
        this.emit('error', error);
      }
    }
  }

  /**
   * Remove all listeners for an event or all events
   * @param {string} [event] - Event name (optional)
   * @returns {EnhancedEventEmitter} this
   */
  removeAllListeners(event) {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
    return this;
  }

  /**
   * Get listener count for an event
   * @param {string} event - Event name
   * @returns {number}
   */
  listenerCount(event) {
    if (!this.listeners.has(event)) return 0;
    return this.listeners.get(event).length;
  }
}
