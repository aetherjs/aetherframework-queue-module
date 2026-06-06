/**
 * @license MIT
 * Copyright (c) 2026-present AetherFramework Contributors.
 * SPDX-License-Identifier: MIT
 * @module @aetherframework/queue/core/ClusterManager
 */

import EventEmitter from 'events';
import crypto from 'crypto';

export default class ClusterManager extends EventEmitter {
  /**
   * Create a new ClusterManager instance
   * @param {Object} options - Cluster configuration options
   * @param {Array} options.nodes - Initial node list with connection details
   * @param {number} options.heartbeatInterval - Heartbeat interval in milliseconds (default: 1000)
   * @param {number} options.electionTimeout - Election timeout in milliseconds (default: 5000)
   * @param {string} options.nodeId - Unique node identifier (auto-generated if not provided)
   * @param {string} options.coordinationDriver - Coordination driver type ('memory', 'shared-memory', etc.)
   * @param {Object} options.driverConfig - Configuration for the coordination driver
   */
  constructor(options = {}) {
    super();
    
    // Configuration with defaults
    this.options = {
      nodes: options.nodes || [],
      heartbeatInterval: options.heartbeatInterval || 1000,
      electionTimeout: options.electionTimeout || 5000,
      nodeId: options.nodeId || `node_${crypto.randomBytes(4).toString('hex')}`,
      coordinationDriver: options.coordinationDriver || 'memory',
      driverConfig: options.driverConfig || {},
      autoStart: options.autoStart !== false,
      ...options
    };
    
    // Internal state management
    this.nodes = new Map(); // nodeId -> { id, status, lastSeen, metadata, role }
    this.leader = null;
    this.status = 'disconnected';
    this.heartbeatInterval = null;
    this.electionTimeout = null;
    this.electionInProgress = false;
    this.votesReceived = new Set();
    
    // Statistics
    this.stats = {
      nodeId: this.options.nodeId,
      status: 'disconnected',
      leader: null,
      totalNodes: 0,
      activeNodes: 0,
      failedNodes: 0,
      uptime: 0,
      startedAt: null
    };
    
    // Initialize node list with self
    this._initializeNodes();
    
    // Auto-start if configured
    if (this.options.autoStart) {
      this.start();
    }
  }
  
  /**
   * Initialize node list with self and provided nodes
   * @private
   */
  _initializeNodes() {
    // Add self to nodes list
    this.nodes.set(this.options.nodeId, {
      id: this.options.nodeId,
      status: 'active',
      role: 'follower',
      lastSeen: Date.now(),
      metadata: {
        host: 'localhost',
        port: 0,
        version: '1.0.0',
        capabilities: ['queue', 'worker', 'scheduler'],
        ...(this.options.nodeMetadata || {})
      }
    });
    
    // Add provided nodes
    this.options.nodes.forEach(node => {
      if (node.id && node.id !== this.options.nodeId) {
        this.nodes.set(node.id, {
          ...node,
          status: 'unknown',
          role: 'follower',
          lastSeen: Date.now(),
          metadata: node.metadata || {}
        });
      }
    });
    
    this._updateStats();
  }
  
  /**
   * Start the cluster manager
   * Begins heartbeat mechanism and leader election process
   */
  start() {
    if (this.status === 'running') {
      return;
    }
    
    this.status = 'running';
    this.stats.startedAt = Date.now();
    this.stats.status = 'running';
    
    // Start heartbeat mechanism
    this._startHeartbeat();
    
    // Start leader election if no leader exists
    if (!this.leader) {
      this._startElection();
    }
    
    this.emit('clusterStarted', {
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      stats: this.getStats()
    });
  }
  
  /**
   * Stop the cluster manager
   * Stops all cluster activities and releases resources
   */
  stop() {
    if (this.status !== 'running') {
      return;
    }
    
    this.status = 'stopped';
    this.stats.status = 'stopped';
    
    // Stop heartbeat
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    
    // Stop election timeout
    if (this.electionTimeout) {
      clearTimeout(this.electionTimeout);
      this.electionTimeout = null;
    }
    
    // Step down as leader if we are the leader
    if (this.leader === this.options.nodeId) {
      this._stepDownAsLeader();
    }
    
    // Notify other nodes we're leaving
    this._broadcastLeave();
    
    this.emit('clusterStopped', {
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      stats: this.getStats()
    });
    
  }
  
  /**
   * Start heartbeat mechanism
   * Periodically sends heartbeats to maintain cluster health
   * @private
   */
  _startHeartbeat() {
    this.heartbeatInterval = setInterval(() => {
      this._sendHeartbeat();
      this._checkNodeHealth();
    }, this.options.heartbeatInterval);
  }
  
  /**
   * Send heartbeat to other nodes
   * Updates node status and detects failures
   * @private
   */
  _sendHeartbeat() {
    const now = Date.now();
    const heartbeat = {
      type: 'heartbeat',
      nodeId: this.options.nodeId,
      timestamp: now,
      status: this.status,
      role: this.nodes.get(this.options.nodeId).role,
      stats: this.getStats()
    };
    
    // Update self
    const self = this.nodes.get(this.options.nodeId);
    if (self) {
      self.lastSeen = now;
    }
    
    // Broadcast heartbeat to other nodes
    this._broadcastMessage(heartbeat);
    
    // If we are the leader, send leader heartbeat
    if (this.leader === this.options.nodeId) {
      this._sendLeaderHeartbeat();
    }
    
    this.emit('heartbeatSent', heartbeat);
  }
  
  /**
   * Send leader-specific heartbeat
   * @private
   */
  _sendLeaderHeartbeat() {
    const leaderHeartbeat = {
      type: 'leader-heartbeat',
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      leader: this.options.nodeId,
      term: this.stats.leaderTerm || 1
    };
    
    this._broadcastMessage(leaderHeartbeat);
    this.emit('leaderHeartbeatSent', leaderHeartbeat);
  }
  
  /**
   * Check health of all nodes
   * Marks nodes as failed if they haven't been seen recently
   * @private
   */
  _checkNodeHealth() {
    const now = Date.now();
    const failureThreshold = this.options.heartbeatInterval * 3; // 3 missed heartbeats
    
    for (const [nodeId, node] of this.nodes.entries()) {
      if (nodeId === this.options.nodeId) {
        continue; // Skip self
      }
      
      const timeSinceLastSeen = now - node.lastSeen;
      
      if (timeSinceLastSeen > failureThreshold) {
        if (node.status !== 'failed') {
          node.status = 'failed';
          node.lastFailed = now;
          
          this.emit('nodeFailed', {
            nodeId,
            node,
            timestamp: now,
            reason: 'heartbeat timeout'
          });
          
  
          
          // If failed node was the leader, start new election
          if (this.leader === nodeId) {
            this._startElection();
          }
        }
      } else if (node.status === 'failed' && timeSinceLastSeen <= failureThreshold) {
        // Node recovered
        node.status = 'active';
        this.emit('nodeRecovered', {
          nodeId,
          node,
          timestamp: now
        });
        
      }
    }
    
    this._updateStats();
  }
  
  /**
   * Start leader election process
   * Implements basic leader election algorithm
   * @private
   */
  _startElection() {
    if (this.electionInProgress) {
      return;
    }
    
    this.electionInProgress = true;
    this.votesReceived.clear();
    
    // Vote for self initially
    this.votesReceived.add(this.options.nodeId);
    
    const electionMessage = {
      type: 'election',
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      term: (this.stats.leaderTerm || 0) + 1
    };
    
    // Request votes from other nodes
    this._broadcastMessage(electionMessage);
    
    this.emit('electionStarted', {
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      term: electionMessage.term
    });
    
    // Set election timeout
    this.electionTimeout = setTimeout(() => {
      this._concludeElection();
    }, this.options.electionTimeout);
  }
  
  /**
   * Conclude election and determine leader
   * @private
   */
  _concludeElection() {
    if (!this.electionInProgress) {
      return;
    }
    
    const totalNodes = this.nodes.size;
    const votesNeeded = Math.floor(totalNodes / 2) + 1; // Majority
    
    if (this.votesReceived.size >= votesNeeded) {
      // We have majority, become leader
      this._becomeLeader();
    } else {
      // Not enough votes, remain follower
      this.electionInProgress = false;
      this.votesReceived.clear();
      
      this.emit('electionFailed', {
        nodeId: this.options.nodeId,
        timestamp: Date.now(),
        votesReceived: this.votesReceived.size,
        votesNeeded
      });
      
      // Wait for random time before retrying to avoid split votes
      const retryDelay = Math.random() * this.options.electionTimeout;
      setTimeout(() => {
        if (!this.leader) {
          this._startElection();
        }
      }, retryDelay);
    }
  }
  
  /**
   * Become the cluster leader
   * @private
   */
  _becomeLeader() {
    this.electionInProgress = false;
    this.leader = this.options.nodeId;
    
    const self = this.nodes.get(this.options.nodeId);
    if (self) {
      self.role = 'leader';
      self.becameLeaderAt = Date.now();
    }
    
    // Update leader term
    this.stats.leaderTerm = (this.stats.leaderTerm || 0) + 1;
    this.stats.leaderSince = Date.now();
    
    // Notify all nodes
    const leaderAnnouncement = {
      type: 'leader-announcement',
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      term: this.stats.leaderTerm
    };
    
    this._broadcastMessage(leaderAnnouncement);
    
    this.emit('leaderElected', {
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      term: this.stats.leaderTerm,
      votes: this.votesReceived.size
    });
    
    // Start leader responsibilities
    this._startLeaderResponsibilities();
  }
  
  /**
   * Start leader responsibilities
   * @private
   */
  _startLeaderResponsibilities() {
    // Leader-specific tasks can be added here
    // For example: distributing work, coordinating nodes, etc.
    
    this.emit('leaderResponsibilitiesStarted', {
      nodeId: this.options.nodeId,
      timestamp: Date.now()
    });
  }
  
  /**
   * Step down as leader
   * @private
   */
  _stepDownAsLeader() {
    if (this.leader === this.options.nodeId) {
      this.leader = null;
      
      const self = this.nodes.get(this.options.nodeId);
      if (self) {
        self.role = 'follower';
        delete self.becameLeaderAt;
      }
      
      this.emit('leaderSteppedDown', {
        nodeId: this.options.nodeId,
        timestamp: Date.now()
      });
      
    }
  }
  
  /**
   * Broadcast message to all nodes
   * @param {Object} message - Message to broadcast
   * @private
   */
  _broadcastMessage(message) {
    // In a real implementation, this would send messages to other nodes
    // For this implementation, we'll simulate broadcasting
    for (const [nodeId, node] of this.nodes.entries()) {
      if (nodeId !== this.options.nodeId && node.status === 'active') {
        this.emit('messageBroadcast', {
          from: this.options.nodeId,
          to: nodeId,
          message,
          timestamp: Date.now()
        });
        
        // Simulate message processing
        setTimeout(() => {
          this._handleMessage(nodeId, message);
        }, Math.random() * 100); // Random delay to simulate network
      }
    }
  }
  
  /**
   * Broadcast leave notification
   * @private
   */
  _broadcastLeave() {
    const leaveMessage = {
      type: 'leave',
      nodeId: this.options.nodeId,
      timestamp: Date.now()
    };
    
    this._broadcastMessage(leaveMessage);
  }
  
  /**
   * Handle incoming message from other nodes
   * @param {string} fromNodeId - Sender node ID
   * @param {Object} message - Message content
   * @private
   */
  _handleMessage(fromNodeId, message) {
    const node = this.nodes.get(fromNodeId);
    if (node) {
      node.lastSeen = Date.now();
      
      if (node.status === 'failed') {
        node.status = 'active';
        this.emit('nodeRecovered', { nodeId: fromNodeId, node, timestamp: Date.now() });
      }
    }
    
    switch (message.type) {
      case 'heartbeat':
        this._handleHeartbeat(fromNodeId, message);
        break;
      case 'leader-heartbeat':
        this._handleLeaderHeartbeat(fromNodeId, message);
        break;
      case 'election':
        this._handleElection(fromNodeId, message);
        break;
      case 'vote':
        this._handleVote(fromNodeId, message);
        break;
      case 'leader-announcement':
        this._handleLeaderAnnouncement(fromNodeId, message);
        break;
      case 'leave':
        this._handleLeave(fromNodeId, message);
        break;
      default:
        this.emit('unknownMessage', { fromNodeId, message, timestamp: Date.now() });
    }
  }
  
  /**
   * Handle heartbeat message
   * @private
   */
  _handleHeartbeat(fromNodeId, message) {
    const node = this.nodes.get(fromNodeId);
    if (node) {
      node.lastSeen = Date.now();
      node.status = 'active';
      node.role = message.role;
    }
    
    this.emit('heartbeatReceived', {
      fromNodeId,
      message,
      timestamp: Date.now()
    });
  }
  
  /**
   * Handle leader heartbeat
   * @private
   */
  _handleLeaderHeartbeat(fromNodeId, message) {
    // Update leader if not set or if this is a newer term
    if (!this.leader || (message.term > (this.stats.leaderTerm || 0))) {
      this.leader = fromNodeId;
      this.stats.leaderTerm = message.term;
      
      const node = this.nodes.get(fromNodeId);
      if (node) {
        node.role = 'leader';
      }
      
      // If we were leader, step down
      if (this.options.nodeId === this.leader) {
        this._stepDownAsLeader();
      }
      
      this.emit('leaderUpdated', {
        leader: fromNodeId,
        term: message.term,
        timestamp: Date.now()
      });
    }
    
    // Reset election timeout since we received a leader heartbeat
    if (this.electionTimeout) {
      clearTimeout(this.electionTimeout);
      this.electionTimeout = setTimeout(() => {
        if (this.leader !== this.options.nodeId) {
          this._startElection();
        }
      }, this.options.electionTimeout);
    }
  }
  
  /**
   * Handle election message
   * @private
   */
  _handleElection(fromNodeId, message) {
    // Check if we should vote for this node
    const currentTerm = this.stats.leaderTerm || 0;
    
    if (message.term > currentTerm) {
      // Vote for the candidate
      const voteMessage = {
        type: 'vote',
        nodeId: this.options.nodeId,
        timestamp: Date.now(),
        term: message.term,
        votedFor: fromNodeId
      };
      
      // Simulate sending vote back
      setTimeout(() => {
        this._handleMessage(this.options.nodeId, {
          ...voteMessage,
          fromNodeId: this.options.nodeId
        });
      }, Math.random() * 50);
      
      this.emit('voteSent', {
        toNodeId: fromNodeId,
        term: message.term,
        timestamp: Date.now()
      });
    }
  }
  
  /**
   * Handle vote message
   * @private
   */
  _handleVote(fromNodeId, message) {
    if (this.electionInProgress && message.votedFor === this.options.nodeId) {
      this.votesReceived.add(fromNodeId);
      
      this.emit('voteReceived', {
        fromNodeId,
        term: message.term,
        timestamp: Date.now(),
        totalVotes: this.votesReceived.size
      });
    }
  }
  
  /**
   * Handle leader announcement
   * @private
   */
  _handleLeaderAnnouncement(fromNodeId, message) {
    this.leader = fromNodeId;
    this.stats.leaderTerm = message.term;
    
    const node = this.nodes.get(fromNodeId);
    if (node) {
      node.role = 'leader';
    }
    
    // Cancel any ongoing election
    this.electionInProgress = false;
    if (this.electionTimeout) {
      clearTimeout(this.electionTimeout);
      this.electionTimeout = null;
    }
    
    this.emit('leaderAnnounced', {
      leader: fromNodeId,
      term: message.term,
      timestamp: Date.now()
    });
  }
  
  /**
   * Handle leave message
   * @private
   */
  _handleLeave(fromNodeId, message) {
    if (this.nodes.has(fromNodeId)) {
      const node = this.nodes.get(fromNodeId);
      node.status = 'left';
      node.leftAt = Date.now();
      
      this.nodes.delete(fromNodeId);
      
      this.emit('nodeLeft', {
        nodeId: fromNodeId,
        node,
        timestamp: Date.now()
      });
      
      
      // If leaving node was leader, start new election
      if (this.leader === fromNodeId) {
        this.leader = null;
        this._startElection();
      }
      
      this._updateStats();
    }
  }
  
  /**
   * Add a new node to the cluster
   * @param {Object} nodeConfig - Node configuration
   * @returns {boolean} True if node was added successfully
   */
  addNode(nodeConfig) {
    if (!nodeConfig.id) {
      throw new Error('Node ID is required');
    }
    
    if (this.nodes.has(nodeConfig.id)) {
      return false;
    }
    
    const node = {
      id: nodeConfig.id,
      status: 'active',
      role: 'follower',
      lastSeen: Date.now(),
      metadata: nodeConfig.metadata || {},
      ...nodeConfig
    };
    
    this.nodes.set(nodeConfig.id, node);
    
    // Notify other nodes about new node
    const newNodeMessage = {
      type: 'new-node',
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      newNode: node
    };
    
    this._broadcastMessage(newNodeMessage);
    
    this.emit('nodeAdded', {
      nodeId: nodeConfig.id,
      node,
      timestamp: Date.now()
    });
    
    this._updateStats();
    
    return true;
  }
  
  /**
   * Remove a node from the cluster
   * @param {string} nodeId - Node ID to remove
   * @returns {boolean} True if node was removed
   */
  removeNode(nodeId) {
    if (nodeId === this.options.nodeId) {
      throw new Error('Cannot remove self from cluster');
    }
    
    if (!this.nodes.has(nodeId)) {
      return false;
    }
    
    const node = this.nodes.get(nodeId);
    this.nodes.delete(nodeId);
    
    // Notify other nodes about removed node
    const removeNodeMessage = {
      type: 'remove-node',
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      removedNodeId: nodeId
    };
    
    this._broadcastMessage(removeNodeMessage);
    
    this.emit('nodeRemoved', {
      nodeId,
      node,
      timestamp: Date.now()
    });
    
    // If removed node was leader, start new election
    if (this.leader === nodeId) {
      this.leader = null;
      this._startElection();
    }
    
    this._updateStats();
    
    return true;
  }
  
  /**
   * Get node information by ID
   * @param {string} nodeId - Node ID
   * @returns {Object|null} Node information or null if not found
   */
  getNode(nodeId) {
    const node = this.nodes.get(nodeId);
    if (!node) {
      return null;
    }
    
    return {
      ...node,
      isSelf: nodeId === this.options.nodeId,
      isLeader: nodeId === this.leader,
      timeSinceLastSeen: Date.now() - node.lastSeen
    };
  }
  
  /**
   * Get all nodes in the cluster
   * @returns {Array} Array of node information
   */
  getAllNodes() {
    const nodes = [];
    for (const [nodeId, node] of this.nodes.entries()) {
      nodes.push({
        ...node,
        isSelf: nodeId === this.options.nodeId,
        isLeader: nodeId === this.leader,
        timeSinceLastSeen: Date.now() - node.lastSeen
      });
    }
    return nodes;
  }
  
  /**
   * Get cluster leader information
   * @returns {Object|null} Leader information or null if no leader
   */
  getLeader() {
    if (!this.leader) {
      return null;
    }
    
    const node = this.nodes.get(this.leader);
    if (!node) {
      return null;
    }
    
    return {
      ...node,
      isSelf: this.leader === this.options.nodeId,
      term: this.stats.leaderTerm,
      leaderSince: this.stats.leaderSince
    };
  }
  
  /**
   * Check if current node is the leader
   * @returns {boolean} True if current node is leader
   */
  isLeader() {
    return this.leader === this.options.nodeId;
  }
  
  /**
   * Get cluster statistics
   * @returns {Object} Cluster statistics
   */
  getStats() {
    const now = Date.now();
    const activeNodes = Array.from(this.nodes.values()).filter(n => n.status === 'active').length;
    const failedNodes = Array.from(this.nodes.values()).filter(n => n.status === 'failed').length;
    
    return {
      ...this.stats,
      nodeId: this.options.nodeId,
      status: this.status,
      leader: this.leader,
      totalNodes: this.nodes.size,
      activeNodes,
      failedNodes,
      uptime: this.stats.startedAt ? now - this.stats.startedAt : 0,
      electionInProgress: this.electionInProgress,
      votesReceived: this.votesReceived.size,
      isLeader: this.isLeader(),
      timestamp: now
    };
  }
  
  /**
   * Update cluster statistics
   * @private
   */
  _updateStats() {
    const now = Date.now();
    const activeNodes = Array.from(this.nodes.values()).filter(n => n.status === 'active').length;
    const failedNodes = Array.from(this.nodes.values()).filter(n => n.status === 'failed').length;
    
    this.stats = {
      ...this.stats,
      totalNodes: this.nodes.size,
      activeNodes,
      failedNodes,
      uptime: this.stats.startedAt ? now - this.stats.startedAt : 0,
      lastUpdated: now
    };
  }
  
  /**
   * Distribute work across cluster nodes
   * @param {string} workType - Type of work to distribute
   * @param {Object} workData - Work data to distribute
   * @param {Object} options - Distribution options
   * @returns {Promise<Object>} Distribution results
   */
  async distributeWork(workType, workData, options = {}) {
    if (!this.isLeader()) {
      throw new Error('Only the leader can distribute work');
    }
    
    const distribution = {
      workType,
      workData,
      options,
      distributedBy: this.options.nodeId,
      timestamp: Date.now(),
      distributionId: `dist_${crypto.randomBytes(8).toString('hex')}`
    };
    
    const activeNodes = Array.from(this.nodes.values())
      .filter(node => node.status === 'active' && node.id !== this.options.nodeId);
    
    if (activeNodes.length === 0) {
      return {
        distributionId: distribution.distributionId,
        status: 'no_workers',
        message: 'No active worker nodes available'
      };
    }
    
    // Simple round-robin distribution
    // In a real implementation, this would be more sophisticated
    const distributionResults = [];
    
    for (const node of activeNodes) {
      const nodeWork = {
        ...distribution,
        assignedTo: node.id,
        workSlice: this._sliceWork(workData, activeNodes.length)
      };
      
      distributionResults.push({
        nodeId: node.id,
        work: nodeWork,
        status: 'assigned'
      });
      
      // Simulate sending work to node
      this.emit('workDistributed', {
        distributionId: distribution.distributionId,
        nodeId: node.id,
        workType,
        timestamp: Date.now()
      });
    }
    
    return {
      distributionId: distribution.distributionId,
      status: 'distributed',
      totalNodes: activeNodes.length,
      results: distributionResults
    };
  }
  
  /**
   * Slice work for distribution
   * @private
   */
  _sliceWork(workData, slices) {
    // Simple work slicing logic
    // In a real implementation, this would be more sophisticated
    if (Array.isArray(workData)) {
      const sliceSize = Math.ceil(workData.length / slices);
      return workData.slice(0, sliceSize);
    }
    
    return workData;
  }
  
  /**
   * Register a queue with the cluster
   * @param {string} queueName - Queue name
   * @param {Object} queueInstance - Queue instance
   */
  registerQueue(queueName, queueInstance) {
    const registration = {
      type: 'queue-registration',
      queueName,
      nodeId: this.options.nodeId,
      timestamp: Date.now(),
      queueInfo: {
        driver: queueInstance.driverType,
        status: queueInstance.status || 'active'
      }
    };
    
    this._broadcastMessage(registration);
    
    this.emit('queueRegistered', {
      queueName,
      nodeId: this.options.nodeId,
      timestamp: Date.now()
    });
  }
  
  /**
   * Unregister a queue from the cluster
   * @param {string} queueName - Queue name
   */
  unregisterQueue(queueName) {
    const unregistration = {
      type: 'queue-unregistration',
      queueName,
      nodeId: this.options.nodeId,
      timestamp: Date.now()
    };
    
    this._broadcastMessage(unregistration);
    
    this.emit('queueUnregistered', {
      queueName,
      nodeId: this.options.nodeId,
      timestamp: Date.now()
    });
  }
}
