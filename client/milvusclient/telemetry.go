// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// TelemetryConfig holds configurable settings for client telemetry
type TelemetryConfig struct {
	// Enabled controls whether telemetry collection is active
	Enabled bool
	// HeartbeatInterval is how often to send heartbeats to server (default: 30 seconds)
	// Snapshot period aligns with heartbeat interval
	HeartbeatInterval time.Duration
	// SamplingRate is the sampling rate for all operations (0.0-1.0, default: 1.0 = 100%)
	// Can be dynamically adjusted
	SamplingRate float64
	// ErrorMaxCount is the maximum number of errors to keep
	ErrorMaxCount int
}

// DefaultTelemetryConfig returns the default telemetry configuration
func DefaultTelemetryConfig() *TelemetryConfig {
	return &TelemetryConfig{
		Enabled:           true,
		HeartbeatInterval: 30 * time.Second,
		SamplingRate:      1.0, // 100% sampling by default
		ErrorMaxCount:     100,
	}
}

// Metrics holds aggregated metrics for operations (local type for internal use)
type Metrics struct {
	RequestCount int64
	SuccessCount int64
	ErrorCount   int64
	AvgLatencyMs float64
	P99LatencyMs float64
	MaxLatencyMs float64
}

// OperationMetrics holds metrics for a specific operation type (local type for internal use)
type OperationMetrics struct {
	Operation         string
	Global            *Metrics
	CollectionMetrics map[string]*Metrics
}

// ClientCommand represents a command from the server (local type for command handler)
type ClientCommand struct {
	CommandId   string
	CommandType string
	Payload     []byte
	CreateTime  int64
	Persistent  bool
	TargetScope string
}

// CommandReply represents a reply to a server command (local type for command handler)
type CommandReply struct {
	CommandId    string
	Success      bool
	ErrorMessage string
	Payload      []byte
}

// ErrorInfo stores error details for client-side error tracking
type ErrorInfo struct {
	Timestamp  int64  `json:"timestamp"`            // Unix timestamp in milliseconds
	Operation  string `json:"operation"`            // Operation that failed (e.g., "Search", "Insert")
	ErrorMsg   string `json:"error_msg"`            // Error message
	Collection string `json:"collection,omitempty"` // Collection name (optional)
	RequestID  string `json:"request_id,omitempty"` // Request ID for tracing
}

// ErrorCollectorImpl implements error collection for client operations
type ErrorCollectorImpl struct {
	mu       sync.RWMutex
	errors   []*ErrorInfo
	maxCount int
	index    int // Ring buffer index
}

// NewErrorCollector creates a new error collector
func NewErrorCollector(maxCount int) *ErrorCollectorImpl {
	// Ensure maxCount is at least 1 to avoid empty slice
	if maxCount <= 0 {
		maxCount = 100 // default to 100 if not specified
	}
	return &ErrorCollectorImpl{
		errors:   make([]*ErrorInfo, maxCount),
		maxCount: maxCount,
	}
}

// RecordError records an error in the circular buffer
func (ec *ErrorCollectorImpl) RecordError(operation, collection, errorMsg, requestID string) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	ec.errors[ec.index] = &ErrorInfo{
		Timestamp:  time.Now().UnixMilli(),
		Operation:  operation,
		ErrorMsg:   errorMsg,
		Collection: collection,
		RequestID:  requestID,
	}
	ec.index = (ec.index + 1) % ec.maxCount
}

// GetRecentErrors returns the most recent errors (up to maxCount)
func (ec *ErrorCollectorImpl) GetRecentErrors(maxCount int) []*ErrorInfo {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	var result []*ErrorInfo

	// Start from most recent (index-1) and go backwards
	for i := 0; i < ec.maxCount && len(result) < maxCount; i++ {
		idx := (ec.index - 1 - i + ec.maxCount) % ec.maxCount
		err := ec.errors[idx]
		if err == nil {
			continue // Empty slot
		}

		result = append(result, err)
	}

	return result
}

// OperationMetricsCollector collects metrics for a single operation type
type OperationMetricsCollector struct {
	mu sync.RWMutex

	// Global metrics
	requestCount int64
	successCount int64
	errorCount   int64
	totalLatency int64 // in microseconds for precision
	maxLatency   int64 // max latency in microseconds

	// Latency samples for P99 calculation
	latencySamples []int64
	sampleIndex    int
	bufferSize     int
	// totalSamples tracks the count of recorded samples, allowing us to distinguish
	// genuine 0µs latencies from uninitialized buffer slots (which are also 0)
	totalSamples int64

	// Per-collection metrics
	collectionMetrics map[string]*CollectionMetrics
}

// CollectionMetrics holds metrics for a specific collection
type CollectionMetrics struct {
	requestCount   int64
	successCount   int64
	errorCount     int64
	totalLatency   int64
	maxLatency     int64 // max latency in microseconds
	latencySamples []int64
	sampleIndex    int
	bufferSize     int
	totalSamples   int64
}

// NewOperationMetricsCollector creates a new metrics collector
// Uses fixed latency sample buffer size of 1000 for P99 calculation
func NewOperationMetricsCollector() *OperationMetricsCollector {
	return &OperationMetricsCollector{
		latencySamples:    make([]int64, 1000),
		bufferSize:        1000,
		collectionMetrics: make(map[string]*CollectionMetrics),
	}
}

// Record records a single operation result
func (c *OperationMetricsCollector) Record(collection string, latencyUs int64, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requestCount++
	c.totalLatency += latencyUs

	// Track max latency
	if latencyUs > c.maxLatency {
		c.maxLatency = latencyUs
	}

	// Store latency sample for P99
	c.latencySamples[c.sampleIndex] = latencyUs
	c.sampleIndex = (c.sampleIndex + 1) % c.bufferSize
	c.totalSamples++

	if success {
		c.successCount++
	} else {
		c.errorCount++
	}

	// Per-collection metrics
	if collection != "" {
		cm, ok := c.collectionMetrics[collection]
		if !ok {
			cm = &CollectionMetrics{
				latencySamples: make([]int64, 1000),
				bufferSize:     1000,
			}
			c.collectionMetrics[collection] = cm
		}
		cm.requestCount++
		cm.totalLatency += latencyUs
		// Track max latency for collection
		if latencyUs > cm.maxLatency {
			cm.maxLatency = latencyUs
		}
		// Store latency sample for P99
		cm.latencySamples[cm.sampleIndex] = latencyUs
		cm.sampleIndex = (cm.sampleIndex + 1) % cm.bufferSize
		cm.totalSamples++
		if success {
			cm.successCount++
		} else {
			cm.errorCount++
		}
	}
}

// GetMetrics returns the current metrics and resets counters
// IMPORTANT: P99 is calculated here atomically before clearing the sample buffer
// This prevents the race condition where sendHeartbeat could calculate P99 from a cleared buffer
func (c *OperationMetricsCollector) GetMetrics() *Metrics {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.requestCount == 0 {
		return nil
	}

	avgLatency := float64(c.totalLatency) / float64(c.requestCount) / 1000.0
	// P99 is calculated HERE before totalSamples is reset to 0
	// This ensures accurate P99 even if sendHeartbeat is called during snapshot creation
	p99Latency := c.calculateP99() / 1000.0      // convert to ms
	maxLatency := float64(c.maxLatency) / 1000.0 // convert to ms

	metrics := &Metrics{
		RequestCount: c.requestCount,
		SuccessCount: c.successCount,
		ErrorCount:   c.errorCount,
		AvgLatencyMs: avgLatency,
		P99LatencyMs: p99Latency,
		MaxLatencyMs: maxLatency,
	}

	// Reset counters for next period
	c.requestCount = 0
	c.successCount = 0
	c.errorCount = 0
	c.totalLatency = 0
	c.maxLatency = 0
	c.sampleIndex = 0
	c.totalSamples = 0

	return metrics
}

// GetMetricsSnapshot returns current metrics WITHOUT resetting counters
func (c *OperationMetricsCollector) GetMetricsSnapshot() *Metrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.requestCount == 0 {
		return nil
	}

	avgLatency := float64(c.totalLatency) / float64(c.requestCount) / 1000.0 // convert to ms
	p99Latency := c.calculateP99() / 1000.0                                  // convert to ms
	maxLatency := float64(c.maxLatency) / 1000.0                             // convert to ms

	return &Metrics{
		RequestCount: c.requestCount,
		SuccessCount: c.successCount,
		ErrorCount:   c.errorCount,
		AvgLatencyMs: avgLatency,
		P99LatencyMs: p99Latency,
		MaxLatencyMs: maxLatency,
	}
}

// GetCollectionMetrics returns per-collection metrics
func (c *OperationMetricsCollector) GetCollectionMetrics() map[string]*Metrics {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make(map[string]*Metrics)
	for collection, cm := range c.collectionMetrics {
		if cm.requestCount == 0 {
			continue
		}
		avgLatency := float64(cm.totalLatency) / float64(cm.requestCount) / 1000.0
		p99Latency := calculateP99FromSamples(cm.latencySamples, cm.totalSamples, cm.bufferSize) / 1000.0
		maxLatency := float64(cm.maxLatency) / 1000.0
		result[collection] = &Metrics{
			RequestCount: cm.requestCount,
			SuccessCount: cm.successCount,
			ErrorCount:   cm.errorCount,
			AvgLatencyMs: avgLatency,
			P99LatencyMs: p99Latency,
			MaxLatencyMs: maxLatency,
		}
	}

	// Reset collection metrics
	c.collectionMetrics = make(map[string]*CollectionMetrics)

	return result
}

// calculateP99FromSamples calculates P99 latency from a samples buffer
func calculateP99FromSamples(samples []int64, totalSamples int64, bufferSize int) float64 {
	if totalSamples == 0 {
		return 0
	}

	count := int(totalSamples)
	if count > bufferSize {
		count = bufferSize
	}

	sorted := make([]int64, count)
	if totalSamples <= int64(bufferSize) {
		copy(sorted, samples[:count])
	} else {
		copy(sorted, samples)
	}

	// Sort using standard library (introsort, more efficient for larger buffers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	index := int(float64(len(sorted)) * 0.99)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return float64(sorted[index])
}

// GetP99Latency calculates P99 latency from current samples (in milliseconds)
// Used primarily for testing; in production, P99 is computed in GetMetrics() during snapshot creation
func (c *OperationMetricsCollector) GetP99Latency() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.calculateP99() / 1000.0 // convert to ms
}

// calculateP99 calculates P99 latency from samples (caller must hold lock)
// Uses totalSamples counter to distinguish genuine 0µs latencies from uninitialized buffer slots
func (c *OperationMetricsCollector) calculateP99() float64 {
	if c.totalSamples == 0 {
		return 0
	}

	// Determine number of valid samples
	// When buffer wraps (totalSamples > bufferSize), use only the most recent bufferSize samples
	count := int(c.totalSamples)
	if count > c.bufferSize {
		count = c.bufferSize
	}

	// Copy valid samples
	sorted := make([]int64, count)
	if c.totalSamples <= int64(c.bufferSize) {
		// Buffer not full, samples are 0 to count-1
		copy(sorted, c.latencySamples[:count])
	} else {
		// Buffer full (wrapped), take all
		copy(sorted, c.latencySamples)
	}

	// Sort using standard library (introsort, more efficient for larger buffers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	// Calculate P99
	index := int(float64(len(sorted)) * 0.99)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return float64(sorted[index])
}

// ClientTelemetryManager manages client-side telemetry
type ClientTelemetryManager struct {
	config   *TelemetryConfig
	configMu sync.RWMutex // Protects config access
	client   *Client

	// Unique client ID (UUID), generated once at startup, stable across reconnections
	clientID string

	// Metrics collectors per operation
	mu         sync.RWMutex
	collectors map[string]*OperationMetricsCollector

	// Historical snapshots
	snapshotsMu sync.RWMutex
	snapshots   []*MetricsSnapshot

	// State tracking
	configHash           string
	configHashMu         sync.RWMutex // Protects configHash access
	pendingReplies       []*commonpb.CommandReply
	pendingRepliesMu     sync.Mutex
	lastCommandTimestamp atomic.Int64

	// Command handlers
	commandHandlers   map[string]CommandHandler
	commandHandlersMu sync.RWMutex

	// Executed commands tracking - prevents duplicate execution of commands
	// Key: command ID, Value: CreateTime of the command
	// Commands with CreateTime < lastCommandTimestamp are filtered by timestamp comparison
	// This map only tracks commands at or after lastCommandTimestamp for same-millisecond deduplication
	executedCommands   map[string]int64
	executedCommandsMu sync.RWMutex

	// Collection-level metrics tracking
	enabledCollections    map[string]bool
	allCollectionsEnabled bool // When true, all collections are enabled (wildcard "*")
	enabledCollectionsMu  sync.RWMutex

	// Error collection for show_errors command
	errorCollector *ErrorCollectorImpl

	// Background goroutines
	stopCh chan struct{}
	wg     sync.WaitGroup
	closed atomic.Bool

	// Startup state - indicates if telemetry manager has started
	ready atomic.Bool

	// Deterministic sampling counter
	samplingCounter uint64
}

// CommandHandler handles a specific command type from the server
type CommandHandler func(cmd *ClientCommand) *CommandReply

// MetricsSnapshot represents a snapshot of metrics at a specific time
type MetricsSnapshot struct {
	Timestamp int64               // Unix timestamp in milliseconds (start of snapshot period)
	EndTime   int64               // Unix timestamp in milliseconds (end of snapshot period)
	Metrics   []*OperationMetrics // Metrics for all operations
}

// NewClientTelemetryManager creates a new client telemetry manager
func NewClientTelemetryManager(client *Client, config *TelemetryConfig) *ClientTelemetryManager {
	if config == nil {
		config = DefaultTelemetryConfig()
	}

	tm := &ClientTelemetryManager{
		config:             config,
		client:             client,
		clientID:           uuid.New().String(),
		collectors:         make(map[string]*OperationMetricsCollector),
		commandHandlers:    make(map[string]CommandHandler),
		executedCommands:   make(map[string]int64),
		enabledCollections: make(map[string]bool),
		errorCollector:     NewErrorCollector(config.ErrorMaxCount),
		stopCh:             make(chan struct{}),
	}

	tm.registerDefaultHandlers()
	return tm
}

// Start starts the background heartbeat goroutine.
// Sends first heartbeat immediately, then every HeartbeatInterval.
func (m *ClientTelemetryManager) Start() {
	m.configMu.RLock()
	enabled := m.config.Enabled
	m.configMu.RUnlock()

	if !enabled {
		m.ready.Store(true) // Mark ready even if disabled
		return
	}

	// Mark as ready immediately - no blocking initial heartbeat
	m.ready.Store(true)

	// Start background heartbeat loop (snapshot creation is done inside heartbeatLoop)
	m.wg.Add(1)
	go m.heartbeatLoop()
}

// IsReady returns true if the client has completed initial setup
func (m *ClientTelemetryManager) IsReady() bool {
	return m.ready.Load()
}

func (m *ClientTelemetryManager) buildClientInfo() *commonpb.ClientInfo {
	hostname, _ := os.Hostname()
	clientInfo := &commonpb.ClientInfo{
		SdkType:    "GoMilvusClient",
		SdkVersion: common.SDKVersion,
		LocalTime:  time.Now().String(),
		Host:       hostname,
		Reserved: map[string]string{
			"client_id": m.clientID, // Always send stable UUID to prevent ID collisions
		},
	}
	if m.client != nil {
		if m.client.config != nil {
			clientInfo.User = m.client.config.Username
		}
		if dbName := m.client.getCurrentDB(); dbName != "" {
			clientInfo.Reserved["db_name"] = dbName
		}
	}
	return clientInfo
}

// Stop stops the background heartbeat goroutine
func (m *ClientTelemetryManager) Stop() {
	if m.closed.Swap(true) {
		return // already closed
	}
	close(m.stopCh)
	m.wg.Wait()
}

// heartbeatLoop runs the background heartbeat
// Sends first heartbeat immediately on start, then every HeartbeatInterval
// The loop dynamically adapts to HeartbeatInterval changes from server-pushed config
func (m *ClientTelemetryManager) heartbeatLoop() {
	defer m.wg.Done()

	// Send first heartbeat immediately
	m.createSnapshot()
	m.sendHeartbeat()

	// Use time.After instead of ticker to dynamically adapt to interval changes
	// This allows server-pushed config to take effect immediately
	for {
		interval := m.getHeartbeatInterval()
		select {
		case <-m.stopCh:
			return
		case <-time.After(interval):
			m.createSnapshot()
			m.sendHeartbeat()
		}
	}
}

// sendHeartbeat sends a heartbeat to the server
func (m *ClientTelemetryManager) sendHeartbeat() {
	m.configMu.RLock()
	enabled := m.config.Enabled
	m.configMu.RUnlock()

	if !enabled {
		return
	}

	if m.client == nil || m.client.service == nil {
		return
	}

	// Get metrics from the latest snapshot (P99 already calculated during snapshot creation)
	var metrics []*commonpb.OperationMetrics
	latestSnapshot := m.GetLatestSnapshot()
	if latestSnapshot != nil {
		enabledCollections, allEnabled := m.snapshotEnabledCollections()

		// Convert snapshot metrics to proto format
		for _, opMetrics := range latestSnapshot.Metrics {
			protoCollMetrics := make(map[string]*commonpb.Metrics)
			// Only include metrics for enabled collections
			// Use "*" wildcard to enable all collections
			for coll, cm := range opMetrics.CollectionMetrics {
				if allEnabled || enabledCollections[coll] {
					protoCollMetrics[coll] = &commonpb.Metrics{
						RequestCount: cm.RequestCount,
						SuccessCount: cm.SuccessCount,
						ErrorCount:   cm.ErrorCount,
						AvgLatencyMs: cm.AvgLatencyMs,
						P99LatencyMs: cm.P99LatencyMs,
					}
				}
			}

			metrics = append(metrics, &commonpb.OperationMetrics{
				Operation: opMetrics.Operation,
				Global: &commonpb.Metrics{
					RequestCount: opMetrics.Global.RequestCount,
					SuccessCount: opMetrics.Global.SuccessCount,
					ErrorCount:   opMetrics.Global.ErrorCount,
					AvgLatencyMs: opMetrics.Global.AvgLatencyMs,
					P99LatencyMs: opMetrics.Global.P99LatencyMs, // Use P99 from snapshot
				},
				CollectionMetrics: protoCollMetrics,
			})
		}
	}

	// Get pending command replies (snapshot only)
	replies := m.getPendingProtoRepliesSnapshot()

	clientInfo := m.buildClientInfo()

	// Build request
	m.configHashMu.RLock()
	configHash := m.configHash
	m.configHashMu.RUnlock()

	req := &milvuspb.ClientHeartbeatRequest{
		ClientInfo:           clientInfo,
		ReportTimestamp:      time.Now().UnixMilli(),
		Metrics:              metrics,
		CommandReplies:       replies,
		ConfigHash:           configHash,
		LastCommandTimestamp: m.lastCommandTimestamp.Load(),
	}

	// Send heartbeat with 30s fixed interval (no retry - telemetry is best effort)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := m.client.telemetryService.ClientHeartbeat(ctx, req)
	if err != nil {
		// Log error but continue - telemetry is best-effort
		return
	}

	if !merr.Ok(resp.GetStatus()) {
		return
	}

	// Clear sent replies only after successful heartbeat
	m.clearPendingProtoReplies(len(replies))

	// Process commands from server
	m.processProtoCommands(resp.GetCommands())
}

func (m *ClientTelemetryManager) getHeartbeatInterval() time.Duration {
	m.configMu.RLock()
	interval := m.config.HeartbeatInterval
	m.configMu.RUnlock()
	if interval <= 0 {
		return 30 * time.Second
	}
	return interval
}

// snapshotEnabledCollections returns a snapshot of enabled collections config.
// Returns (nil, true) when all collections are enabled via wildcard "*".
// Returns (map, false) when specific collections are enabled.
// Returns (nil, false) when no collections are enabled.
func (m *ClientTelemetryManager) snapshotEnabledCollections() (map[string]bool, bool) {
	m.enabledCollectionsMu.RLock()
	defer m.enabledCollectionsMu.RUnlock()

	if m.allCollectionsEnabled {
		return nil, true // All collections enabled
	}

	if len(m.enabledCollections) == 0 {
		return nil, false // No collections enabled
	}

	snapshot := make(map[string]bool, len(m.enabledCollections))
	for coll, enabled := range m.enabledCollections {
		snapshot[coll] = enabled
	}
	return snapshot, false
}

const samplingDenominator = 10000

func (m *ClientTelemetryManager) shouldSample(samplingRate float64) bool {
	if samplingRate >= 1.0 {
		return true
	}
	if samplingRate <= 0.0 {
		return false
	}

	threshold := uint64(samplingRate * float64(samplingDenominator))
	if threshold == 0 {
		return false
	}

	counter := atomic.AddUint64(&m.samplingCounter, 1)
	return counter%samplingDenominator < threshold
}

// collectProtoMetrics collects all operation metrics and converts to proto format
func (m *ClientTelemetryManager) collectProtoMetrics() []*commonpb.OperationMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	enabledCollections, allEnabled := m.snapshotEnabledCollections()

	var result []*commonpb.OperationMetrics
	for opName, collector := range m.collectors {
		globalMetrics := collector.GetMetrics()
		if globalMetrics == nil {
			continue
		}

		collMetrics := collector.GetCollectionMetrics()

		protoCollMetrics := make(map[string]*commonpb.Metrics)
		// Only include metrics for enabled collections
		// Use "*" wildcard to enable all collections
		for coll, cm := range collMetrics {
			if allEnabled || enabledCollections[coll] {
				protoCollMetrics[coll] = &commonpb.Metrics{
					RequestCount: cm.RequestCount,
					SuccessCount: cm.SuccessCount,
					ErrorCount:   cm.ErrorCount,
					AvgLatencyMs: cm.AvgLatencyMs,
					P99LatencyMs: cm.P99LatencyMs,
				}
			}
		}

		result = append(result, &commonpb.OperationMetrics{
			Operation: opName,
			Global: &commonpb.Metrics{
				RequestCount: globalMetrics.RequestCount,
				SuccessCount: globalMetrics.SuccessCount,
				ErrorCount:   globalMetrics.ErrorCount,
				AvgLatencyMs: globalMetrics.AvgLatencyMs,
				P99LatencyMs: globalMetrics.P99LatencyMs,
			},
			CollectionMetrics: protoCollMetrics,
		})
	}

	return result
}

// getPendingProtoRepliesSnapshot returns a snapshot of pending replies without clearing.
// Clearing is done only after a successful heartbeat to avoid losing replies on failures.
func (m *ClientTelemetryManager) getPendingProtoRepliesSnapshot() []*commonpb.CommandReply {
	m.pendingRepliesMu.Lock()
	defer m.pendingRepliesMu.Unlock()

	if len(m.pendingReplies) == 0 {
		return nil
	}
	replies := make([]*commonpb.CommandReply, len(m.pendingReplies))
	copy(replies, m.pendingReplies)
	return replies
}

// clearPendingProtoReplies removes the oldest sent replies after a successful heartbeat.
func (m *ClientTelemetryManager) clearPendingProtoReplies(sentCount int) {
	if sentCount <= 0 {
		return
	}
	m.pendingRepliesMu.Lock()
	defer m.pendingRepliesMu.Unlock()

	if sentCount >= len(m.pendingReplies) {
		m.pendingReplies = nil
		return
	}
	m.pendingReplies = m.pendingReplies[sentCount:]
}

// processProtoCommands processes commands received from the server (proto format)
// Commands are only executed once using timestamp-based deduplication:
// - Commands with CreateTime < lastCommandTimestamp are filtered by timestamp (already processed)
// - Commands with CreateTime >= lastCommandTimestamp use ID-based tracking for same-millisecond deduplication
func (m *ClientTelemetryManager) processProtoCommands(commands []*commonpb.ClientCommand) {
	hasPersistent := false
	lastTS := m.lastCommandTimestamp.Load()
	maxCommandTS := lastTS

	// First, process all commands
	for _, cmd := range commands {
		localCmd := &ClientCommand{
			CommandId:   cmd.GetCommandId(),
			CommandType: cmd.GetCommandType(),
			Payload:     cmd.GetPayload(),
			CreateTime:  cmd.GetCreateTime(),
			Persistent:  cmd.GetPersistent(),
			TargetScope: cmd.GetTargetScope(),
		}

		if localCmd.Persistent {
			hasPersistent = true
		}
		if cmd.GetCreateTime() > maxCommandTS {
			maxCommandTS = cmd.GetCreateTime()
		}

		// Timestamp-based deduplication: commands older than lastTS are already processed
		if cmd.GetCreateTime() < lastTS {
			// Already processed in a previous cycle - send ACK but skip execution
			m.pendingRepliesMu.Lock()
			m.pendingReplies = append(m.pendingReplies, &commonpb.CommandReply{
				CommandId: localCmd.CommandId,
				Success:   true,
			})
			m.pendingRepliesMu.Unlock()
			continue
		}

		// For commands at or after lastTS, check map for same-millisecond duplicates
		m.executedCommandsMu.RLock()
		_, alreadyExecuted := m.executedCommands[localCmd.CommandId]
		m.executedCommandsMu.RUnlock()

		if alreadyExecuted {
			// Skip execution but still generate a success reply
			// This ensures server knows the command was received (idempotent ACK)
			m.pendingRepliesMu.Lock()
			m.pendingReplies = append(m.pendingReplies, &commonpb.CommandReply{
				CommandId: localCmd.CommandId,
				Success:   true,
			})
			m.pendingRepliesMu.Unlock()
			continue
		}

		// Handle the command
		reply := m.handleCommand(localCmd)

		// Track command with its timestamp for later cleanup
		m.executedCommandsMu.Lock()
		m.executedCommands[localCmd.CommandId] = cmd.GetCreateTime()
		m.executedCommandsMu.Unlock()

		if reply != nil {
			m.pendingRepliesMu.Lock()
			m.pendingReplies = append(m.pendingReplies, &commonpb.CommandReply{
				CommandId:    reply.CommandId,
				Success:      reply.Success,
				ErrorMessage: reply.ErrorMessage,
				Payload:      reply.Payload,
			})
			m.pendingRepliesMu.Unlock()
		}
	}

	// Clean up old entries from executedCommands map
	// Commands with CreateTime <= lastTS are now filtered by timestamp comparison
	// Using <= ensures commands with same millisecond timestamp are also cleaned up
	m.executedCommandsMu.Lock()
	for cmdID, ts := range m.executedCommands {
		if ts <= lastTS {
			delete(m.executedCommands, cmdID)
		}
	}
	m.executedCommandsMu.Unlock()

	// Update config hash AFTER all commands are processed
	// This ensures partial processing doesn't lead to lost configs on reconnect
	if hasPersistent {
		m.configHashMu.Lock()
		m.configHash = m.calculateProtoConfigHash(commands)
		m.configHashMu.Unlock()
	}

	m.updateLastCommandTimestamp(maxCommandTS)
}

// calculateProtoConfigHash calculates a hash for persistent commands (proto format)
// Includes payload in hash to detect configuration changes
func (m *ClientTelemetryManager) calculateProtoConfigHash(commands []*commonpb.ClientCommand) string {
	if len(commands) == 0 {
		return ""
	}

	var persistentCmds []*commonpb.ClientCommand
	for _, cmd := range commands {
		if cmd.GetPersistent() {
			persistentCmds = append(persistentCmds, cmd)
		}
	}

	if len(persistentCmds) == 0 {
		return ""
	}

	// Sort by command ID to ensure consistent ordering
	sort.Slice(persistentCmds, func(i, j int) bool {
		return persistentCmds[i].GetCommandId() < persistentCmds[j].GetCommandId()
	})

	h := sha256.New()
	for _, cmd := range persistentCmds {
		// Include command ID, type, AND payload in hash
		h.Write([]byte(cmd.GetCommandId()))
		h.Write([]byte(cmd.GetCommandType()))
		h.Write(cmd.GetPayload()) // Include payload to detect configuration changes
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func (m *ClientTelemetryManager) updateLastCommandTimestamp(ts int64) {
	if ts <= 0 {
		return
	}
	for {
		current := m.lastCommandTimestamp.Load()
		if ts <= current {
			return
		}
		if m.lastCommandTimestamp.CompareAndSwap(current, ts) {
			return
		}
	}
}

// collectMetrics collects all operation metrics (local types, for testing)
func (m *ClientTelemetryManager) collectMetrics() []*OperationMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*OperationMetrics
	for opName, collector := range m.collectors {
		globalMetrics := collector.GetMetrics()
		if globalMetrics == nil {
			continue
		}

		collMetrics := collector.GetCollectionMetrics()

		result = append(result, &OperationMetrics{
			Operation:         opName,
			Global:            globalMetrics,
			CollectionMetrics: collMetrics,
		})
	}

	return result
}

// getPendingReplies gets and clears pending command replies (local types, for testing)
func (m *ClientTelemetryManager) getPendingReplies() []*CommandReply {
	m.pendingRepliesMu.Lock()
	defer m.pendingRepliesMu.Unlock()

	var result []*CommandReply
	for _, r := range m.pendingReplies {
		result = append(result, &CommandReply{
			CommandId:    r.GetCommandId(),
			Success:      r.GetSuccess(),
			ErrorMessage: r.GetErrorMessage(),
			Payload:      r.GetPayload(),
		})
	}
	m.pendingReplies = nil
	return result
}

// processCommands processes commands (local types, for testing)
// Commands are only executed once using timestamp-based deduplication:
// - Commands with CreateTime < lastCommandTimestamp are filtered by timestamp (already processed)
// - Commands with CreateTime >= lastCommandTimestamp use ID-based tracking for same-millisecond deduplication
func (m *ClientTelemetryManager) processCommands(commands []*ClientCommand) {
	hasPersistent := false
	lastTS := m.lastCommandTimestamp.Load()
	maxCommandTS := lastTS

	// First, process all commands
	for _, cmd := range commands {
		if cmd.Persistent {
			hasPersistent = true
		}
		if cmd.CreateTime > maxCommandTS {
			maxCommandTS = cmd.CreateTime
		}

		// Timestamp-based deduplication: commands older than lastTS are already processed
		if cmd.CreateTime < lastTS {
			// Already processed in a previous cycle - skip
			continue
		}

		// For commands at or after lastTS, check map for same-millisecond duplicates
		m.executedCommandsMu.RLock()
		_, alreadyExecuted := m.executedCommands[cmd.CommandId]
		m.executedCommandsMu.RUnlock()

		if alreadyExecuted {
			// Skip execution but still generate a success reply
			continue
		}

		// Handle the command
		reply := m.handleCommand(cmd)

		// Track command with its timestamp for later cleanup
		m.executedCommandsMu.Lock()
		m.executedCommands[cmd.CommandId] = cmd.CreateTime
		m.executedCommandsMu.Unlock()

		if reply != nil {
			m.pendingRepliesMu.Lock()
			m.pendingReplies = append(m.pendingReplies, &commonpb.CommandReply{
				CommandId:    reply.CommandId,
				Success:      reply.Success,
				ErrorMessage: reply.ErrorMessage,
				Payload:      reply.Payload,
			})
			m.pendingRepliesMu.Unlock()
		}
	}

	// Clean up old entries from executedCommands map
	// Commands with CreateTime <= lastTS are now filtered by timestamp comparison
	// Using <= ensures commands with same millisecond timestamp are also cleaned up
	m.executedCommandsMu.Lock()
	for cmdID, ts := range m.executedCommands {
		if ts <= lastTS {
			delete(m.executedCommands, cmdID)
		}
	}
	m.executedCommandsMu.Unlock()

	// Update config hash AFTER all commands are processed
	// This ensures partial processing doesn't lead to lost configs on reconnect
	if hasPersistent {
		m.configHashMu.Lock()
		m.configHash = m.calculateConfigHash(commands)
		m.configHashMu.Unlock()
	}

	m.updateLastCommandTimestamp(maxCommandTS)
}

// handleCommand handles a single command
func (m *ClientTelemetryManager) handleCommand(cmd *ClientCommand) *CommandReply {
	m.commandHandlersMu.RLock()
	handler, ok := m.commandHandlers[cmd.CommandType]
	m.commandHandlersMu.RUnlock()

	if !ok {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "unknown command type: " + cmd.CommandType,
		}
	}

	return handler(cmd)
}

// calculateConfigHash calculates a hash for persistent commands (local types)
func (m *ClientTelemetryManager) calculateConfigHash(commands []*ClientCommand) string {
	if len(commands) == 0 {
		return ""
	}

	var commandStrs []string
	for _, cmd := range commands {
		if cmd.Persistent {
			commandStrs = append(commandStrs, cmd.CommandId+":"+cmd.CommandType)
		}
	}

	if len(commandStrs) == 0 {
		return ""
	}

	sort.Strings(commandStrs)

	h := sha256.New()
	for _, str := range commandStrs {
		h.Write([]byte(str))
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// RegisterCommandHandler registers a handler for a command type
func (m *ClientTelemetryManager) RegisterCommandHandler(cmdType string, handler CommandHandler) {
	m.commandHandlersMu.Lock()
	defer m.commandHandlersMu.Unlock()
	m.commandHandlers[cmdType] = handler
}

// HandlePushConfigCommand applies a push_config command payload.
func (m *ClientTelemetryManager) HandlePushConfigCommand(cmd *ClientCommand) *CommandReply {
	return m.handlePushConfig(cmd)
}

// HandleCollectionMetricsCommand handles collection-level metrics configuration.
func (m *ClientTelemetryManager) HandleCollectionMetricsCommand(cmd *ClientCommand) *CommandReply {
	return m.handleCollectionMetrics(cmd)
}

// HandleShowErrorsCommand handles the show_errors command to return last N errors.
func (m *ClientTelemetryManager) HandleShowErrorsCommand(cmd *ClientCommand) *CommandReply {
	return m.handleShowErrors(cmd)
}

// RecordOperation records an operation for telemetry
func (m *ClientTelemetryManager) RecordOperation(operation, collection string, startTime time.Time, err error) {
	m.configMu.RLock()
	enabled := m.config.Enabled
	samplingRate := m.config.SamplingRate
	m.configMu.RUnlock()

	if !enabled {
		return
	}

	if !m.shouldSample(samplingRate) {
		return
	}

	latencyUs := time.Since(startTime).Microseconds()
	success := err == nil

	m.mu.RLock()
	collector, ok := m.collectors[operation]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		collector, ok = m.collectors[operation]
		if !ok {
			collector = NewOperationMetricsCollector()
			m.collectors[operation] = collector
		}
		m.mu.Unlock()
	}

	// Check if collection-level metrics are enabled for this collection
	// By default, collection metrics are DISABLED. They must be explicitly enabled
	// via the collection_metrics command. Use "*" wildcard to enable all collections.
	collectionToRecord := ""
	if collection != "" {
		m.enabledCollectionsMu.RLock()
		enabled := m.allCollectionsEnabled || m.enabledCollections[collection]
		m.enabledCollectionsMu.RUnlock()

		if enabled {
			collectionToRecord = collection
		}
	}

	collector.Record(collectionToRecord, latencyUs, success)

	// Record error for last 100 errors tracking
	if err != nil && m.errorCollector != nil {
		m.errorCollector.RecordError(operation, collection, err.Error(), "")
	}
}

// RecordOperationWithRequestID records an operation with optional request ID for error tracing
func (m *ClientTelemetryManager) RecordOperationWithRequestID(operation, collection, requestID string, startTime time.Time, err error) {
	m.configMu.RLock()
	enabled := m.config.Enabled
	samplingRate := m.config.SamplingRate
	m.configMu.RUnlock()

	if !enabled {
		return
	}

	if !m.shouldSample(samplingRate) {
		return
	}

	latencyUs := time.Since(startTime).Microseconds()
	success := err == nil

	m.mu.RLock()
	collector, ok := m.collectors[operation]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		collector, ok = m.collectors[operation]
		if !ok {
			collector = NewOperationMetricsCollector()
			m.collectors[operation] = collector
		}
		m.mu.Unlock()
	}

	// Check if collection-level metrics are enabled for this collection
	// By default, collection metrics are DISABLED. They must be explicitly enabled
	// via the collection_metrics command. Use "*" wildcard to enable all collections.
	collectionToRecord := ""
	if collection != "" {
		m.enabledCollectionsMu.RLock()
		enabled := m.allCollectionsEnabled || m.enabledCollections[collection]
		m.enabledCollectionsMu.RUnlock()

		if enabled {
			collectionToRecord = collection
		}
	}

	collector.Record(collectionToRecord, latencyUs, success)

	// Record error for last 100 errors tracking
	if err != nil && m.errorCollector != nil {
		m.errorCollector.RecordError(operation, collection, err.Error(), requestID)
	}
}

// GetClientID returns the unique client ID (UUID)
func (m *ClientTelemetryManager) GetClientID() string {
	return m.clientID
}

// GetConfigHash returns the current config hash
func (m *ClientTelemetryManager) GetConfigHash() string {
	m.configHashMu.RLock()
	defer m.configHashMu.RUnlock()
	return m.configHash
}

// SetConfigHash sets the config hash (for testing)
func (m *ClientTelemetryManager) SetConfigHash(hash string) {
	m.configHashMu.Lock()
	defer m.configHashMu.Unlock()
	m.configHash = hash
}

// createSnapshot creates a new metrics snapshot and adds it to the history
// P99 is calculated atomically in collectMetrics() -> GetMetrics() -> calculateP99()
// This eliminates the race condition where sendHeartbeat could read from a cleared sample buffer
// Note: QPS calculation uses fixed heartbeat interval as window (configured via HeartbeatInterval)
func (m *ClientTelemetryManager) createSnapshot() {
	m.configMu.RLock()
	enabled := m.config.Enabled
	heartbeatInterval := m.config.HeartbeatInterval
	m.configMu.RUnlock()
	if !enabled {
		return
	}

	// Collect current metrics (and reset counters)
	// P99 is calculated here, before samples are cleared
	metrics := m.collectMetrics()

	now := time.Now().UnixMilli()
	snapshot := &MetricsSnapshot{
		Timestamp: now - heartbeatInterval.Milliseconds(), // Start of the snapshot period
		EndTime:   now,                                    // End of the snapshot period
		Metrics:   metrics,
	}

	// Add to snapshot list (keep only the most recent 120 = 1 hour at 30s intervals)
	m.snapshotsMu.Lock()
	m.snapshots = append(m.snapshots, snapshot)
	if len(m.snapshots) > 120 {
		m.snapshots = m.snapshots[len(m.snapshots)-120:]
	}
	m.snapshotsMu.Unlock()
}

// GetMetricsSnapshots returns all historical snapshots
func (m *ClientTelemetryManager) GetMetricsSnapshots() []*MetricsSnapshot {
	m.snapshotsMu.RLock()
	defer m.snapshotsMu.RUnlock()

	// Return a copy to avoid external modification
	result := make([]*MetricsSnapshot, len(m.snapshots))
	copy(result, m.snapshots)
	return result
}

// GetLatestSnapshot returns the most recent snapshot, or nil if none exists
func (m *ClientTelemetryManager) GetLatestSnapshot() *MetricsSnapshot {
	m.snapshotsMu.RLock()
	defer m.snapshotsMu.RUnlock()

	if len(m.snapshots) == 0 {
		return nil
	}
	return m.snapshots[len(m.snapshots)-1]
}

// registerDefaultHandlers registers default command handlers
func (m *ClientTelemetryManager) registerDefaultHandlers() {
	// Config handler - dynamically modify client telemetry configuration
	m.RegisterCommandHandler("push_config", m.handlePushConfig)

	// Collection metrics handler - enable/disable fine-grained collection metrics
	m.RegisterCommandHandler("collection_metrics", m.handleCollectionMetrics)

	// Show errors handler - return last N errors
	m.RegisterCommandHandler("show_errors", m.handleShowErrors)

	// Show latency history handler - return historical latency data
	m.RegisterCommandHandler("show_latency_history", m.handleShowLatencyHistory)

	// Get config handler - return client configuration
	m.RegisterCommandHandler("get_config", m.handleGetConfig)
}

// ConfigPayload represents the payload for push_config command
type ConfigPayload struct {
	Enabled           *bool    `json:"enabled,omitempty"`
	HeartbeatInterval *int64   `json:"heartbeat_interval_ms,omitempty"`
	SamplingRate      *float64 `json:"sampling_rate,omitempty"`
	TTLSeconds        int64    `json:"ttl_seconds,omitempty"`
}

// CollectionMetricsPayload represents the payload for collection_metrics command
type CollectionMetricsPayload struct {
	Enabled      bool     `json:"enabled"`
	Collections  []string `json:"collections,omitempty"`
	MetricsTypes []string `json:"metrics_types,omitempty"` // e.g., "qps", "latency"
}

// handlePushConfig handles dynamic configuration updates
func (m *ClientTelemetryManager) handlePushConfig(cmd *ClientCommand) *CommandReply {
	var payload ConfigPayload
	if len(cmd.Payload) > 0 {
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			return &CommandReply{
				CommandId:    cmd.CommandId,
				Success:      false,
				ErrorMessage: "failed to parse config payload: " + err.Error(),
			}
		}
	}

	// Apply configuration changes with write lock
	m.configMu.Lock()
	if payload.Enabled != nil {
		m.config.Enabled = *payload.Enabled
	}
	if payload.HeartbeatInterval != nil {
		if *payload.HeartbeatInterval <= 0 {
			m.configMu.Unlock()
			return &CommandReply{
				CommandId:    cmd.CommandId,
				Success:      false,
				ErrorMessage: "heartbeat_interval_ms must be positive",
			}
		}
		m.config.HeartbeatInterval = time.Duration(*payload.HeartbeatInterval) * time.Millisecond
	}
	if payload.SamplingRate != nil {
		samplingRate := *payload.SamplingRate
		if samplingRate < 0.0 {
			samplingRate = 0.0
		} else if samplingRate > 1.0 {
			samplingRate = 1.0
		}
		m.config.SamplingRate = samplingRate
	}
	m.configMu.Unlock()

	return &CommandReply{
		CommandId: cmd.CommandId,
		Success:   true,
	}
}

// GetConfigResponse represents the response for get_config command
type GetConfigResponse struct {
	// UserConfig contains the client configuration provided by the user (secrets excluded)
	UserConfig map[string]interface{} `json:"user_config"`
}

// handleGetConfig handles the get_config command to return client configuration
func (m *ClientTelemetryManager) handleGetConfig(cmd *ClientCommand) *CommandReply {
	response := &GetConfigResponse{
		UserConfig: make(map[string]interface{}),
	}

	// Extract user config from client (exclude secrets)
	if m.client != nil && m.client.config != nil {
		cfg := m.client.config
		response.UserConfig["address"] = cfg.Address
		response.UserConfig["username"] = cfg.Username
		// Password and APIKey are excluded for security
		response.UserConfig["db_name"] = cfg.DBName
		response.UserConfig["enable_tls_auth"] = cfg.EnableTLSAuth
		response.UserConfig["server_version"] = cfg.ServerVersion

		// RetryRateLimit settings
		if cfg.RetryRateLimit != nil {
			response.UserConfig["retry_max_retry"] = cfg.RetryRateLimit.MaxRetry
			response.UserConfig["retry_max_backoff_ms"] = cfg.RetryRateLimit.MaxBackoff.Milliseconds()
		}

		// Current database (may differ from initial DBName if UseDatabase was called)
		response.UserConfig["current_db"] = m.client.getCurrentDB()
	}

	// Add current telemetry config
	m.configMu.RLock()
	response.UserConfig["telemetry_enabled"] = m.config.Enabled
	response.UserConfig["telemetry_heartbeat_interval_ms"] = m.config.HeartbeatInterval.Milliseconds()
	response.UserConfig["telemetry_sampling_rate"] = m.config.SamplingRate
	m.configMu.RUnlock()

	// Add enabled collections info (always use []string for consistent JSON parsing)
	m.enabledCollectionsMu.RLock()
	if m.allCollectionsEnabled {
		response.UserConfig["enabled_collections"] = []string{"*"}
	} else {
		collections := make([]string, 0, len(m.enabledCollections))
		for coll := range m.enabledCollections {
			collections = append(collections, coll)
		}
		response.UserConfig["enabled_collections"] = collections
	}
	response.UserConfig["all_collections_enabled"] = m.allCollectionsEnabled
	m.enabledCollectionsMu.RUnlock()

	// Marshal response
	responseJSON, err := json.Marshal(response)
	if err != nil {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "failed to marshal config response: " + err.Error(),
		}
	}

	return &CommandReply{
		CommandId: cmd.CommandId,
		Success:   true,
		Payload:   responseJSON,
	}
}

// handleCollectionMetrics handles collection-level metrics configuration
func (m *ClientTelemetryManager) handleCollectionMetrics(cmd *ClientCommand) *CommandReply {
	// Empty payload means "list enabled collections" - return current state
	if len(cmd.Payload) == 0 {
		m.enabledCollectionsMu.RLock()
		allEnabled := m.allCollectionsEnabled
		collections := make([]string, 0, len(m.enabledCollections))
		for coll := range m.enabledCollections {
			collections = append(collections, coll)
		}
		m.enabledCollectionsMu.RUnlock()

		response := map[string]interface{}{
			"enabled_collections":     collections,
			"all_collections_enabled": allEnabled,
		}
		responsePayload, _ := json.Marshal(response)
		return &CommandReply{
			CommandId: cmd.CommandId,
			Success:   true,
			Payload:   responsePayload,
		}
	}

	var payload CollectionMetricsPayload
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "failed to parse collection_metrics payload: " + err.Error(),
		}
	}

	m.enabledCollectionsMu.Lock()
	defer m.enabledCollectionsMu.Unlock()

	// Check for wildcard "*" in collections
	hasWildcard := false
	for _, coll := range payload.Collections {
		if coll == "*" {
			hasWildcard = true
			break
		}
	}

	if payload.Enabled {
		// Enable collection-level metrics
		if len(payload.Collections) == 0 {
			return &CommandReply{
				CommandId:    cmd.CommandId,
				Success:      false,
				ErrorMessage: "collections list cannot be empty when enabled=true",
			}
		}
		if hasWildcard {
			// Enable all collections via wildcard
			m.allCollectionsEnabled = true
		} else {
			// Enable specific collections
			for _, coll := range payload.Collections {
				m.enabledCollections[coll] = true
			}
		}
	} else {
		// Disable collection-level metrics
		if hasWildcard || len(payload.Collections) == 0 {
			// Disable all collections
			m.allCollectionsEnabled = false
			m.enabledCollections = make(map[string]bool)
		} else {
			// Disable specific collections
			for _, coll := range payload.Collections {
				delete(m.enabledCollections, coll)
			}
		}
	}

	return &CommandReply{
		CommandId: cmd.CommandId,
		Success:   true,
	}
}

// GetRecentErrors returns the most recent N errors
// This method allows external access to the error history for debugging
func (m *ClientTelemetryManager) GetRecentErrors(maxCount int) []*ErrorInfo {
	if m.errorCollector == nil {
		return nil
	}
	return m.errorCollector.GetRecentErrors(maxCount)
}

// maxErrorPayloadSize is the maximum size of error payload (1MB)
const maxErrorPayloadSize = 1 * 1024 * 1024

// handleShowErrors handles the show_errors command to return last N errors
func (m *ClientTelemetryManager) handleShowErrors(cmd *ClientCommand) *CommandReply {
	reply := &CommandReply{
		CommandId: cmd.CommandId,
		Success:   true,
	}

	// Parse payload to get max count
	var payload struct {
		MaxCount int `json:"max_count,omitempty"`
	}

	maxCount := 100 // default
	if len(cmd.Payload) > 0 {
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			return &CommandReply{
				CommandId:    cmd.CommandId,
				Success:      false,
				ErrorMessage: "failed to parse show_errors payload: " + err.Error(),
			}
		}
		if payload.MaxCount > 0 {
			maxCount = payload.MaxCount
		}
	}

	// Get recent errors from error collector
	var errors []*ErrorInfo
	if m.errorCollector != nil {
		errors = m.errorCollector.GetRecentErrors(maxCount)
	}

	// Convert errors to JSON and include in reply payload
	// Truncate if payload exceeds 1MB
	if len(errors) > 0 {
		errorsJSON, err := json.Marshal(errors)
		if err == nil {
			// If payload exceeds 1MB, truncate errors until it fits
			for len(errorsJSON) > maxErrorPayloadSize && len(errors) > 1 {
				// Remove oldest errors (at the end of the list) to reduce size
				errors = errors[:len(errors)/2] // Binary reduction for efficiency
				errorsJSON, err = json.Marshal(errors)
				if err != nil {
					break
				}
			}
			// Final check - if still too large with only 1 error, truncate the error message
			if len(errorsJSON) > maxErrorPayloadSize && len(errors) == 1 {
				// Truncate the error message itself
				maxMsgLen := maxErrorPayloadSize - 200 // Leave room for JSON structure
				if len(errors[0].ErrorMsg) > maxMsgLen {
					errors[0].ErrorMsg = errors[0].ErrorMsg[:maxMsgLen] + "...(truncated)"
					errorsJSON, _ = json.Marshal(errors)
				}
			}
			reply.Payload = errorsJSON
		}
	}

	return reply
}

// LatencyHistoryPayload represents the payload for show_latency_history command
type LatencyHistoryPayload struct {
	StartTime string `json:"start_time"` // RFC3339 format
	EndTime   string `json:"end_time"`   // RFC3339 format
	Detail    bool   `json:"detail"`     // When true, return all snapshots instead of aggregated metrics (default: false)
}

// LatencyHistoryResponse represents the response for show_latency_history command
type LatencyHistoryResponse struct {
	Snapshots      []*SnapshotResponse `json:"snapshots"`
	TotalSnapshots int                 `json:"total_snapshots"`
}

// SnapshotResponse represents a single snapshot in the latency history response
type SnapshotResponse struct {
	Timestamp int64                       `json:"timestamp"` // Unix timestamp in milliseconds (start)
	EndTime   int64                       `json:"end_time"`  // Unix timestamp in milliseconds (end)
	Metrics   map[string]*MetricsResponse `json:"metrics"`   // Operation name -> metrics
}

// MetricsResponse represents metrics for a single operation type in the response
type MetricsResponse struct {
	RequestCount int64   `json:"request_count"`
	SuccessCount int64   `json:"success_count,omitempty"`
	ErrorCount   int64   `json:"error_count,omitempty"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
	MaxLatencyMs float64 `json:"max_latency_ms"`
}

// AggregatedMetrics represents aggregated metrics across multiple snapshots
type AggregatedMetrics struct {
	StartTime int64                       `json:"start_time"` // Unix timestamp in milliseconds
	EndTime   int64                       `json:"end_time"`   // Unix timestamp in milliseconds
	Metrics   map[string]*MetricsResponse `json:"metrics"`    // Operation name -> aggregated metrics
}

// AggregatedLatencyHistoryResponse represents the aggregated response for show_latency_history
type AggregatedLatencyHistoryResponse struct {
	Aggregated    *AggregatedMetrics `json:"aggregated"`
	SnapshotCount int                `json:"snapshot_count"` // Number of snapshots aggregated
}

// GetHistoricalLatency returns snapshots within the specified time range
func (m *ClientTelemetryManager) GetHistoricalLatency(startTime, endTime time.Time) []*MetricsSnapshot {
	m.snapshotsMu.RLock()
	defer m.snapshotsMu.RUnlock()

	startMs := startTime.UnixMilli()
	endMs := endTime.UnixMilli()

	var result []*MetricsSnapshot
	for _, snapshot := range m.snapshots {
		// Include snapshot if its period overlaps with the query range
		if snapshot.EndTime >= startMs && snapshot.Timestamp <= endMs {
			result = append(result, snapshot)
		}
	}

	return result
}

// handleShowLatencyHistory handles the show_latency_history command
func (m *ClientTelemetryManager) handleShowLatencyHistory(cmd *ClientCommand) *CommandReply {
	var payload LatencyHistoryPayload
	if len(cmd.Payload) == 0 {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "payload is required with start_time and end_time",
		}
	}

	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "failed to parse show_latency_history payload: " + err.Error(),
		}
	}

	// Parse times
	startTime, err := time.Parse(time.RFC3339, payload.StartTime)
	if err != nil {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "invalid start_time format, expected RFC3339: " + err.Error(),
		}
	}

	endTime, err := time.Parse(time.RFC3339, payload.EndTime)
	if err != nil {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "invalid end_time format, expected RFC3339: " + err.Error(),
		}
	}

	// Validate time range (max 1 hour)
	if endTime.Sub(startTime) > time.Hour {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "time range cannot exceed 1 hour",
		}
	}

	if endTime.Before(startTime) {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "end_time must be after start_time",
		}
	}

	// Get historical snapshots
	snapshots := m.GetHistoricalLatency(startTime, endTime)

	var responseJSON []byte

	if payload.Detail {
		// Return all snapshots (when detail=true)
		response := &LatencyHistoryResponse{
			Snapshots:      make([]*SnapshotResponse, 0, len(snapshots)),
			TotalSnapshots: len(snapshots),
		}

		for _, snapshot := range snapshots {
			snapshotResp := &SnapshotResponse{
				Timestamp: snapshot.Timestamp,
				EndTime:   snapshot.EndTime,
				Metrics:   make(map[string]*MetricsResponse),
			}

			for _, opMetrics := range snapshot.Metrics {
				if opMetrics.Global != nil {
					snapshotResp.Metrics[opMetrics.Operation] = &MetricsResponse{
						RequestCount: opMetrics.Global.RequestCount,
						SuccessCount: opMetrics.Global.SuccessCount,
						ErrorCount:   opMetrics.Global.ErrorCount,
						AvgLatencyMs: opMetrics.Global.AvgLatencyMs,
						P99LatencyMs: opMetrics.Global.P99LatencyMs,
						MaxLatencyMs: opMetrics.Global.MaxLatencyMs,
					}
				}
			}

			response.Snapshots = append(response.Snapshots, snapshotResp)
		}
		responseJSON, err = json.Marshal(response)
	} else {
		// Return aggregated metrics (default)
		response := m.aggregateSnapshots(snapshots, startTime.UnixMilli(), endTime.UnixMilli())
		responseJSON, err = json.Marshal(response)
	}

	if err != nil {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "failed to marshal response: " + err.Error(),
		}
	}

	// Check payload size (max 1MB)
	if len(responseJSON) > maxErrorPayloadSize {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "response too large, try a smaller time range",
		}
	}

	return &CommandReply{
		CommandId: cmd.CommandId,
		Success:   true,
		Payload:   responseJSON,
	}
}

// aggregateSnapshots aggregates multiple snapshots into a single response
// Uses weighted average for latencies (weighted by request count)
func (m *ClientTelemetryManager) aggregateSnapshots(snapshots []*MetricsSnapshot, startTime, endTime int64) *AggregatedLatencyHistoryResponse {
	if len(snapshots) == 0 {
		return &AggregatedLatencyHistoryResponse{
			Aggregated: &AggregatedMetrics{
				StartTime: startTime,
				EndTime:   endTime,
				Metrics:   make(map[string]*MetricsResponse),
			},
			SnapshotCount: 0,
		}
	}

	// Aggregate metrics by operation
	type aggregator struct {
		requestCount   int64
		successCount   int64
		errorCount     int64
		weightedAvgSum float64 // sum of (avg_latency * request_count)
		weightedP99Sum float64 // sum of (p99_latency * request_count)
		maxLatency     float64
	}

	aggregators := make(map[string]*aggregator)

	for _, snapshot := range snapshots {
		for _, opMetrics := range snapshot.Metrics {
			if opMetrics.Global == nil {
				continue
			}

			agg, ok := aggregators[opMetrics.Operation]
			if !ok {
				agg = &aggregator{}
				aggregators[opMetrics.Operation] = agg
			}

			agg.requestCount += opMetrics.Global.RequestCount
			agg.successCount += opMetrics.Global.SuccessCount
			agg.errorCount += opMetrics.Global.ErrorCount
			agg.weightedAvgSum += opMetrics.Global.AvgLatencyMs * float64(opMetrics.Global.RequestCount)
			agg.weightedP99Sum += opMetrics.Global.P99LatencyMs * float64(opMetrics.Global.RequestCount)
			if opMetrics.Global.MaxLatencyMs > agg.maxLatency {
				agg.maxLatency = opMetrics.Global.MaxLatencyMs
			}
		}
	}

	// Convert to response format
	metrics := make(map[string]*MetricsResponse)
	for op, agg := range aggregators {
		avgLatency := 0.0
		p99Latency := 0.0
		if agg.requestCount > 0 {
			avgLatency = agg.weightedAvgSum / float64(agg.requestCount)
			p99Latency = agg.weightedP99Sum / float64(agg.requestCount)
		}

		metrics[op] = &MetricsResponse{
			RequestCount: agg.requestCount,
			SuccessCount: agg.successCount,
			ErrorCount:   agg.errorCount,
			AvgLatencyMs: avgLatency,
			P99LatencyMs: p99Latency,
			MaxLatencyMs: agg.maxLatency,
		}
	}

	return &AggregatedLatencyHistoryResponse{
		Aggregated: &AggregatedMetrics{
			StartTime: startTime,
			EndTime:   endTime,
			Metrics:   metrics,
		},
		SnapshotCount: len(snapshots),
	}
}
