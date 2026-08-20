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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/common"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// TelemetryConfig holds configurable settings for client telemetry
type TelemetryConfig struct {
	// Enabled controls whether telemetry collection is active
	Enabled bool
	// HeartbeatInterval is how often to send heartbeats to server (default: 10 seconds).
	//
	// It is also the metrics window: each heartbeat carries the operations since the last
	// one. The coordinator answers a telemetry query from the window before the newest, so
	// what a caller reads is between one and two intervals old -- ten to twenty seconds at
	// the default. Raising it cuts the coordinator's heartbeat load, which scales with the
	// number of connected clients rather than with traffic, at the cost of that staleness.
	HeartbeatInterval time.Duration
	// SamplingRate is the sampling rate for all operations (0.0-1.0, default: 1.0 = 100%)
	// Can be dynamically adjusted
	SamplingRate float64
	// ErrorMaxCount is the maximum number of errors to keep
	ErrorMaxCount int
	// ClientID identifies this client to the server across process restarts.
	//
	// When empty, a random UUID is generated per Client, which means the server sees a
	// brand-new client on every restart: telemetry history fragments and `client:<id>`
	// scoped commands cannot target a long-lived client. Set it to a stable value (e.g.
	// a pod name, or hostname+role) to get continuity. It must be unique per process --
	// reusing one value across processes collapses them into a single server-side entry.
	ClientID string
}

// DefaultTelemetryConfig returns the default telemetry configuration
func DefaultTelemetryConfig() *TelemetryConfig {
	return &TelemetryConfig{
		Enabled:           true,
		HeartbeatInterval: 10 * time.Second,
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

	// Unique client ID, resolved once at construction. Stable for the lifetime of this
	// Client (and therefore across gRPC reconnects). It only survives a process restart
	// when the caller pins it via TelemetryConfig.ClientID; otherwise it is a fresh UUID.
	clientID string

	// clientIDStable records whether clientID came from TelemetryConfig.ClientID (and so
	// survives a restart) rather than being generated for this process.
	clientIDStable bool

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

	// unsupportedStreak counts consecutive heartbeats rejected with codes.Unimplemented.
	// It backs off the heartbeat interval instead of latching telemetry off: a client
	// load-balances across proxies, so during a rolling upgrade a single heartbeat can
	// land on an old one while the rest of the cluster already supports the service.
	// Disabling permanently on one such answer would never recover. Reset by any
	// successful heartbeat.
	unsupportedStreak atomic.Int64

	// lastHeartbeatErr records the most recent heartbeat failure. The client module has
	// no logger, so this is the only way to surface an otherwise silent best-effort
	// failure; read it with LastHeartbeatError().
	lastHeartbeatErr   error
	lastHeartbeatErrMu sync.RWMutex

	// lastSnapshotEnd is the end of the most recent snapshot window, in Unix milliseconds,
	// so the next one can start where it left off instead of assuming the configured
	// interval elapsed. Zero until the first snapshot.
	lastSnapshotEnd atomic.Int64

	// samplingAccum carries the fractional sampling rate between calls, in samplingScale
	// units: each operation adds the rate and the one that pushes it past a whole unit is
	// the one sampled. See shouldSample.
	samplingAccum uint64
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

	// Prefer a caller-supplied stable ID so the server can correlate this client across
	// process restarts; fall back to a per-process UUID.
	clientID := config.ClientID
	clientIDStable := clientID != ""
	if clientID == "" {
		clientID = uuid.New().String()
	}

	tm := &ClientTelemetryManager{
		config:             config,
		client:             client,
		clientID:           clientID,
		clientIDStable:     clientIDStable,
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
			"client_id": m.clientID,
			// Tell the server whether this ID survives a restart. It only does when the
			// caller pinned it via TelemetryConfig.ClientID; a generated UUID does not.
			// The server needs this to decide whether a client-scoped persistent config
			// would keep matching.
			"client_id_stable": strconv.FormatBool(m.clientIDStable),
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
		interval := m.nextHeartbeatDelay()
		select {
		case <-m.stopCh:
			return
		case <-time.After(interval):
			m.createSnapshot()
			m.sendHeartbeat()
		}
	}
}

// maxUnsupportedBackoff caps the heartbeat interval while the server keeps answering
// Unimplemented. Large enough that talking to an old cluster costs almost nothing, small
// enough that a client notices an upgrade without being restarted.
const maxUnsupportedBackoff = 30 * time.Minute

// nextHeartbeatDelay returns how long to wait before the next heartbeat: the configured
// interval normally, backed off while the server keeps reporting Unimplemented.
//
// The backoff doubles per consecutive rejection and is capped, so a client against a
// server without the service settles at one probe per maxUnsupportedBackoff rather than
// one per interval -- and still recovers on its own once the server gains the service.
func (m *ClientTelemetryManager) nextHeartbeatDelay() time.Duration {
	interval := m.getHeartbeatInterval()

	streak := m.unsupportedStreak.Load()
	if streak <= 0 {
		return interval
	}

	backoff := interval
	for i := int64(0); i < streak && backoff < maxUnsupportedBackoff; i++ {
		backoff *= 2
	}
	if backoff > maxUnsupportedBackoff {
		backoff = maxUnsupportedBackoff
	}
	if backoff < interval {
		// A client configured to heartbeat less often than the cap must not start
		// heartbeating *more* often because the server is rejecting it.
		return interval
	}
	return backoff
}

// IsSupported reports whether the server is currently known *not* to implement
// ClientTelemetryService. It is optimistic: it returns true before the first heartbeat has
// been sent, because nothing is known yet, so true means "no evidence of an old server"
// rather than "confirmed supported". Use LastHeartbeatError to tell those apart.
//
// It goes false while the server answers codes.Unimplemented and returns true again on the
// first reply, so it reflects current reachability rather than a permanent verdict -- a
// client may be load-balanced across proxies that differ mid upgrade.
func (m *ClientTelemetryManager) IsSupported() bool {
	return m.unsupportedStreak.Load() == 0
}

// LastHeartbeatError returns the most recent heartbeat failure, or nil if the last
// heartbeat succeeded. Heartbeats are best-effort and never surfaced through the normal
// API, so this is the supported way to diagnose a client that is not reporting.
func (m *ClientTelemetryManager) LastHeartbeatError() error {
	m.lastHeartbeatErrMu.RLock()
	defer m.lastHeartbeatErrMu.RUnlock()
	return m.lastHeartbeatErr
}

func (m *ClientTelemetryManager) setLastHeartbeatError(err error) {
	m.lastHeartbeatErrMu.Lock()
	defer m.lastHeartbeatErrMu.Unlock()
	m.lastHeartbeatErr = err
}

// sendHeartbeat sends a heartbeat to the server
func (m *ClientTelemetryManager) sendHeartbeat() {
	m.configMu.RLock()
	enabled := m.config.Enabled
	m.configMu.RUnlock()

	if !enabled {
		return
	}

	if m.client == nil || m.client.telemetryService == nil {
		return
	}

	// Get metrics from the latest snapshot (P99 already calculated during snapshot creation)
	var metrics []*commonpb.OperationMetrics
	if latestSnapshot := m.GetLatestSnapshot(); latestSnapshot != nil {
		metrics = m.toProtoOperationMetrics(latestSnapshot.Metrics)
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Telemetry is best-effort: opt out of the client-wide retry interceptor so a failing
	// heartbeat costs exactly one RPC instead of up to 6 with backoff. The next heartbeat
	// is the retry.
	resp, err := m.client.telemetryService.ClientHeartbeat(ctx, req, grpc_retry.Disable())
	if err != nil {
		m.setLastHeartbeatError(err)
		// Unimplemented means *this* server does not offer the service. That may be the
		// whole cluster, or it may be one stale proxy during a rolling upgrade, so back
		// off rather than give up: the interval grows with the streak and snaps back on
		// the first success.
		if s, ok := status.FromError(err); ok && s.Code() == codes.Unimplemented {
			m.unsupportedStreak.Add(1)
		}
		return
	}

	// Reaching a reply at all proves the server implements the service: an unimplemented
	// method never returns a business status. Clear the backoff before inspecting that
	// status, so a server that is reachable but unhealthy -- a coordinator still starting,
	// a rate limit -- does not leave the client stuck at an inflated interval left over
	// from some earlier Unimplemented.
	m.unsupportedStreak.Store(0)

	if err := merr.Error(resp.GetStatus()); err != nil {
		m.setLastHeartbeatError(err)
		return
	}
	m.setLastHeartbeatError(nil)

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
		// Matches DefaultTelemetryConfig: a config built field by field can leave this
		// zero, and falling back to a different number than the documented default would
		// make the interval depend on how the config was constructed.
		return DefaultTelemetryConfig().HeartbeatInterval
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

// samplingScale is the fixed-point unit for accumulating a fractional sampling rate. A
// rate becomes an integer step of samplingScale units, so the smallest rate that still
// samples is 1e-9 -- far below anything an operator would set, which is the point: a
// configured rate must never round down to "off".
const samplingScale = 1_000_000_000

// shouldSample decides whether this operation is recorded, spreading the sampled ones
// evenly rather than in runs.
//
// Each call adds the rate to a shared accumulator and samples on the call that carries it
// across a whole unit: at 0.25 that is every fourth operation, at 0.1 every tenth. What
// matters is that the ratio holds over any stretch of calls, not only over a long one --
// metrics are reported per heartbeat window, and a window is tens or hundreds of
// operations. A scheme that sampled a contiguous run and then dropped one would give the
// right long-run ratio while making every individual window either complete or empty.
//
// The accumulator is shared, so concurrent callers reorder which of them observes a
// crossing, but each crossing is observed exactly once: atomic.AddUint64 hands every caller
// a distinct interval, and the step is smaller than one unit, so no interval spans two
// crossings. The count of sampled operations is therefore exact, not statistical, which is
// also why this needs no random source.
func (m *ClientTelemetryManager) shouldSample(samplingRate float64) bool {
	if samplingRate >= 1.0 {
		return true
	}
	if samplingRate <= 0.0 {
		return false
	}

	step := uint64(samplingRate * float64(samplingScale))
	if step == 0 {
		// A rate too small to represent still means "sample rarely", never "sample never":
		// silently disabling telemetry for a positive rate is the one outcome nobody could
		// have intended.
		step = 1
	}

	after := atomic.AddUint64(&m.samplingAccum, step)
	before := after - step
	return after/samplingScale != before/samplingScale
}

// toProtoOperationMetrics converts collected metrics into their proto form, dropping
// collection-level entries for collections that are not currently enabled ("*" enables
// all). This is the single conversion path used for everything put on the wire.
func (m *ClientTelemetryManager) toProtoOperationMetrics(opMetricsList []*OperationMetrics) []*commonpb.OperationMetrics {
	if len(opMetricsList) == 0 {
		return nil
	}

	enabledCollections, allEnabled := m.snapshotEnabledCollections()

	result := make([]*commonpb.OperationMetrics, 0, len(opMetricsList))
	for _, opMetrics := range opMetricsList {
		protoCollMetrics := make(map[string]*commonpb.Metrics)
		for coll, cm := range opMetrics.CollectionMetrics {
			if allEnabled || enabledCollections[coll] {
				protoCollMetrics[coll] = toProtoMetrics(cm)
			}
		}

		result = append(result, &commonpb.OperationMetrics{
			Operation:         opMetrics.Operation,
			Global:            toProtoMetrics(opMetrics.Global),
			CollectionMetrics: protoCollMetrics,
		})
	}

	return result
}

// toProtoMetrics converts a single metrics bucket to proto form.
func toProtoMetrics(metrics *Metrics) *commonpb.Metrics {
	if metrics == nil {
		return nil
	}
	return &commonpb.Metrics{
		RequestCount: metrics.RequestCount,
		SuccessCount: metrics.SuccessCount,
		ErrorCount:   metrics.ErrorCount,
		AvgLatencyMs: metrics.AvgLatencyMs,
		P99LatencyMs: metrics.P99LatencyMs,
		MaxLatencyMs: metrics.MaxLatencyMs,
	}
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

	// The window starts where the previous one ended, not one configured interval ago.
	// Counters accumulate until they are collected, so the window has to be the time
	// actually covered. Assuming HeartbeatInterval is wrong whenever the loop did not run
	// on schedule: the Unimplemented backoff can stretch a gap to 30 minutes, and a
	// server-pushed interval change moves it too. Labeling half an hour of traffic as a
	// 30 second window makes every rate derived from it, and every history query that
	// filters on the range, wrong by the same factor.
	start := m.lastSnapshotEnd.Load()
	if start == 0 || start > now {
		// First snapshot of this process, or the clock moved backwards. Fall back to the
		// configured interval -- it is the best guess available for a window with no
		// predecessor.
		start = now - heartbeatInterval.Milliseconds()
	}
	m.lastSnapshotEnd.Store(now)

	snapshot := &MetricsSnapshot{
		Timestamp: start, // Start of the snapshot period
		EndTime:   now,   // End of the snapshot period
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

// ConfigApplyResult is what a push_config reply carries back, so the sender can tell a
// config that took effect from one that was quietly dropped.
//
// encoding/json ignores fields it does not know, which made every payload look accepted:
// a misspelled key, a key belonging to a newer client, or ttl_seconds -- which lives in
// ConfigPayload and is sent by the web UI but has never been read by anything -- all
// produced the same bare Success. Naming both halves is the only way the caller can see
// the difference.
type ConfigApplyResult struct {
	// Applied lists the payload keys that changed this client's configuration.
	Applied []string `json:"applied"`
	// Ignored lists the payload keys this client does not act on. It is not an error:
	// failing the whole command would stop a newer server from configuring an older
	// client at all, so the keys are reported and the rest is still applied.
	Ignored []string `json:"ignored,omitempty"`
}

// configPayloadKeys are the payload keys handlePushConfig acts on. Anything else in a
// payload is reported as ignored.
var configPayloadKeys = map[string]struct{}{
	"enabled":               {},
	"heartbeat_interval_ms": {},
	"sampling_rate":         {},
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
	// raw is decoded alongside the typed payload purely to see which keys were sent:
	// unmarshalling into ConfigPayload cannot distinguish "key absent" from "key unknown",
	// and both halves are needed to answer honestly.
	raw := map[string]json.RawMessage{}
	if len(cmd.Payload) > 0 {
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			return &CommandReply{
				CommandId:    cmd.CommandId,
				Success:      false,
				ErrorMessage: "failed to parse config payload: " + err.Error(),
			}
		}
		if err := json.Unmarshal(cmd.Payload, &raw); err != nil {
			return &CommandReply{
				CommandId:    cmd.CommandId,
				Success:      false,
				ErrorMessage: "failed to parse config payload: " + err.Error(),
			}
		}
	}

	ignored := make([]string, 0, len(raw))
	for key := range raw {
		if _, known := configPayloadKeys[key]; !known {
			ignored = append(ignored, key)
		}
	}
	sort.Strings(ignored)

	// Validate before touching anything: a payload is applied whole or not at all. Writing
	// the fields as they were read would let a rejected value leave earlier ones applied,
	// so a command carrying both enabled and a bad interval would switch telemetry off and
	// still report failure -- the caller would have no way to know what state it left.
	if payload.HeartbeatInterval != nil && *payload.HeartbeatInterval <= 0 {
		return &CommandReply{
			CommandId:    cmd.CommandId,
			Success:      false,
			ErrorMessage: "heartbeat_interval_ms must be positive",
		}
	}

	applied := make([]string, 0, len(configPayloadKeys))

	// Apply configuration changes with write lock
	m.configMu.Lock()
	if payload.Enabled != nil {
		m.config.Enabled = *payload.Enabled
		applied = append(applied, "enabled")
	}
	if payload.HeartbeatInterval != nil {
		m.config.HeartbeatInterval = time.Duration(*payload.HeartbeatInterval) * time.Millisecond
		applied = append(applied, "heartbeat_interval_ms")
	}
	if payload.SamplingRate != nil {
		// Out-of-range rates are clamped rather than rejected, which is why this is not
		// part of the validation above: 1.5 means "everything" and -0.5 means "nothing",
		// and neither is ambiguous enough to refuse.
		samplingRate := *payload.SamplingRate
		if samplingRate < 0.0 {
			samplingRate = 0.0
		} else if samplingRate > 1.0 {
			samplingRate = 1.0
		}
		m.config.SamplingRate = samplingRate
		applied = append(applied, "sampling_rate")
	}
	m.configMu.Unlock()

	reply := &CommandReply{
		CommandId: cmd.CommandId,
		Success:   true,
	}
	// A reply that cannot be encoded would be worse than one without the detail, so the
	// command still succeeds -- the configuration was applied either way.
	if encoded, err := json.Marshal(ConfigApplyResult{Applied: applied, Ignored: ignored}); err == nil {
		reply.Payload = encoded
	}
	return reply
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
