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

package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// TelemetryConfig holds configurable time values for the telemetry manager
type TelemetryConfig struct {
	// CleanupInterval is how often the cleanup loop runs (default: 5 minutes)
	CleanupInterval time.Duration
	// InactiveClientThreshold is how long since last heartbeat before a client is removed (default: 10 minutes)
	InactiveClientThreshold time.Duration
	// ClientStatusThreshold is how long since last heartbeat before a client is marked inactive (default: 1 minute)
	ClientStatusThreshold time.Duration
	// CommandCleanupTimeout is the context timeout for command cleanup operations (default: 10 seconds)
	CommandCleanupTimeout time.Duration
	// MaxMetricsPerClient is the maximum size of metrics payload per client (default: 1MB)
	MaxMetricsPerClient int
	// MaxOperationTypesPerClient is the maximum number of operation types per client (default: 100)
	MaxOperationTypesPerClient int
	// MaxClientsInMemory is the maximum number of clients to track in memory (default: 100,000)
	// This prevents unbounded memory growth from malicious or misconfigured clients
	MaxClientsInMemory int
}

// DefaultTelemetryConfig returns the default configuration
func DefaultTelemetryConfig() *TelemetryConfig {
	return &TelemetryConfig{
		CleanupInterval:            1 * time.Minute,
		InactiveClientThreshold:    10 * time.Minute,
		ClientStatusThreshold:      1 * time.Minute,
		CommandCleanupTimeout:      10 * time.Second,
		MaxMetricsPerClient:        1 * 1024 * 1024, // 1MB max per client
		MaxOperationTypesPerClient: 100,             // Maximum 100 operation types
		MaxClientsInMemory:         100000,          // Maximum 100k clients in memory
	}
}

// StoredCommandReply stores a command reply with metadata
// NOTE: Payload and CommandPayload are stored as strings (not []byte) to ensure
// proper JSON serialization without base64 encoding, making the API response
// directly parseable by JavaScript clients.
type StoredCommandReply struct {
	CommandID      string `json:"command_id"`
	CommandType    string `json:"command_type,omitempty"`
	CommandPayload string `json:"command_payload,omitempty"`
	Success        bool   `json:"success"`
	ErrorMsg       string `json:"error_msg,omitempty"`
	Payload        string `json:"payload,omitempty"`
	ReceivedAt     int64  `json:"received_at"`
}

// ClientMetricsCache stores the latest metrics from a client
type ClientMetricsCache struct {
	ClientInfo        *commonpb.ClientInfo
	ClientID          string
	LastHeartbeat     atomic.Int64 // Unix nanoseconds for atomic access
	LatestMetrics     []*commonpb.OperationMetrics
	AccessedDatabases sync.Map // map[string]struct{} for concurrent access
	ConfigHash        string
	LastCommandTS     int64
	CommandReplies    []*StoredCommandReply // Last N command replies from this client
}

// TelemetryManager manages client telemetry data
type TelemetryManager struct {
	clientMetrics sync.Map // key: client_id -> *ClientMetricsCache

	commandStore  CommandStoreInterface
	commandRouter *CommandRouter
	config        *TelemetryConfig

	// Background cleanup
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewTelemetryManager creates a new TelemetryManager with default config
func NewTelemetryManager(etcdClient *clientv3.Client) *TelemetryManager {
	return NewTelemetryManagerWithConfig(etcdClient, DefaultTelemetryConfig())
}

// NewTelemetryManagerWithConfig creates a new TelemetryManager with custom config
func NewTelemetryManagerWithConfig(etcdClient *clientv3.Client, config *TelemetryConfig) *TelemetryManager {
	var store CommandStoreInterface
	if etcdClient != nil {
		store = NewCommandStore(etcdClient, "/client-telemetry/")
	}

	if config == nil {
		config = DefaultTelemetryConfig()
	}

	tm := &TelemetryManager{
		commandStore: store,
		config:       config,
		stopCh:       make(chan struct{}),
	}

	// Initialize command router with default handlers
	tm.commandRouter = NewCommandRouter()
	tm.initializeCommandHandlers()

	return tm
}

// SetCommandStore sets the command store (for testing)
func (m *TelemetryManager) SetCommandStore(store CommandStoreInterface) {
	m.commandStore = store
}

// SetConfig updates the telemetry configuration
func (m *TelemetryManager) SetConfig(config *TelemetryConfig) {
	m.config = config
}

// Start launches the background cleanup goroutine
func (m *TelemetryManager) Start() {
	m.wg.Add(1)
	go m.cleanupLoop()
}

// Stop stops the background cleanup goroutine
func (m *TelemetryManager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// cleanupLoop periodically cleans up inactive clients
func (m *TelemetryManager) cleanupLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			// Create context for background cleanup operations
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			m.cleanupInactiveClients(ctx)
			m.cleanupExpiredCommands(ctx)
			cancel()
		}
	}
}

// cleanupInactiveClients removes clients that haven't sent a heartbeat within threshold
// Also enforces memory limits using LRU eviction when limit is reached
func (m *TelemetryManager) cleanupInactiveClients(ctx context.Context) {
	now := time.Now()

	// First pass: remove clients that exceed the inactive threshold
	cleaned := 0
	m.clientMetrics.Range(func(key, value any) bool {
		clientID := key.(string)
		cache := value.(*ClientMetricsCache)
		lastHeartbeat := time.Unix(0, cache.LastHeartbeat.Load())
		inactiveDuration := now.Sub(lastHeartbeat)
		if inactiveDuration > m.config.InactiveClientThreshold {
			m.clientMetrics.Delete(clientID)
			cleaned++
			log.Ctx(ctx).Debug("cleanupInactiveClients: removed inactive client",
				zap.String("client_id", clientID),
				zap.Duration("inactive_duration", inactiveDuration),
				zap.Duration("threshold", m.config.InactiveClientThreshold))
		}
		return true
	})
	if cleaned > 0 {
		log.Ctx(ctx).Debug("cleanupInactiveClients: normal cleanup completed",
			zap.Int("cleaned_count", cleaned))
	}

	// Second pass: enforce LRU eviction if still over limit
	m.evictLRUIfNeeded(ctx)
}

// evictLRUIfNeeded removes the least recently used clients if the count exceeds MaxClientsInMemory
func (m *TelemetryManager) evictLRUIfNeeded(ctx context.Context) {
	// Count current clients
	clientCount := 0
	m.clientMetrics.Range(func(key, value any) bool {
		clientCount++
		return true
	})

	if clientCount <= m.config.MaxClientsInMemory {
		return
	}

	// Collect all clients with their last heartbeat time for LRU sorting
	type clientEntry struct {
		clientID      string
		lastHeartbeat time.Time
	}
	entries := make([]clientEntry, 0, clientCount)
	m.clientMetrics.Range(func(key, value any) bool {
		clientID := key.(string)
		cache := value.(*ClientMetricsCache)
		entries = append(entries, clientEntry{
			clientID:      clientID,
			lastHeartbeat: time.Unix(0, cache.LastHeartbeat.Load()),
		})
		return true
	})

	// Sort by last heartbeat (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastHeartbeat.Before(entries[j].lastHeartbeat)
	})

	// Evict oldest clients until we're under the limit
	toEvict := len(entries) - m.config.MaxClientsInMemory
	if toEvict > 0 {
		log.Ctx(ctx).Warn("telemetry client count exceeds limit, LRU eviction required",
			zap.Int("current_count", len(entries)),
			zap.Int("max_allowed", m.config.MaxClientsInMemory),
			zap.Int("to_evict", toEvict))

		for i := 0; i < toEvict && i < len(entries); i++ {
			m.clientMetrics.Delete(entries[i].clientID)
			log.Ctx(ctx).Debug("cleanupInactiveClients: LRU evicted client",
				zap.String("client_id", entries[i].clientID),
				zap.Time("last_heartbeat", entries[i].lastHeartbeat))
		}

		log.Ctx(ctx).Info("cleanupInactiveClients: LRU eviction completed",
			zap.Int("evicted_count", toEvict),
			zap.Int("max_allowed", m.config.MaxClientsInMemory))
	}
}

// cleanupExpiredCommands removes expired commands from etcd
func (m *TelemetryManager) cleanupExpiredCommands(ctx context.Context) {
	if m.commandStore == nil {
		log.Ctx(ctx).Debug("cleanupExpiredCommands: command store not initialized")
		return
	}

	// Create a timeout context for command cleanup
	cleanupCtx, cancel := context.WithTimeout(ctx, m.config.CommandCleanupTimeout)
	defer cancel()

	m.commandStore.CleanupExpiredCommands(cleanupCtx)
}

// validateAndTruncateMetrics validates metrics size and truncates if needed to prevent DoS
// Only keeps collections with requests to reduce data size
func (m *TelemetryManager) validateAndTruncateMetrics(metrics []*commonpb.OperationMetrics) []*commonpb.OperationMetrics {
	if len(metrics) == 0 {
		return metrics
	}

	// Limit number of operation types
	if len(metrics) > m.config.MaxOperationTypesPerClient {
		metrics = metrics[:m.config.MaxOperationTypesPerClient]
	}

	// Filter out collections with zero requests to compress data
	for _, opMetrics := range metrics {
		if len(opMetrics.CollectionMetrics) > 0 {
			// Keep only collections that have requests
			newMap := make(map[string]*commonpb.Metrics)
			for name, m := range opMetrics.CollectionMetrics {
				if m != nil && m.RequestCount > 0 {
					newMap[name] = m
				}
			}
			opMetrics.CollectionMetrics = newMap
		}
	}

	// Enforce max payload size (best-effort based on proto size).
	// First drop collection-level metrics, then truncate operations if still too large.
	if m.config.MaxMetricsPerClient > 0 {
		size := m.estimateMetricsSize(metrics)
		if size > m.config.MaxMetricsPerClient {
			for _, opMetrics := range metrics {
				if opMetrics != nil {
					opMetrics.CollectionMetrics = nil
				}
			}
			size = m.estimateMetricsSize(metrics)
			if size > m.config.MaxMetricsPerClient {
				var truncated []*commonpb.OperationMetrics
				total := 0
				for _, opMetrics := range metrics {
					if opMetrics == nil {
						continue
					}
					s := proto.Size(opMetrics)
					if total+s > m.config.MaxMetricsPerClient {
						break
					}
					truncated = append(truncated, opMetrics)
					total += s
				}
				metrics = truncated
			}
		}
	}

	return metrics
}

func (m *TelemetryManager) estimateMetricsSize(metrics []*commonpb.OperationMetrics) int {
	total := 0
	for _, opMetrics := range metrics {
		if opMetrics == nil {
			continue
		}
		total += proto.Size(opMetrics)
	}
	return total
}

// HandleHeartbeat processes a client heartbeat and returns commands
// This method uses a two-phase approach for scalability:
// 1. Fast path: Update client metrics cache in memory using sync.Map
// 2. Slow path: Fetch commands from in-memory cache (not etcd)
// This design prevents lock contention and etcd query amplification for 10,000+ clients
func (m *TelemetryManager) HandleHeartbeat(req *milvuspb.ClientHeartbeatRequest) (*milvuspb.ClientHeartbeatResponse, error) {
	// Phase 1: Fast cache update using sync.Map (lock-free for reads)
	// IMPORTANT: Generate clientID once and reuse to avoid inconsistency
	clientID := m.getOrCreateClientID(req.ClientInfo)

	// Load or create client cache
	var cache *ClientMetricsCache
	if existing, loaded := m.clientMetrics.Load(clientID); loaded {
		cache = existing.(*ClientMetricsCache)
	} else {
		cache = &ClientMetricsCache{
			ClientID: clientID,
		}
		// Use LoadOrStore to handle race condition
		if actual, loaded := m.clientMetrics.LoadOrStore(clientID, cache); loaded {
			cache = actual.(*ClientMetricsCache)
		}
	}

	// Update cache fields (these updates are safe as each client has its own cache)
	cache.ClientInfo = req.ClientInfo
	cache.LastHeartbeat.Store(time.Now().UnixNano())
	cache.LatestMetrics = m.validateAndTruncateMetrics(req.Metrics) // Validate and truncate metrics
	cache.ConfigHash = req.ConfigHash
	cache.LastCommandTS = req.LastCommandTimestamp
	if dbName := m.getDatabaseFromClientInfo(req.ClientInfo); dbName != "" {
		cache.AccessedDatabases.Store(dbName, struct{}{})
	}

	// Process command replies from client (client's acknowledgment of commands it received)
	// This is used to track which commands have been successfully processed by the client
	var repliedIDs []string
	if len(req.CommandReplies) > 0 {
		repliedIDs = m.processCommandReplies(cache, req.CommandReplies)
	}

	if len(repliedIDs) > 0 {
		m.cleanupRepliedCommands(repliedIDs)
	}

	// Phase 2: Fetch commands WITHOUT holding the lock (uses in-memory cache, not etcd)
	// This prevents serialization of heartbeats and allows parallel processing
	// IMPORTANT: Pass clientID to avoid regenerating it
	commands := m.getCommandsForClientWithID(clientID, req)

	return &milvuspb.ClientHeartbeatResponse{
		Status:          &commonpb.Status{},
		ServerTimestamp: time.Now().UnixMilli(),
		Commands:        commands,
	}, nil
}

// processCommandReplies processes acknowledgments from client about executed commands
// This tracks which commands were successfully executed by clients for monitoring and retry
func (m *TelemetryManager) processCommandReplies(cache *ClientMetricsCache, replies []*commonpb.CommandReply) []string {
	if len(replies) == 0 || cache == nil {
		return nil
	}

	now := time.Now().UnixMilli()
	const maxStoredReplies = 50 // Keep last 50 replies per client
	deletedIDs := make([]string, 0, len(replies))

	for _, reply := range replies {
		if reply == nil {
			continue
		}

		cmdType, cmdPayload := m.lookupCommandInfo(reply.CommandId)
		stored := &StoredCommandReply{
			CommandID:      reply.CommandId,
			CommandType:    cmdType,
			CommandPayload: string(cmdPayload), // Convert []byte to string for JSON serialization
			Success:        reply.Success,
			ErrorMsg:       reply.ErrorMessage,
			Payload:        string(reply.Payload), // Convert []byte to string for JSON serialization
			ReceivedAt:     now,
		}

		cache.CommandReplies = append(cache.CommandReplies, stored)

		if !reply.Success {
			log.Warn("processCommandReplies: command execution failed",
				zap.String("client_id", cache.ClientID),
				zap.String("command_id", reply.CommandId),
				zap.String("command_type", cmdType),
				zap.String("error", reply.ErrorMessage))
		}

		if reply.CommandId != "" {
			deletedIDs = append(deletedIDs, reply.CommandId)
		}
	}

	// Keep only the most recent replies
	if len(cache.CommandReplies) > maxStoredReplies {
		cache.CommandReplies = cache.CommandReplies[len(cache.CommandReplies)-maxStoredReplies:]
	}

	return deletedIDs
}

func (m *TelemetryManager) lookupCommandInfo(commandID string) (string, []byte) {
	if m.commandStore == nil || commandID == "" {
		return "", nil
	}
	cmdType, payload, _, ok := m.commandStore.GetCommandInfo(commandID)
	if !ok {
		return "", nil
	}
	return cmdType, payload
}

func (m *TelemetryManager) cleanupRepliedCommands(commandIDs []string) {
	if m.commandStore == nil {
		return
	}
	for _, id := range commandIDs {
		if id == "" {
			continue
		}
		m.commandStore.DeleteNonPersistentCommand(id)
	}
}

func (m *TelemetryManager) getOrCreateClientID(info *commonpb.ClientInfo) string {
	// Use reserved["client_id"] if exists - this must be a stable UUID from the client
	if info != nil && info.Reserved != nil {
		if id, ok := info.Reserved["client_id"]; ok && id != "" {
			return id
		}
	}
	// Fallback: generate a stable legacy ID from client attributes.
	// This avoids unbounded growth when old clients don't supply client_id.
	host := "unknown"
	sdkType := ""
	sdkVersion := ""
	user := ""
	if info != nil {
		if info.Host != "" {
			host = info.Host
		}
		sdkType = info.SdkType
		sdkVersion = info.SdkVersion
		user = info.User
	}
	seed := fmt.Sprintf("%s|%s|%s|%s", sdkType, sdkVersion, host, user)
	sum := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("legacy:%s:%s", host, hex.EncodeToString(sum[:8]))
}

func (m *TelemetryManager) getDatabaseFromClientInfo(info *commonpb.ClientInfo) string {
	if info == nil || info.Reserved == nil {
		return ""
	}
	if db := strings.TrimSpace(info.Reserved["db_name"]); db != "" {
		return db
	}
	if db := strings.TrimSpace(info.Reserved["database"]); db != "" {
		return db
	}
	return ""
}

// getCommandsForClientWithID fetches commands for a specific client using the provided clientID
// This avoids regenerating clientID which could cause inconsistency
// CommandStore handles all caching internally with TTL, so we just call it directly
func (m *TelemetryManager) getCommandsForClientWithID(clientID string, req *milvuspb.ClientHeartbeatRequest) []*commonpb.ClientCommand {
	if m.commandStore == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fetch commands from CommandStore (handles caching internally)
	commands, err := m.commandStore.ListCommands(ctx)
	if err != nil {
		log.Ctx(ctx).Warn("getCommandsForClientWithID: failed to fetch commands from CommandStore",
			zap.Error(err))
		return nil
	}

	// Fetch configs from CommandStore (handles caching internally)
	configs, _, err := m.commandStore.ListConfigs(ctx)
	if err != nil {
		log.Ctx(ctx).Warn("getCommandsForClientWithID: failed to fetch configs from CommandStore",
			zap.Error(err))
		return nil
	}

	// Filter and build result from fetched data
	var result []*commonpb.ClientCommand

	// Filter one-time commands by scope and timestamp
	for _, cmd := range commands {
		if !m.matchesScope(cmd.TargetScope, clientID, req.ClientInfo) {
			continue
		}
		// One-time commands: return if newer than last command timestamp
		if cmd.CreateTime > req.LastCommandTimestamp {
			result = append(result, cmd)
		}
	}

	// Filter persistent configs by scope first, then compute hash over filtered configs
	// This ensures the hash matches what the client will compute over the configs it receives
	var filteredConfigs []*ClientConfig
	for _, cfg := range configs {
		if m.matchesScope(cfg.TargetScope, clientID, req.ClientInfo) {
			filteredConfigs = append(filteredConfigs, cfg)
		}
	}

	// Compute hash only over configs this client will receive
	filteredConfigHash := computeClientConfigHash(filteredConfigs)

	// Only send configs if client's hash differs from filtered hash
	if req.ConfigHash != filteredConfigHash {
		for _, cfg := range filteredConfigs {
			// Convert config to command format for response
			result = append(result, &commonpb.ClientCommand{
				CommandId:   cfg.ConfigId,
				CommandType: cfg.ConfigType,
				Payload:     cfg.Payload,
				CreateTime:  cfg.CreateTime,
				TargetScope: cfg.TargetScope,
				Persistent:  true, // Mark as persistent so client knows to track it
			})
		}
	}

	return result
}

func (m *TelemetryManager) matchesScope(scope, clientID string, info *commonpb.ClientInfo) bool {
	if scope == "global" {
		return true
	}
	if strings.HasPrefix(scope, "client:") {
		return scope == fmt.Sprintf("client:%s", clientID)
	}
	if strings.HasPrefix(scope, "database:") {
		targetDB := strings.TrimPrefix(scope, "database:")
		// Check if client has accessed this database
		if existing, loaded := m.clientMetrics.Load(clientID); loaded {
			cache := existing.(*ClientMetricsCache)
			_, hasAccess := cache.AccessedDatabases.Load(targetDB)
			return hasAccess
		}
		return false
	}
	return false
}

// computeClientConfigHash computes hash over configs that a specific client will receive
// This ensures hash comparison works correctly when configs have different scopes
func computeClientConfigHash(configs []*ClientConfig) string {
	if len(configs) == 0 {
		return ""
	}

	// Sort by config ID for consistent hash
	sorted := make([]*ClientConfig, len(configs))
	copy(sorted, configs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ConfigId < sorted[j].ConfigId
	})

	h := sha256.New()
	for _, cfg := range sorted {
		h.Write([]byte(cfg.ConfigId))
		h.Write([]byte(cfg.ConfigType))
		h.Write(cfg.Payload)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ListClients returns all clients, optionally filtered by database
func (m *TelemetryManager) ListClients(database string) []*ClientMetricsCache {
	var result []*ClientMetricsCache
	m.clientMetrics.Range(func(key, value any) bool {
		cache := value.(*ClientMetricsCache)
		if database == "" {
			result = append(result, cache)
		} else if _, ok := cache.AccessedDatabases.Load(database); ok {
			result = append(result, cache)
		}
		return true
	})
	return result
}

// GetClientTelemetry returns telemetry data for clients
func (m *TelemetryManager) GetClientTelemetry(req *milvuspb.GetClientTelemetryRequest) (*milvuspb.GetClientTelemetryResponse, error) {
	var clients []*milvuspb.ClientTelemetry
	aggregated := &commonpb.Metrics{}

	m.clientMetrics.Range(func(key, value any) bool {
		clientID := key.(string)
		cache := value.(*ClientMetricsCache)

		// Filter by client_id if specified
		if req.ClientId != "" && clientID != req.ClientId {
			return true
		}

		// Filter by database if specified
		if req.Database != "" {
			if _, ok := cache.AccessedDatabases.Load(req.Database); !ok {
				return true
			}
		}

		ct := &milvuspb.ClientTelemetry{
			ClientInfo:        cloneClientInfo(cache.ClientInfo),
			LastHeartbeatTime: cache.LastHeartbeat.Load() / int64(time.Millisecond),
			Status:            m.getClientStatus(cache),
			Databases:         m.getDatabaseList(cache),
		}

		// Ensure client_id is always set in Reserved (for legacy clients that don't have it)
		if ct.ClientInfo == nil {
			ct.ClientInfo = &commonpb.ClientInfo{}
		}
		if ct.ClientInfo.Reserved == nil {
			ct.ClientInfo.Reserved = make(map[string]string)
		}
		if ct.ClientInfo.Reserved["client_id"] == "" {
			ct.ClientInfo.Reserved["client_id"] = clientID
		}

		if req.IncludeMetrics {
			ct.Metrics = cloneOperationMetrics(cache.LatestMetrics)
		}

		// Add command replies to ClientInfo.Reserved if there are any
		if len(cache.CommandReplies) > 0 {
			if ct.ClientInfo == nil {
				ct.ClientInfo = &commonpb.ClientInfo{}
			}
			if ct.ClientInfo.Reserved == nil {
				ct.ClientInfo.Reserved = make(map[string]string)
			}
			// JSON encode command replies and store in Reserved
			if repliesJSON, err := json.Marshal(cache.CommandReplies); err == nil {
				ct.ClientInfo.Reserved["command_replies"] = string(repliesJSON)
			}
		}

		clients = append(clients, ct)

		// Aggregate metrics
		for _, opMetrics := range cache.LatestMetrics {
			if opMetrics.Global != nil {
				aggregated.RequestCount += opMetrics.Global.RequestCount
				aggregated.SuccessCount += opMetrics.Global.SuccessCount
				aggregated.ErrorCount += opMetrics.Global.ErrorCount
			}
		}
		return true
	})

	return &milvuspb.GetClientTelemetryResponse{
		Status:     &commonpb.Status{},
		Clients:    clients,
		Aggregated: aggregated,
	}, nil
}

func cloneClientInfo(info *commonpb.ClientInfo) *commonpb.ClientInfo {
	if info == nil {
		return nil
	}
	clone := proto.Clone(info)
	if ci, ok := clone.(*commonpb.ClientInfo); ok {
		return ci
	}
	return info
}

func cloneOperationMetrics(metrics []*commonpb.OperationMetrics) []*commonpb.OperationMetrics {
	if len(metrics) == 0 {
		return nil
	}
	result := make([]*commonpb.OperationMetrics, 0, len(metrics))
	for _, m := range metrics {
		if m == nil {
			result = append(result, nil)
			continue
		}
		clone := proto.Clone(m)
		if om, ok := clone.(*commonpb.OperationMetrics); ok {
			result = append(result, om)
		} else {
			result = append(result, m)
		}
	}
	return result
}

func (m *TelemetryManager) getClientStatus(cache *ClientMetricsCache) string {
	lastHeartbeat := time.Unix(0, cache.LastHeartbeat.Load())
	if time.Since(lastHeartbeat) > m.config.ClientStatusThreshold {
		return "inactive"
	}
	return "active"
}

func (m *TelemetryManager) getDatabaseList(cache *ClientMetricsCache) []string {
	var dbs []string
	cache.AccessedDatabases.Range(func(key, value any) bool {
		dbs = append(dbs, key.(string))
		return true
	})
	return dbs
}

// PushCommand stores a command to be sent to clients
func (m *TelemetryManager) PushCommand(ctx context.Context, req *milvuspb.PushClientCommandRequest) (*milvuspb.PushClientCommandResponse, error) {
	if m.commandStore == nil {
		// Non-retriable: service not ready
		err := merr.WrapErrServiceNotReady("telemetry", 0, "command_store_not_initialized",
			"command store not initialized")
		log.Ctx(ctx).Warn("PushCommand: command store not initialized",
			zap.Error(err))
		return nil, err
	}
	cmdID, err := m.commandStore.PushCommand(ctx, req)
	if err != nil {
		// Errors from commandStore are already wrapped with merr
		log.Ctx(ctx).Warn("PushCommand: failed to push command",
			zap.Error(err),
			zap.String("command_type", req.CommandType),
			zap.Bool("persistent", req.Persistent))
		return nil, err
	}
	log.Ctx(ctx).Debug("PushCommand: command pushed successfully",
		zap.String("command_id", cmdID),
		zap.String("command_type", req.CommandType),
		zap.Bool("persistent", req.Persistent))
	return &milvuspb.PushClientCommandResponse{
		Status:    &commonpb.Status{},
		CommandId: cmdID,
	}, nil
}

// DeleteCommand removes a command
func (m *TelemetryManager) DeleteCommand(ctx context.Context, req *milvuspb.DeleteClientCommandRequest) (*milvuspb.DeleteClientCommandResponse, error) {
	if m.commandStore == nil {
		// Non-retriable: service not ready
		err := merr.WrapErrServiceNotReady("telemetry", 0, "command_store_not_initialized",
			"command store not initialized")
		log.Ctx(ctx).Warn("DeleteCommand: command store not initialized",
			zap.Error(err))
		return nil, err
	}
	err := m.commandStore.DeleteCommand(ctx, req.CommandId)
	if err != nil {
		// Errors from commandStore are already wrapped with merr
		log.Ctx(ctx).Warn("DeleteCommand: failed to delete command",
			zap.Error(err),
			zap.String("command_id", req.CommandId))
		return nil, err
	}
	log.Ctx(ctx).Debug("DeleteCommand: command deleted successfully",
		zap.String("command_id", req.CommandId))
	return &milvuspb.DeleteClientCommandResponse{
		Status: &commonpb.Status{},
	}, nil
}

// initializeCommandHandlers sets up default command handlers for all command types
func (m *TelemetryManager) initializeCommandHandlers() {
	// Show errors handler - display last 100 error messages
	m.commandRouter.RegisterHandler(CommandTypeShowErrors, NewShowErrorsHandler(nil))

	// Collection metrics handler - enable fine-grained collection-level metrics
	m.commandRouter.RegisterHandler(CommandTypeCollectionMetrics, NewCollectionMetricsHandler())

	// Push config handler - push persistent configuration to clients
	m.commandRouter.RegisterHandler(CommandTypePushConfig, NewPushConfigHandler())
}

// SetErrorCollector sets the error collector for the show_errors command handler
func (m *TelemetryManager) SetErrorCollector(collector ErrorCollector) {
	m.commandRouter.RegisterHandler(CommandTypeShowErrors, NewShowErrorsHandler(collector))
}

// CommandInfo represents command information for API responses
type CommandInfo struct {
	CommandID   string `json:"command_id"`
	CommandType string `json:"command_type"`
	TargetScope string `json:"target_scope"`
	Persistent  bool   `json:"persistent"`
	CreateTime  int64  `json:"create_time"`
	TTLSeconds  int64  `json:"ttl_seconds,omitempty"`
}

// GetClientCommandReplies returns the stored command replies for a specific client
func (m *TelemetryManager) GetClientCommandReplies(clientID string) []*StoredCommandReply {
	existing, loaded := m.clientMetrics.Load(clientID)
	if !loaded {
		return nil
	}

	cache := existing.(*ClientMetricsCache)
	if len(cache.CommandReplies) == 0 {
		return nil
	}

	// Return a copy to avoid external modification
	result := make([]*StoredCommandReply, len(cache.CommandReplies))
	for i, reply := range cache.CommandReplies {
		copied := *reply
		result[i] = &copied
	}
	return result
}

// ListAllCommands returns all active commands (both one-time commands and persistent configs)
func (m *TelemetryManager) ListAllCommands(ctx context.Context) ([]*CommandInfo, error) {
	if m.commandStore == nil {
		return nil, nil
	}

	// Use ListCommandsWithInfo to get all commands including TTLSeconds
	cmdInfos, err := m.commandStore.ListCommandsWithInfo(ctx)
	if err != nil {
		log.Ctx(ctx).Warn("ListAllCommands: failed to list commands", zap.Error(err))
		return nil, err
	}

	var result []*CommandInfo
	for _, info := range cmdInfos {
		result = append(result, &CommandInfo{
			CommandID:   info.CommandID,
			CommandType: info.CommandType,
			TargetScope: info.TargetScope,
			Persistent:  info.Persistent,
			CreateTime:  info.CreateTime,
			TTLSeconds:  info.TTLSeconds,
		})
	}

	return result, nil
}
