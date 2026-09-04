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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TelemetryConfig holds configurable time values for the telemetry manager.
//
// Defaults come from DefaultTelemetryConfig and are exported as rootCoord.clientTelemetry.*;
// the numbers named below are those defaults, so keep the two in step.
type TelemetryConfig struct {
	// CleanupInterval is how often the cleanup loop runs (default: 1 minute)
	CleanupInterval time.Duration
	// InactiveClientThreshold is how long since last heartbeat before a client is removed (default: 10 minutes)
	InactiveClientThreshold time.Duration
	// ClientStatusThreshold is how long since last heartbeat before a client is marked inactive (default: 1 minute)
	ClientStatusThreshold time.Duration
	// CommandCleanupTimeout is the context timeout for command cleanup operations (default: 10 seconds)
	CommandCleanupTimeout time.Duration
	// MaxMetricsPerClient is the maximum size of metrics payload per client (default: 1MB).
	// Zero disables the cap; see normalize.
	MaxMetricsPerClient int
	// MaxOperationTypesPerClient is the maximum number of operation types per client (default: 100)
	MaxOperationTypesPerClient int
	// MaxClientsInMemory is the maximum number of clients to track in memory (default: 100,000)
	// This prevents unbounded memory growth from malicious or misconfigured clients
	MaxClientsInMemory int
	// RetainedWindows is how many heartbeat windows to keep per client (default: 2).
	//
	// A telemetry query is answered from the oldest retained window, so this is also how
	// far the answer lags the client and how many consecutive idle intervals it survives:
	// at 2, one quiet interval cannot blank the view; at 3, two cannot. Each extra window
	// is another full copy of every operation and per-collection breakdown for every
	// connected client, and a client silent for several intervals is better described by
	// its status than by metrics from minutes ago -- so raise it for deployments whose
	// clients heartbeat frequently, not as a way to remember long-gone traffic.
	RetainedWindows int
}

// defaultRetainedWindows is the smallest retention that survives one idle interval: one
// window to serve and one to absorb the quiet one.
const defaultRetainedWindows = 2

// The heartbeat response has no authoritative config hash, so an empty command list is
// ambiguous to existing clients: it can mean either "your non-empty hash still matches" or
// "the effective config set is now empty". When the latter follows deletion of the last
// matching config, send this stable no-op persistent command once. All telemetry SDKs accept
// an empty push_config object without changing runtime settings and compute the same hash from
// command ID, type, and payload.
//
// Empty-string and sentinel hashes are both accepted as the empty state. That keeps fresh
// clients on the original empty hash while allowing a client with a stale non-empty hash to
// converge without a protobuf change. The sentinel is synthesized at response time; it must
// never be stored or exposed through the command APIs.
const (
	emptyConfigSentinelCommandID = "00000000-0000-0000-0000-000000000000"
	emptyConfigSentinelHash      = "d34aea1518ff0217"
)

func newEmptyConfigSentinelCommand() *commonpb.ClientCommand {
	return &commonpb.ClientCommand{
		CommandId:   emptyConfigSentinelCommandID,
		CommandType: "push_config",
		Payload:     []byte("{}"),
		CreateTime:  0,
		TargetScope: "global",
		Persistent:  true,
	}
}

// normalize replaces values that cannot mean what they say with their defaults.
//
// It exists because a caller may build a TelemetryConfig field by field and leave zeroes
// behind, and for most of these fields a zero is not a weaker setting but a broken one:
//
//   - CleanupInterval reaches time.NewTicker, which panics on a non-positive interval, so
//     a zero takes down the coordinator rather than merely misbehaving.
//   - InactiveClientThreshold and MaxClientsInMemory become "evict everyone on every
//     sweep", and ClientStatusThreshold becomes "every client is inactive".
//   - CommandCleanupTimeout becomes an already-expired context, so expired commands are
//     never actually removed.
//   - MaxOperationTypesPerClient truncates every heartbeat to no operations at all, which
//     is the whole payload gone.
//   - RetainedWindows leaves no window to answer a query from, so every client reports
//     nothing forever.
//
// Silently behaving that way is far worse than ignoring the value, and no caller can have
// meant any of it.
//
// MaxMetricsPerClient is deliberately not normalized. validateAndTruncateMetrics enforces
// it only when positive, so zero already means "no cap" -- an operator who removed the
// limit on purpose would get the default back if this touched it.
//
// Every path that installs a config runs this, so the invariant has one owner rather than
// a check at each use.
func (c *TelemetryConfig) normalize() {
	defaults := DefaultTelemetryConfig()
	if c.CleanupInterval <= 0 {
		c.CleanupInterval = defaults.CleanupInterval
	}
	if c.InactiveClientThreshold <= 0 {
		c.InactiveClientThreshold = defaults.InactiveClientThreshold
	}
	if c.ClientStatusThreshold <= 0 {
		c.ClientStatusThreshold = defaults.ClientStatusThreshold
	}
	if c.CommandCleanupTimeout <= 0 {
		c.CommandCleanupTimeout = defaults.CommandCleanupTimeout
	}
	if c.MaxOperationTypesPerClient < 1 {
		c.MaxOperationTypesPerClient = defaults.MaxOperationTypesPerClient
	}
	if c.MaxClientsInMemory < 1 {
		c.MaxClientsInMemory = defaults.MaxClientsInMemory
	}
	if c.MaxMetricsPerClient < 0 {
		// Negative is not the documented "no cap" spelling, and the enforcement branch
		// would skip it just the same, so treat it as the mistake it is.
		c.MaxMetricsPerClient = defaults.MaxMetricsPerClient
	}
	if c.RetainedWindows < 1 {
		c.RetainedWindows = defaultRetainedWindows
	}
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
		RetainedWindows:            defaultRetainedWindows,
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

// ClientMetricsCache stores the latest metrics from a client.
//
// One cache belongs to one client, but that is not the same as one goroutine: a client's
// heartbeat writes it while any number of admin readers -- GetClientTelemetry from the
// WebUI or the REST API, the reply endpoints, the inactive-client sweeper -- read it
// concurrently. So every field is either guarded by mu or individually concurrency-safe;
// see the two groups below.
type ClientMetricsCache struct {
	// mu guards the four fields below it. They are written together by a heartbeat and read
	// together by a telemetry query, so one lock for the group is both sufficient and the
	// only way to hand a reader a self-consistent view: metrics and replies that came from
	// the same heartbeat rather than a mix of two.
	//
	// Readers copy under RLock and do the expensive work -- proto cloning, JSON encoding --
	// after releasing it. That is safe because these values are never mutated in place: a
	// heartbeat appends one window wholesale, and CommandReplies is only ever appended
	// to or resliced, never written through. Holding the lock across a JSON encode of up to
	// fifty replies would stall the heartbeat that is trying to record the next one.
	mu         sync.RWMutex
	ClientInfo *commonpb.ClientInfo
	// windows holds the TelemetryConfig.RetainedWindows most recent heartbeat windows, oldest first.
	//
	// Each heartbeat carries the operations since the previous one, and the counters behind
	// it are reset as the client takes the snapshot, so a window is the whole record of an
	// interval and there is nothing to accumulate across them. A heartbeat that carried no
	// traffic is still a window and still takes a slot: an idle interval is data, not a gap.
	//
	// Retaining more than one exists so that a single idle interval does not blank the view
	// of a client that is plainly still there -- see the note on servedMetricsLocked.
	windows        [][]*commonpb.OperationMetrics
	ConfigHash     string
	LastCommandTS  int64
	CommandReplies []*StoredCommandReply // Last N command replies from this client

	// The rest carry their own synchronization and must not be read under mu, so that the
	// paths that only need liveness -- the sweeper, the persistent-target check -- stay off
	// the lock entirely.
	//
	// ClientID is written once when the cache is created and never again.
	ClientID          string
	LastHeartbeat     atomic.Int64 // Unix nanoseconds for atomic access
	AccessedDatabases sync.Map     // map[string]struct{} for concurrent access

	// ClientIDStable mirrors ClientInfo.Reserved[clientIDStableKey] so validatePersistentTarget
	// can consult it from an admin goroutine without taking mu.
	ClientIDStable atomic.Bool
}

// servedMetricsLocked returns the window a telemetry query should report: last, or current
// while last does not exist yet.
//
// A client's two retained windows are current -- the interval its most recent heartbeat
// closed -- and last, the one before it. Reporting current would mean a client that idles
// for a single interval reports nothing, even though it is connected and was busy moments
// earlier: current truthfully says "no traffic since the last heartbeat", which reads as
// "this client does nothing" to anyone looking at the API. Reporting last instead trades one
// heartbeat interval of freshness for a view that a single quiet interval cannot blank.
//
// Until a second heartbeat arrives there is no last, and current is served rather than
// nothing, so a client is visible from its first heartbeat instead of one interval later.
//
// Which window is served is positional, never conditional on what is in it: an empty window
// is served like any other, and an idle client reports nothing once both windows are empty.
// Reaching further back for the last window that happened to carry traffic would report
// activity from an unbounded and unstated time ago as if it were current.
//
// Caller must hold c.mu.
func (c *ClientMetricsCache) servedMetricsLocked() []*commonpb.OperationMetrics {
	if len(c.windows) == 0 {
		return nil
	}
	return c.windows[0]
}

// snapshot copies the guarded fields so a caller can clone, encode and aggregate without
// holding mu. See the note on mu for why sharing these pointers past the unlock is safe.
func (c *ClientMetricsCache) snapshot() (*commonpb.ClientInfo, []*commonpb.OperationMetrics, []*StoredCommandReply) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ClientInfo, c.servedMetricsLocked(), c.CommandReplies
}

// LatestWindow returns the metrics from this client's most recent heartbeat, which is what
// it reported for the interval that just ended rather than what a telemetry query serves.
func (c *ClientMetricsCache) LatestWindow() []*commonpb.OperationMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.windows) == 0 {
		return nil
	}
	return c.windows[len(c.windows)-1]
}

// storeHeartbeat records everything a heartbeat carries in one critical section, so a
// concurrent reader never sees the metrics of one heartbeat beside the config hash of
// another.
// retain is TelemetryConfig.RetainedWindows, passed in because the cache is per client
// while the setting belongs to the manager. It is already normalized to at least 1, so a
// lowered setting takes effect on the next heartbeat by dropping the surplus windows.
func (c *ClientMetricsCache) storeHeartbeat(info *commonpb.ClientInfo, metrics []*commonpb.OperationMetrics, configHash string, lastCommandTS int64, retain int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ClientInfo = info
	c.windows = append(c.windows, metrics)
	if len(c.windows) > retain {
		// Shift the survivors down rather than resliced off the front, so the dropped
		// windows' slots are overwritten and stop keeping their metrics alive. Reslicing
		// would leave those pointers in the backing array for as long as the client stays
		// connected.
		c.windows = append(c.windows[:0], c.windows[len(c.windows)-retain:]...)
	}
	c.ConfigHash = configHash
	c.LastCommandTS = lastCommandTS
}

// appendReplies adds this heartbeat's replies and trims the history to the most recent
// maxStoredReplies.
func (c *ClientMetricsCache) appendReplies(stored []*StoredCommandReply, maxStoredReplies int) {
	if len(stored) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CommandReplies = append(c.CommandReplies, stored...)
	if len(c.CommandReplies) > maxStoredReplies {
		c.CommandReplies = c.CommandReplies[len(c.CommandReplies)-maxStoredReplies:]
	}
}

// replies returns the stored replies. The slice is a copy, but the elements are shared;
// callers that hand them outside the package must copy the values.
func (c *ClientMetricsCache) replies() []*StoredCommandReply {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.CommandReplies) == 0 {
		return nil
	}
	return append([]*StoredCommandReply(nil), c.CommandReplies...)
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
	config.normalize()

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
	if config == nil {
		config = DefaultTelemetryConfig()
	}
	config.normalize()
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
			mlog.Debug(ctx, "cleanupInactiveClients: removed inactive client",
				mlog.String("client_id", clientID),
				mlog.Duration("inactive_duration", inactiveDuration),
				mlog.Duration("threshold", m.config.InactiveClientThreshold))
		}
		return true
	})
	if cleaned > 0 {
		mlog.Debug(ctx, "cleanupInactiveClients: normal cleanup completed",
			mlog.Int("cleaned_count", cleaned))
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
		mlog.Warn(ctx, "telemetry client count exceeds limit, LRU eviction required",
			mlog.Int("current_count", len(entries)),
			mlog.Int("max_allowed", m.config.MaxClientsInMemory),
			mlog.Int("to_evict", toEvict))

		for i := 0; i < toEvict && i < len(entries); i++ {
			m.clientMetrics.Delete(entries[i].clientID)
			mlog.Debug(ctx, "cleanupInactiveClients: LRU evicted client",
				mlog.String("client_id", entries[i].clientID),
				mlog.Time("last_heartbeat", entries[i].lastHeartbeat))
		}

		mlog.Info(ctx, "cleanupInactiveClients: LRU eviction completed",
			mlog.Int("evicted_count", toEvict),
			mlog.Int("max_allowed", m.config.MaxClientsInMemory))
	}
}

// cleanupExpiredCommands removes expired commands from etcd
func (m *TelemetryManager) cleanupExpiredCommands(ctx context.Context) {
	if m.commandStore == nil {
		mlog.Debug(ctx, "cleanupExpiredCommands: command store not initialized")
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
		// Fully initialize before publishing. The moment LoadOrStore returns, an admin
		// goroutine can find this cache and read ClientIDStable -- and a zero value there
		// means "generated ID", which validatePersistentTarget rejects as a non-retriable
		// ParameterInvalid. A client that declared a stable ID would be turned away on the
		// strength of a field that had simply not been written yet.
		cache.ClientIDStable.Store(declaresStableClientID(req.GetClientInfo()))
		// Use LoadOrStore to handle race condition
		if actual, loaded := m.clientMetrics.LoadOrStore(clientID, cache); loaded {
			cache = actual.(*ClientMetricsCache)
		}
	}

	// One client owns one cache, but readers -- the WebUI, the REST endpoints, the
	// inactive-client sweeper -- run concurrently with this write, so it goes through the
	// cache's own synchronization rather than assigning the fields directly.
	cache.storeHeartbeat(
		req.ClientInfo,
		m.validateAndTruncateMetrics(req.Metrics), // Validate and truncate metrics
		req.ConfigHash,
		req.LastCommandTimestamp,
		m.config.RetainedWindows,
	)
	cache.ClientIDStable.Store(declaresStableClientID(req.GetClientInfo()))
	cache.LastHeartbeat.Store(time.Now().UnixNano())
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

	// Build the batch outside the cache lock: lookupCommandInfo reaches into the command
	// store, and holding the client's lock across that would couple two unrelated
	// subsystems' contention.
	stored := make([]*StoredCommandReply, 0, len(replies))
	for _, reply := range replies {
		if reply == nil {
			continue
		}
		// The empty-config sentinel is synthesized and has no command-store entry. Its ACK
		// only confirms that the client adopted the sentinel hash; retaining it would expose
		// a phantom command in reply history and make cleanup probe a reserved ID.
		if reply.CommandId == emptyConfigSentinelCommandID {
			continue
		}

		cmdType, cmdPayload := m.lookupCommandInfo(reply.CommandId)
		stored = append(stored, &StoredCommandReply{
			CommandID:      reply.CommandId,
			CommandType:    cmdType,
			CommandPayload: string(cmdPayload), // Convert []byte to string for JSON serialization
			Success:        reply.Success,
			ErrorMsg:       reply.ErrorMessage,
			Payload:        string(reply.Payload), // Convert []byte to string for JSON serialization
			ReceivedAt:     now,
		})

		if !reply.Success {
			mlog.Warn(context.TODO(), "processCommandReplies: command execution failed",
				mlog.String("client_id", cache.ClientID),
				mlog.String("command_id", reply.CommandId),
				mlog.String("command_type", cmdType),
				mlog.String("error", reply.ErrorMessage))
		}

		if reply.CommandId != "" {
			deletedIDs = append(deletedIDs, reply.CommandId)
		}
	}

	cache.appendReplies(stored, maxStoredReplies)

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

// cleanupRepliedCommands retires the commands this heartbeat answered.
//
// Only client-scoped commands are removed here; see DeleteCommandOnReply for why a
// broadcast command must outlive its first reply.
func (m *TelemetryManager) cleanupRepliedCommands(commandIDs []string) {
	if m.commandStore == nil {
		return
	}
	for _, id := range commandIDs {
		if id == "" {
			continue
		}
		m.commandStore.DeleteCommandOnReply(id)
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
		mlog.Warn(ctx, "getCommandsForClientWithID: failed to fetch commands from CommandStore",
			mlog.Err(err))
		return nil
	}

	// Fetch configs from CommandStore (handles caching internally)
	configs, _, err := m.commandStore.ListConfigs(ctx)
	if err != nil {
		mlog.Warn(ctx, "getCommandsForClientWithID: failed to fetch configs from CommandStore",
			mlog.Err(err))
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

	// The wire response has no authoritative hash. If the last matching config was deleted,
	// an existing client still reports its old non-empty hash but an ordinary empty response
	// cannot tell it to clear that hash. Transition only that stale state through a stable
	// no-op persistent command. Fresh clients keep using the original empty hash, while both
	// empty representations suppress future sentinel delivery.
	if len(filteredConfigs) == 0 {
		if req.ConfigHash != "" && req.ConfigHash != emptyConfigSentinelHash {
			result = append(result, newEmptyConfigSentinelCommand())
		}
		return result
	}

	// Compute hash only over configs this client will receive.
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

		// One snapshot for the whole entry, so the reply set and the metrics reported here
		// come from the same heartbeat, and the expensive clone/encode below happens off
		// the cache lock.
		info, latestMetrics, storedReplies := cache.snapshot()

		ct := &milvuspb.ClientTelemetry{
			ClientInfo:        cloneClientInfo(info),
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
			ct.Metrics = cloneOperationMetrics(latestMetrics)
		}

		// Add command replies to ClientInfo.Reserved if there are any
		if len(storedReplies) > 0 {
			if ct.ClientInfo == nil {
				ct.ClientInfo = &commonpb.ClientInfo{}
			}
			if ct.ClientInfo.Reserved == nil {
				ct.ClientInfo.Reserved = make(map[string]string)
			}
			// JSON encode command replies and store in Reserved
			if repliesJSON, err := json.Marshal(storedReplies); err == nil {
				ct.ClientInfo.Reserved["command_replies"] = string(repliesJSON)
			}
		}

		clients = append(clients, ct)

		// Aggregate metrics
		for _, opMetrics := range latestMetrics {
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

// validatePersistentTarget rejects a persistent config aimed at a client whose ID will not
// survive its restart.
//
// A persistent config is keyed by target scope, so if the target ID changes the config
// stops matching, silently, while remaining in etcd. That is the common case: the SDK
// generates a per-process UUID by default. But it is not universal -- a client that sets
// TelemetryConfig.ClientID keeps its ID across restarts, and for those a persistent
// client-scoped config is exactly right. So this decides on the client's declared
// identity, not on the scope.
//
// An unknown target is rejected rather than assumed stable: the whole failure mode being
// prevented is a config that looks accepted and never applies.
//
// Reads ClientMetricsCache.ClientIDStable rather than ClientInfo so this admin path stays
// off the cache lock entirely; the two carry the same answer.
func (m *TelemetryManager) validatePersistentTarget(req *milvuspb.PushClientCommandRequest) error {
	if !req.GetPersistent() || req.GetTargetClientId() == "" {
		return nil
	}

	clientID := req.GetTargetClientId()
	value, loaded := m.clientMetrics.Load(clientID)
	if !loaded {
		// Retriable, not an input error. clientMetrics is RootCoord-local memory rebuilt
		// only from heartbeats, so after a restart or failover it is empty for up to a full
		// heartbeat interval -- 30s by default, and the server is never told what a client
		// actually uses. A correct request for a legitimately pinned client lands here
		// purely because the cache is cold, and classifying that as a non-retriable
		// ParameterInvalid makes a provisioning script give up on something that would
		// succeed seconds later. The server cannot tell "never existed" from "has not
		// heartbeated yet", and retrying a typo costs far less than silently defeating a
		// correct request, so it reports the transient reading. Same trade-off as the note
		// on ErrCollectionNotFound in pkg/util/merr/errors.go.
		return merr.WrapErrServiceNotReadyMsg(
			"cannot push a persistent config to client %q yet: it is not currently known to "+
				"this coordinator, either because it has not heartbeated since the coordinator "+
				"started or because no such client exists. Retry once it has heartbeated; a "+
				"persistent client-scoped config also requires the client to set a stable "+
				"TelemetryConfig.ClientID, otherwise target a database or global scope",
			clientID)
	}

	if !value.(*ClientMetricsCache).ClientIDStable.Load() {
		return merr.WrapErrParameterInvalidMsg(
			"cannot push a persistent config to client %q: it uses a generated client ID, "+
				"which changes on restart, so the config would stop applying and could never "+
				"match again. Push it as a one-time command (persistent=false) to configure the "+
				"running client, set a stable TelemetryConfig.ClientID on the client, or target "+
				"a database or global scope",
			clientID)
	}

	return nil
}

// declaresStableClientID reports whether the client said its ID is configured rather than
// generated, via ClientInfo.Reserved. Clients that do not report either way are treated as
// unstable, which is what every client predating that field is.
func declaresStableClientID(info *commonpb.ClientInfo) bool {
	return info.GetReserved()[clientIDStableKey] == "true"
}

// PushCommand stores a command to be sent to clients.
func (m *TelemetryManager) PushCommand(ctx context.Context, req *milvuspb.PushClientCommandRequest) (*milvuspb.PushClientCommandResponse, error) {
	if m.commandStore == nil {
		// Non-retriable: service not ready
		err := merr.WrapErrServiceNotReady("telemetry", 0, "command_store_not_initialized",
			"command store not initialized")
		mlog.Warn(ctx, "PushCommand: command store not initialized",
			mlog.Err(err))
		return nil, err
	}
	if err := m.validatePersistentTarget(req); err != nil {
		mlog.Warn(ctx, "PushCommand: rejected persistent config",
			mlog.Err(err),
			mlog.String("target_client_id", req.GetTargetClientId()))
		return nil, err
	}

	cmdID, err := m.commandStore.PushCommand(ctx, req)
	if err != nil {
		// Errors from commandStore are already wrapped with merr
		mlog.Warn(ctx, "PushCommand: failed to push command",
			mlog.Err(err),
			mlog.String("command_type", req.CommandType),
			mlog.Bool("persistent", req.Persistent))
		return nil, err
	}
	mlog.Debug(ctx, "PushCommand: command pushed successfully",
		mlog.String("command_id", cmdID),
		mlog.String("command_type", req.CommandType),
		mlog.Bool("persistent", req.Persistent))
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
		mlog.Warn(ctx, "DeleteCommand: command store not initialized",
			mlog.Err(err))
		return nil, err
	}
	err := m.commandStore.DeleteCommand(ctx, req.CommandId)
	if err != nil {
		// Errors from commandStore are already wrapped with merr
		mlog.Warn(ctx, "DeleteCommand: failed to delete command",
			mlog.Err(err),
			mlog.String("command_id", req.CommandId))
		return nil, err
	}
	mlog.Debug(ctx, "DeleteCommand: command deleted successfully",
		mlog.String("command_id", req.CommandId))
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

	stored := existing.(*ClientMetricsCache).replies()
	if len(stored) == 0 {
		return nil
	}

	// Return a copy to avoid external modification
	result := make([]*StoredCommandReply, len(stored))
	for i, reply := range stored {
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
		mlog.Warn(ctx, "ListAllCommands: failed to list commands", mlog.Err(err))
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
