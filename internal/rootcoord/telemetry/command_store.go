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
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PushClientConfigRequest is a request to push a persistent config
// TODO: Move to proto definition
type PushClientConfigRequest struct {
	ConfigType     string
	Payload        []byte
	TargetClientId string
}

// ClientConfig represents a persistent configuration for clients
// TODO: Move to proto definition
type ClientConfig struct {
	ConfigId    string
	ConfigType  string
	Payload     []byte
	CreateTime  int64
	TargetScope string
}

// CommandStoreInterface defines methods for command storage operations.
// Commands with Persistent=true are stored as persistent configs in etcd.
// Commands with Persistent=false are stored in memory with optional TTL.
type CommandStoreInterface interface {
	// Unified command/config operations
	PushCommand(ctx context.Context, req *milvuspb.PushClientCommandRequest) (string, error)
	ListCommands(ctx context.Context) ([]*commonpb.ClientCommand, error)
	ListConfigs(ctx context.Context) ([]*ClientConfig, string, error)
	DeleteCommand(ctx context.Context, commandID string) error
	CleanupExpiredCommands(ctx context.Context)
	// DeleteNonPersistentCommand removes a non-persistent command by ID (no-op for configs).
	DeleteNonPersistentCommand(commandID string) bool
	// DeleteCommandOnReply removes a replied one-time command, but only if it was aimed at
	// a single client; broadcast commands must survive until their TTL so every recipient
	// still gets them.
	DeleteCommandOnReply(commandID string) bool
	// GetCommandInfo returns command type and payload by ID for display/debugging.
	GetCommandInfo(commandID string) (commandType string, payload []byte, persistent bool, ok bool)
	// ListCommandsWithInfo returns all active commands with TTL information
	ListCommandsWithInfo(ctx context.Context) ([]*CommandInfoData, error)
}

// CommandInfoData contains command info including TTL for listing
type CommandInfoData struct {
	CommandID   string
	CommandType string
	TargetScope string
	Persistent  bool
	CreateTime  int64
	TTLSeconds  int64
}

// KVInterface abstracts the etcd client operations for testing
type KVInterface interface {
	Put(ctx context.Context, key, val string) error
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error
}

// etcdKVWrapper wraps clientv3.Client to implement KVInterface
type etcdKVWrapper struct {
	client *clientv3.Client
}

func (w *etcdKVWrapper) Put(ctx context.Context, key, val string) error {
	_, err := w.client.Put(ctx, key, val)
	return err
}

func (w *etcdKVWrapper) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return w.client.Get(ctx, key, opts...)
}

func (w *etcdKVWrapper) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error {
	_, err := w.client.Delete(ctx, key, opts...)
	return err
}

const (
	// clientScopePrefix marks a target scope naming a single client.
	clientScopePrefix = "client:"

	// clientIDStableKey is how a client declares, in ClientInfo.Reserved, that its client
	// ID was configured rather than generated and therefore survives a restart.
	clientIDStableKey = "client_id_stable"
)

// defaultCommandTTLSeconds bounds how long a one-time command pushed without a ttl_seconds
// survives, so a command nobody ever collects is eventually reclaimed instead of occupying
// RootCoord memory for the life of the process.
//
// It is a bound on memory, not a delivery window, and deliberately does not try to encode
// "N heartbeat cycles": HeartbeatInterval is client-side config with no upper bound, the
// server is not told what it is, and clients matched by one scope may use different values.
// An hour covers a client on a multi-minute interval, or one briefly disconnected.
const defaultCommandTTLSeconds = 3600

// resolveCommandTTL applies the default to a request that did not specify a TTL.
//
// ttl_seconds is `optional` in the proto precisely so this distinction exists: without
// presence, an omitted field and an explicit 0 decode identically, and any default applied
// here would silently redefine what an existing caller's 0 means -- while the proto
// documents 0 as "no expiry" and gives them no way to ask for it back.
//
//	absent -> defaultCommandTTLSeconds
//	0      -> 0, an explicit "never expire"
//	other  -> honored verbatim (negative also means never expire)
func resolveCommandTTL(req *milvuspb.PushClientCommandRequest) int64 {
	if req.TtlSeconds == nil {
		return defaultCommandTTLSeconds
	}
	return req.GetTtlSeconds()
}

// cache holds in-memory cache of all commands and configs
// Loaded at initialization and kept in sync with etcd on writes
type cache struct {
	commands   map[string]*storedCommand // commandID -> command
	configs    map[string]*storedConfig  // configID -> config
	configHash string                    // hash for client change detection
}

// CommandStore handles etcd storage for client configs and in-memory storage for commands.
// Persistent configs are stored in etcd and cached; non-persistent commands live in memory only.
type CommandStore struct {
	kv         KVInterface
	configPath string       // etcd path for persistent configs
	cache      *cache       // in-memory cache
	cacheMu    sync.RWMutex // protects cache
}

// Ensure CommandStore implements CommandStoreInterface
var _ CommandStoreInterface = (*CommandStore)(nil)

// storedCommand represents a one-time command with TTL
type storedCommand struct {
	CommandID   string `json:"command_id"`
	CommandType string `json:"command_type"`
	Payload     []byte `json:"payload"`
	CreateTime  int64  `json:"create_time"`
	TargetScope string `json:"target_scope"`
	TTLSeconds  int64  `json:"ttl_seconds"`
}

// storedConfig represents a persistent configuration
type storedConfig struct {
	ConfigID    string `json:"config_id"`
	ConfigType  string `json:"config_type"`
	Payload     []byte `json:"payload"`
	CreateTime  int64  `json:"create_time"`
	TargetScope string `json:"target_scope"`
}

// NewCommandStore creates a new CommandStore and loads configs from etcd
func NewCommandStore(client *clientv3.Client, basePath string) *CommandStore {
	store := &CommandStore{
		kv:         &etcdKVWrapper{client: client},
		configPath: basePath + "configs/",
		cache: &cache{
			commands: make(map[string]*storedCommand),
			configs:  make(map[string]*storedConfig),
		},
	}
	store.loadCache()
	return store
}

// NewCommandStoreWithKV creates a CommandStore with custom KV interface (for testing)
func NewCommandStoreWithKV(kv KVInterface, basePath string) *CommandStore {
	store := &CommandStore{
		kv:         kv,
		configPath: basePath + "configs/",
		cache: &cache{
			commands: make(map[string]*storedCommand),
			configs:  make(map[string]*storedConfig),
		},
	}
	store.loadCache()
	return store
}

// loadCache loads all configs from etcd into memory
func (s *CommandStore) loadCache() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	clientScoped := 0

	// Load configs
	if resp, err := s.kv.Get(ctx, s.configPath, clientv3.WithPrefix()); err != nil {
		mlog.Warn(ctx, "loadCache: failed to load configs", mlog.Err(err))
	} else {
		for _, kv := range resp.Kvs {
			var cfg storedConfig
			if err := json.Unmarshal(kv.Value, &cfg); err != nil {
				mlog.Warn(ctx, "loadCache: failed to unmarshal config",
					mlog.Err(err),
					mlog.String("key", string(kv.Key)))
				continue
			}
			// Client-scoped configs are loaded like any other. A client ID is not
			// necessarily ephemeral -- a client that sets TelemetryConfig.ClientID keeps
			// the same ID across restarts -- so the scope alone does not prove the config
			// is dead, and deleting operator-created configuration on startup because of
			// a guess is not something to do silently. They are counted so an operator
			// can see how many exist and retire them with DeleteClientCommand.
			if strings.HasPrefix(cfg.TargetScope, clientScopePrefix) {
				clientScoped++
			}
			s.cache.configs[cfg.ConfigID] = &cfg
		}
	}

	// Calculate config hash
	s.cache.configHash = s.computeConfigHash()

	if clientScoped > 0 {
		// Visibility, not a failure: these only keep matching if the target client uses a
		// stable TelemetryConfig.ClientID.
		mlog.Info(ctx, "loadCache: loaded client-scoped configs; these match only clients using a stable ClientID",
			mlog.Int("client_scoped_configs", clientScoped))
	}
	mlog.Info(ctx, "loadCache: completed",
		mlog.Int("commands", len(s.cache.commands)),
		mlog.Int("configs", len(s.cache.configs)))
}

// PushCommand stores a command/config in etcd and cache
// Persistent=true: stored as config (no TTL), Persistent=false: one-time command (with TTL)
func (s *CommandStore) PushCommand(ctx context.Context, req *milvuspb.PushClientCommandRequest) (string, error) {
	// Validate persistent command types
	if req.Persistent && req.CommandType != "push_config" {
		return "", merr.WrapErrParameterInvalid("push_config", req.CommandType,
			"only push_config can be persistent")
	}

	// Whether a client-scoped config may be persistent depends on whether the target's ID
	// survives a restart, which is client state only the manager can see. That check lives
	// in TelemetryManager.PushCommand.

	cmdID := uuid.New().String()
	scope := "global"
	if req.TargetClientId != "" {
		scope = "client:" + req.TargetClientId
	} else if req.TargetDatabase != "" {
		scope = "database:" + req.TargetDatabase
	}
	createTime := time.Now().UnixMilli()

	if req.Persistent {
		// Hold write lock during entire persistent config operation to prevent
		// read-modify-write race between getConfigIDsAndPayloadsLocked and etcd write.
		s.cacheMu.Lock()
		defer s.cacheMu.Unlock()
		// Keep only one config per (type, scope).
		existingIDs, existingPayloads := s.getConfigIDsAndPayloadsLocked(req.CommandType, scope)

		payload := req.Payload
		if req.CommandType == "push_config" && len(existingPayloads) > 0 {
			if merged, ok := mergeJSONPayloads(existingPayloads, payload); ok {
				payload = merged
			}
		}

		cfg := &storedConfig{
			ConfigID:    cmdID,
			ConfigType:  req.CommandType,
			Payload:     payload,
			CreateTime:  createTime,
			TargetScope: scope,
		}
		data, err := json.Marshal(cfg)
		if err != nil {
			return "", merr.WrapErrServiceInternal("marshal config: " + err.Error())
		}
		if err := s.kv.Put(ctx, s.configPath+cmdID, string(data)); err != nil {
			return "", merr.WrapErrIoFailed(s.configPath+cmdID, err)
		}
		// Best-effort cleanup of old configs with same key.
		failedDeletes := make(map[string]struct{})
		for _, id := range existingIDs {
			if err := s.kv.Delete(ctx, s.configPath+id); err != nil {
				mlog.Warn(ctx, "PushCommand: failed to delete old config",
					mlog.String("config_id", id),
					mlog.Err(err))
				failedDeletes[id] = struct{}{}
			}
		}
		for _, id := range existingIDs {
			if _, failed := failedDeletes[id]; failed {
				continue
			}
			delete(s.cache.configs, id)
		}
		s.cache.configs[cmdID] = cfg
		s.cache.configHash = s.computeConfigHash()
		// Note: cacheMu.Unlock() is handled by defer at line 232
	} else {
		cmd := &storedCommand{
			CommandID:   cmdID,
			CommandType: req.CommandType,
			Payload:     req.Payload,
			CreateTime:  createTime,
			TargetScope: scope,
			// Resolved once at push time: absent gets the default, an explicit 0 stays
			// "never expire". See resolveCommandTTL.
			TTLSeconds: resolveCommandTTL(req),
		}
		// Update cache
		s.cacheMu.Lock()
		s.cache.commands[cmdID] = cmd
		s.cacheMu.Unlock()
	}

	return cmdID, nil
}

// getConfigIDsAndPayloadsLocked returns existing config IDs and payloads for the given type and scope.
// Caller must hold s.cacheMu (read or write lock).
func (s *CommandStore) getConfigIDsAndPayloadsLocked(configType, scope string) ([]string, [][]byte) {
	var ids []string
	var payloads [][]byte
	for id, cfg := range s.cache.configs {
		if cfg.ConfigType == configType && cfg.TargetScope == scope {
			ids = append(ids, id)
			if len(cfg.Payload) > 0 {
				payloads = append(payloads, cfg.Payload)
			}
		}
	}
	return ids, payloads
}

func mergeJSONPayloads(existingPayloads [][]byte, newPayload []byte) ([]byte, bool) {
	if len(newPayload) == 0 {
		return nil, false
	}

	var newMap map[string]interface{}
	if err := json.Unmarshal(newPayload, &newMap); err != nil {
		return nil, false
	}

	merged := make(map[string]interface{})
	for _, p := range existingPayloads {
		var m map[string]interface{}
		if err := json.Unmarshal(p, &m); err != nil {
			continue
		}
		for k, v := range m {
			merged[k] = v
		}
	}
	for k, v := range newMap {
		merged[k] = v
	}

	out, err := json.Marshal(merged)
	if err != nil {
		return nil, false
	}
	return out, true
}

// ListCommands returns all non-expired commands from cache
func (s *CommandStore) ListCommands(ctx context.Context) ([]*commonpb.ClientCommand, error) {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	now := time.Now().UnixMilli()
	var commands []*commonpb.ClientCommand

	for _, cmd := range s.cache.commands {
		// Skip expired commands
		if cmd.TTLSeconds > 0 && now > cmd.CreateTime+cmd.TTLSeconds*1000 {
			continue
		}
		commands = append(commands, &commonpb.ClientCommand{
			CommandId:   cmd.CommandID,
			CommandType: cmd.CommandType,
			Payload:     cmd.Payload,
			CreateTime:  cmd.CreateTime,
			TargetScope: cmd.TargetScope,
		})
	}

	return commands, nil
}

// ListCommandsWithInfo returns all active commands and configs with TTL information
func (s *CommandStore) ListCommandsWithInfo(ctx context.Context) ([]*CommandInfoData, error) {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	now := time.Now().UnixMilli()
	var result []*CommandInfoData

	// Add one-time commands (non-persistent)
	for _, cmd := range s.cache.commands {
		// Skip expired commands
		if cmd.TTLSeconds > 0 && now > cmd.CreateTime+cmd.TTLSeconds*1000 {
			continue
		}
		result = append(result, &CommandInfoData{
			CommandID:   cmd.CommandID,
			CommandType: cmd.CommandType,
			TargetScope: cmd.TargetScope,
			Persistent:  false,
			CreateTime:  cmd.CreateTime,
			TTLSeconds:  cmd.TTLSeconds,
		})
	}

	// Add persistent configs
	for _, cfg := range s.cache.configs {
		result = append(result, &CommandInfoData{
			CommandID:   cfg.ConfigID,
			CommandType: cfg.ConfigType,
			TargetScope: cfg.TargetScope,
			Persistent:  true,
			CreateTime:  cfg.CreateTime,
			TTLSeconds:  0, // Persistent configs don't expire
		})
	}

	return result, nil
}

// DeleteCommand removes a command from memory or a config from etcd/cache
func (s *CommandStore) DeleteCommand(ctx context.Context, commandID string) error {
	s.cacheMu.RLock()
	_, commandExists := s.cache.commands[commandID]
	_, configExists := s.cache.configs[commandID]
	s.cacheMu.RUnlock()

	if configExists {
		if err := s.kv.Delete(ctx, s.configPath+commandID); err != nil {
			return merr.WrapErrIoFailed(commandID, merr.WrapErrServiceInternalMsg("delete failed: %v", err))
		}
	}

	if commandExists || configExists {
		s.cacheMu.Lock()
		if commandExists {
			delete(s.cache.commands, commandID)
		}
		if configExists {
			delete(s.cache.configs, commandID)
			s.cache.configHash = s.computeConfigHash()
		}
		s.cacheMu.Unlock()
	}

	return nil
}

// CleanupExpiredCommands removes expired commands from memory cache
func (s *CommandStore) CleanupExpiredCommands(ctx context.Context) {
	now := time.Now().UnixMilli()

	// maxReapedSamples bounds the detail in the log line below. A command reaped here is
	// one no client ever collected, so whoever pushed it is still waiting on a reply that
	// can never arrive, and the ID and scope are what let them correlate. But nothing
	// bounds how many commands can expire at once, and formatting every one of them --
	// inside the read lock, into a single log record -- would turn a large sweep into a
	// giant allocation, a giant log line, and a long lock hold. A few examples plus the
	// total is enough to recognize what happened.
	const maxReapedSamples = 10

	// Find expired commands
	s.cacheMu.RLock()
	var expired []string
	var reaped []string
	for _, cmd := range s.cache.commands {
		if cmd.TTLSeconds > 0 && now > cmd.CreateTime+cmd.TTLSeconds*1000 {
			expired = append(expired, cmd.CommandID)
			if len(reaped) < maxReapedSamples {
				reaped = append(reaped, fmt.Sprintf("%s(%s,scope=%s,ttl=%ds)",
					cmd.CommandID, cmd.CommandType, cmd.TargetScope, cmd.TTLSeconds))
			}
		}
	}
	s.cacheMu.RUnlock()

	// Delete them
	for _, id := range expired {
		s.DeleteCommand(ctx, id)
	}

	if len(expired) > 0 {
		mlog.Info(ctx, "CleanupExpiredCommands: reaped commands no client collected before their TTL",
			mlog.Int("deleted", len(expired)),
			mlog.Int("sampled", len(reaped)),
			mlog.Strings("sample", reaped))
	}
}

// DeleteNonPersistentCommand removes a one-time command by ID.
// Returns true if a command was removed, false otherwise.
func (s *CommandStore) DeleteNonPersistentCommand(commandID string) bool {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	if _, ok := s.cache.commands[commandID]; ok {
		delete(s.cache.commands, commandID)
		return true
	}
	return false
}

// DeleteCommandOnReply removes a one-time command because a client answered it -- but only
// when the command was aimed at that single client. Returns true if it was removed.
//
// A client-scoped command has exactly one recipient, so the reply that just arrived is the
// whole answer and the command is finished.
//
// A global or database-scoped command is delivered to every matching client, each answering
// on its own heartbeat. Deleting on the first reply hands whichever client heartbeats
// soonest the power to cancel delivery to everyone else: with clients on a 30s and a 5min
// interval, the fast one answers and the slow one never sees the command at all. That made
// a broadcast collection_metrics -- a state change meant for the whole fleet -- silently
// apply to part of it, with no error and no way to tell from the outside. Those commands
// are left to expire on their TTL instead.
//
// Retention does not cause re-execution: clients skip commands older than their
// last_command_timestamp watermark and track executed IDs for same-millisecond ties. It
// does mean a client that connects during the TTL window also executes the command, which
// is what you want for a fleet-wide state change and merely noisy for a one-off query.
func (s *CommandStore) DeleteCommandOnReply(commandID string) bool {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	cmd, ok := s.cache.commands[commandID]
	if !ok {
		return false
	}
	if !strings.HasPrefix(cmd.TargetScope, clientScopePrefix) {
		return false
	}
	delete(s.cache.commands, commandID)
	return true
}

// GetCommandInfo returns command metadata from cache.
func (s *CommandStore) GetCommandInfo(commandID string) (string, []byte, bool, bool) {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	if cmd, ok := s.cache.commands[commandID]; ok {
		return cmd.CommandType, cmd.Payload, false, true
	}
	if cfg, ok := s.cache.configs[commandID]; ok {
		return cfg.ConfigType, cfg.Payload, true, true
	}
	return "", nil, false, false
}

// ListConfigs returns all configs from cache with hash for change detection
func (s *CommandStore) ListConfigs(ctx context.Context) ([]*ClientConfig, string, error) {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	configs := make([]*ClientConfig, 0, len(s.cache.configs))
	for _, cfg := range s.cache.configs {
		configs = append(configs, &ClientConfig{
			ConfigId:    cfg.ConfigID,
			ConfigType:  cfg.ConfigType,
			Payload:     cfg.Payload,
			CreateTime:  cfg.CreateTime,
			TargetScope: cfg.TargetScope,
		})
	}
	return configs, s.cache.configHash, nil
}

// computeConfigHash computes hash of all configs in cache for change detection
// Must be called while holding cacheMu lock
func (s *CommandStore) computeConfigHash() string {
	return computeConfigHashFromConfigs(s.cache.configs)
}

func computeConfigHashFromConfigs(configs map[string]*storedConfig) string {
	if len(configs) == 0 {
		return ""
	}

	// Sort by config ID for consistent hash
	ids := make([]string, 0, len(configs))
	for id := range configs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	h := sha256.New()
	for _, id := range ids {
		cfg := configs[id]
		h.Write([]byte(cfg.ConfigID))
		h.Write([]byte(cfg.ConfigType))
		h.Write(cfg.Payload)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}
