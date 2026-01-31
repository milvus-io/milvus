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
	"sync"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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

	// Load configs
	if resp, err := s.kv.Get(ctx, s.configPath, clientv3.WithPrefix()); err != nil {
		log.Ctx(ctx).Warn("loadCache: failed to load configs", zap.Error(err))
	} else {
		for _, kv := range resp.Kvs {
			var cfg storedConfig
			if err := json.Unmarshal(kv.Value, &cfg); err != nil {
				log.Ctx(ctx).Warn("loadCache: failed to unmarshal config",
					zap.Error(err),
					zap.String("key", string(kv.Key)))
				continue
			}
			s.cache.configs[cfg.ConfigID] = &cfg
		}
	}

	// Calculate config hash
	s.cache.configHash = s.computeConfigHash()

	log.Ctx(ctx).Info("loadCache: completed",
		zap.Int("commands", len(s.cache.commands)),
		zap.Int("configs", len(s.cache.configs)))
}

// PushCommand stores a command/config in etcd and cache
// Persistent=true: stored as config (no TTL), Persistent=false: one-time command (with TTL)
func (s *CommandStore) PushCommand(ctx context.Context, req *milvuspb.PushClientCommandRequest) (string, error) {
	// Validate persistent command types
	if req.Persistent && req.CommandType != "push_config" {
		return "", merr.WrapErrParameterInvalid("push_config", req.CommandType,
			"only push_config can be persistent")
	}

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
				log.Ctx(ctx).Warn("PushCommand: failed to delete old config",
					zap.String("config_id", id),
					zap.Error(err))
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
			TTLSeconds:  req.TtlSeconds,
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
			return merr.WrapErrIoFailed(commandID, fmt.Errorf("delete failed: %v", err))
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

	// Find expired commands
	s.cacheMu.RLock()
	var expired []string
	for _, cmd := range s.cache.commands {
		if cmd.TTLSeconds > 0 && now > cmd.CreateTime+cmd.TTLSeconds*1000 {
			expired = append(expired, cmd.CommandID)
		}
	}
	s.cacheMu.RUnlock()

	// Delete them
	for _, id := range expired {
		s.DeleteCommand(ctx, id)
	}

	if len(expired) > 0 {
		log.Ctx(ctx).Info("CleanupExpiredCommands", zap.Int("deleted", len(expired)))
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
