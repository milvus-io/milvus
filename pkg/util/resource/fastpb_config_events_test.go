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

package resource

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func invalidFastPBConfigEventString() []byte {
	wire := protowire.AppendTag(nil, 7, protowire.BytesType)
	return protowire.AppendBytes(wire, []byte{0xff})
}

func initFastPBConfigEventTestParams(t *testing.T) *paramtable.ComponentParam {
	t.Helper()
	base := paramtable.NewBaseTable(paramtable.Files([]string{}), paramtable.SkipRemote(true), paramtable.SkipEnv(true))
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	paramtable.InitWithBaseTable(base)
	params := paramtable.GetIfInitialized()
	require.NotNil(t, params)
	old := params.CommonCfg.EnableFastPB.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save("common.enableFastPB", old)) })
	return params
}

// fastPBConfigSource publishes separator-free keys like the etcd source.
type fastPBConfigSource struct {
	name   string
	mu     sync.RWMutex
	values map[string]string
}

func (s *fastPBConfigSource) GetConfigurations() (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make(map[string]string, len(s.values))
	for key, value := range s.values {
		values[key] = value
	}
	return values, nil
}

func (s *fastPBConfigSource) GetConfigurationByKey(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[key]
	if !ok {
		return "", config.ErrKeyNotFound
	}
	return value, nil
}

func (s *fastPBConfigSource) GetPriority() int                    { return config.HighPriority }
func (s *fastPBConfigSource) GetSourceName() string               { return s.name }
func (s *fastPBConfigSource) SetEventHandler(config.EventHandler) {}
func (s *fastPBConfigSource) SetManager(config.ConfigManager)     {}
func (s *fastPBConfigSource) UpdateOptions(config.Options)        {}
func (s *fastPBConfigSource) Close()                              {}

func TestFastPBConfigSourceEvents(t *testing.T) {
	params := initFastPBConfigEventTestParams(t)
	key := params.CommonCfg.EnableFastPB.Key
	etcdKey := config.EtcdConfigKey(key)
	manager := paramtable.GetBaseTable().Manager()
	source := &fastPBConfigSource{name: t.TempDir(), values: make(map[string]string)}
	require.NoError(t, manager.AddSource(source))
	send := func(eventType, value string) {
		source.mu.Lock()
		if eventType == config.DeleteType {
			delete(source.values, etcdKey)
		} else {
			source.values[etcdKey] = value
		}
		source.mu.Unlock()
		// Do not evict the typed cache here: the fastPB handler must also work
		// when it runs before the dispatcher's cache-eviction prefix handler.
		manager.OnEvent(&config.Event{
			EventSource: source.name,
			EventType:   eventType,
			Key:         etcdKey,
			Value:       value,
		})
	}
	t.Cleanup(func() {
		send(config.DeleteType, "")
		require.NoError(t, params.Reset(key))
	})
	check := func(enabled bool) {
		t.Helper()
		// Reading this before the next event also warms the typed cache.
		require.Equal(t, enabled, params.CommonCfg.EnableFastPB.GetAsBool())
		require.Equal(t, enabled, paramtable.FastPBEnabled())
		retrieveErr := (releaseCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(invalidFastPBConfigEventString())}, &internalpb.RetrieveResults{})
		searchErr := UnmarshalSearchResultData(invalidFastPBConfigEventString(), &schemapb.SearchResultData{})
		if enabled {
			require.NoError(t, retrieveErr)
			require.NoError(t, searchErr)
		} else {
			require.Error(t, retrieveErr)
			require.Error(t, searchErr)
		}
	}

	require.NoError(t, params.Reset(key))
	check(true)
	send(config.CreateType, "false")
	check(false)
	send(config.UpdateType, "true")
	check(true)

	// An underlying source event must not override the effective runtime value.
	require.NoError(t, params.Save(key, "false"))
	check(false)
	send(config.UpdateType, "false")
	check(false)
	send(config.UpdateType, "true")
	check(false)
	require.NoError(t, params.Reset(key))
	check(true)

	send(config.UpdateType, "false")
	check(false)
	send(config.DeleteType, "false")
	check(true)

	// Reset without an underlying value emits DELETE and restores the default.
	require.NoError(t, params.Save(key, "false"))
	check(false)
	require.NoError(t, params.Reset(key))
	check(true)

	// Runtime events also accept the same normalized aliases as source events.
	for _, alias := range []string{etcdKey, "COMMON_ENABLEFASTPB"} {
		require.NoError(t, params.Save(alias, "false"))
		check(false)
		require.NoError(t, params.Reset(alias))
		check(true)
	}
}
