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
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func invalidResultString() []byte {
	wire := protowire.AppendTag(nil, 7, protowire.BytesType)
	return protowire.AppendBytes(wire, []byte{0xff})
}

func TestFastPBBeforeInitialization(t *testing.T) {
	if os.Getenv("MILVUS_TEST_FASTPB_PREINIT") == "1" {
		require.Nil(t, paramtable.GetIfInitialized())
		require.True(t, fastPBEnabled())
		require.NoError(t, (releaseCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(invalidResultString())}, &internalpb.RetrieveResults{}))
		require.NoError(t, UnmarshalSearchResultData(invalidResultString(), &schemapb.SearchResultData{}))
		require.Nil(t, paramtable.GetIfInitialized(), "codec initialized configuration")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFastPBBeforeInitialization$") // #nosec G204 G702 -- Re-exec this test binary with a fixed filter.
	cmd.Env = append(os.Environ(), "MILVUS_TEST_FASTPB_PREINIT=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func initFastPBTestParams(t *testing.T) *paramtable.ComponentParam {
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

func TestFastPBRuntimeResultDecoderSelection(t *testing.T) {
	params := initFastPBTestParams(t)
	require.True(t, params.CommonCfg.EnableFastPB.GetAsBool())
	for _, value := range []string{"true", "false", "true"} {
		require.NoError(t, params.Save("common.enableFastPB", value))
		enabled := value == "true"
		retrieve := &internalpb.RetrieveResults{}
		search := &schemapb.SearchResultData{}
		retrieveErr := (releaseCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(invalidResultString())}, retrieve)
		searchErr := UnmarshalSearchResultData(invalidResultString(), search)
		if enabled {
			require.NoError(t, retrieveErr)
			require.NoError(t, searchErr)
			require.Equal(t, []string{string([]byte{0xff})}, retrieve.ChannelIDsRetrieved)
			require.Equal(t, []string{string([]byte{0xff})}, search.OutputFields)
		} else {
			require.Error(t, retrieveErr)
			require.Error(t, searchErr)
		}
	}
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
	params := initFastPBTestParams(t)
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
		retrieveErr := (releaseCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(invalidResultString())}, &internalpb.RetrieveResults{})
		searchErr := UnmarshalSearchResultData(invalidResultString(), &schemapb.SearchResultData{})
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

func TestFastPBRuntimeAllRPCDispatch(t *testing.T) {
	params := initFastPBTestParams(t)
	selected := errors.New("fast decoder selected")
	calls := 0
	patch := mockey.Mock(fastpb.TryUnmarshal).To(func(v any, data []byte) (bool, error) {
		calls++
		switch v.(type) {
		case *internalpb.RetrieveResults, *milvuspb.InsertRequest, *milvuspb.UpsertRequest:
			return true, selected
		default:
			return false, nil
		}
	}).Build()
	defer patch.UnPatch()
	for _, value := range []string{"true", "false", "true"} {
		require.NoError(t, params.Save("common.enableFastPB", value))
		for _, message := range []proto.Message{&internalpb.RetrieveResults{}, &milvuspb.InsertRequest{}, &milvuspb.UpsertRequest{}} {
			before := calls
			err := (releaseCodec{}).Unmarshal(nil, message)
			if value == "true" {
				require.ErrorIs(t, err, selected)
				require.Equal(t, before+1, calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, before, calls)
			}
		}
		require.NoError(t, (releaseCodec{}).Unmarshal(nil, &commonpb.Status{}))
	}
}
