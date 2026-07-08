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

package rootcoord

import "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

// DatabaseCipherHelper abstracts database encryption property handling so that
// MetaTable does not depend on internal/util/hookutil directly. This keeps
// MetaTable free of internal-only imports so it can be moved into the shared
// pkg module; the production adapter lives in meta_deps_adapter.go.
type DatabaseCipherHelper interface {
	TidyDBCipherProperties(ezID int64, dbProperties []*commonpb.KeyValuePair) ([]*commonpb.KeyValuePair, error)
	CreateEZByDBProperties(dbProperties []*commonpb.KeyValuePair) error
}

// PChannelStatsManager abstracts pchannel stats updates for the streaming
// balancer so that MetaTable does not depend on internal/streamingcoord
// directly. The production adapter lives in meta_deps_adapter.go.
type PChannelStatsManager interface {
	Recover(vchannels []string)
	AddVChannel(vchannels ...string)
	RemoveVChannel(vchannels ...string)
}

// noopCipherHelper is the default used when no DatabaseCipherHelper is injected
// (e.g. MetaTable built directly via a struct literal in tests). Production code
// always goes through NewMetaTable, which injects the real helper.
type noopCipherHelper struct{}

func (noopCipherHelper) TidyDBCipherProperties(_ int64, dbProperties []*commonpb.KeyValuePair) ([]*commonpb.KeyValuePair, error) {
	return dbProperties, nil
}

func (noopCipherHelper) CreateEZByDBProperties([]*commonpb.KeyValuePair) error { return nil }

// noopPChannelStatsManager is the default used when no PChannelStatsManager is
// injected. Production code injects the real adapter via NewMetaTable.
type noopPChannelStatsManager struct{}

func (noopPChannelStatsManager) Recover([]string)         {}
func (noopPChannelStatsManager) AddVChannel(...string)    {}
func (noopPChannelStatsManager) RemoveVChannel(...string) {}

// cipher returns the injected DatabaseCipherHelper, or a no-op when unset.
func (mt *MetaTable) cipher() DatabaseCipherHelper {
	if mt.cipherHelper == nil {
		return noopCipherHelper{}
	}
	return mt.cipherHelper
}

// chStats returns the injected PChannelStatsManager, or a no-op when unset.
func (mt *MetaTable) chStats() PChannelStatsManager {
	if mt.channelStats == nil {
		return noopPChannelStatsManager{}
	}
	return mt.channelStats
}

// TSOAllocator abstracts timestamp allocation so MetaTable does not depend on
// internal/tso. The production *tso.GlobalTSOAllocator satisfies it structurally
// and is injected via NewMetaTable.
type TSOAllocator interface {
	GenerateTSO(count uint32) (uint64, error)
}
