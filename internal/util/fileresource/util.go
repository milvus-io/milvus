/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package fileresource

import (
	"strings"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type ResolvedFileResource struct {
	ID        int64
	Name      string
	Path      string
	LocalPath string
}

type SyncEvent struct {
	Version   uint64
	Resources []*ResolvedFileResource
}

type Listener interface {
	OnFileResourceSync(event SyncEvent) error
}

const (
	SyncModeStr  string = "sync"
	RefModeStr   string = "ref"
	CloseModeStr string = "close"
)

const localModeUnset int32 = 0

var localMode atomic.Int32

type LocalRoles struct {
	QueryNode     bool
	DataNode      bool
	Proxy         bool
	StreamingNode bool
}

func (m Mode) String() string {
	switch m {
	case SyncMode:
		return SyncModeStr
	case RefMode:
		return RefModeStr
	case CloseMode:
		return CloseModeStr
	default:
		return "unknown"
	}
}

func ParseMode(value string) Mode {
	switch value {
	case CloseModeStr:
		return CloseMode
	case SyncModeStr:
		return SyncMode
	case RefModeStr:
		return RefMode
	default:
		return CloseMode
	}
}

func IsSyncMode(value string) bool {
	return value == SyncModeStr
}

func IsRefMode(value string) bool {
	return value == RefModeStr
}

func isExplicitClose(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), CloseModeStr)
}

func GetStandaloneMode() Mode {
	params := paramtable.Get()
	if isExplicitClose(params.CommonCfg.QNFileResourceMode.GetValue()) &&
		isExplicitClose(params.CommonCfg.DNFileResourceMode.GetValue()) &&
		isExplicitClose(params.CommonCfg.PNFileResourceMode.GetValue()) {
		return CloseMode
	}
	return SyncMode
}

func GetQueryNodeMode() Mode {
	if paramtable.IsStandalone() {
		return GetStandaloneMode()
	}
	return ParseMode(paramtable.Get().CommonCfg.QNFileResourceMode.GetValue())
}

func GetDataNodeMode() Mode {
	if paramtable.IsStandalone() {
		return GetStandaloneMode()
	}
	return ParseMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue())
}

func GetProxyMode() Mode {
	if paramtable.IsStandalone() {
		return GetStandaloneMode()
	}
	if ParseMode(paramtable.Get().CommonCfg.PNFileResourceMode.GetValue()) == SyncMode {
		return SyncMode
	}
	return CloseMode
}

func ResolveLocalMode(roles LocalRoles) Mode {
	if paramtable.IsStandalone() {
		return GetStandaloneMode()
	}

	modes := make([]Mode, 0, 4)
	if roles.QueryNode || roles.StreamingNode {
		modes = append(modes, GetQueryNodeMode())
	}
	if roles.DataNode {
		modes = append(modes, GetDataNodeMode())
	}
	if roles.Proxy {
		modes = append(modes, GetProxyMode())
	}

	resolved := CloseMode
	for _, mode := range modes {
		if mode == SyncMode {
			return SyncMode
		}
		if mode == RefMode {
			resolved = RefMode
		}
	}
	return resolved
}

func SetLocalMode(mode Mode) {
	localMode.Store(int32(mode))
}

func GetLocalMode(fallback Mode) Mode {
	mode := localMode.Load()
	if mode == localModeUnset {
		return fallback
	}
	return Mode(mode)
}

func ResetLocalModeForTest() {
	localMode.Store(localModeUnset)
}
