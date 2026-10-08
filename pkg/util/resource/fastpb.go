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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// fastPBEnabled preserves the default fast decoder before configuration is ready.
// A codec used during startup must not initialize configuration itself.
func fastPBEnabled() bool {
	params := paramtable.GetIfInitialized()
	return params == nil || params.CommonCfg.EnableFastPB.GetAsBool()
}

// UnmarshalSearchResultData decodes search results with the currently configured
// protobuf decoder. The underlying fastpb decoders remain independent of config.
func UnmarshalSearchResultData(data []byte, result *schemapb.SearchResultData) error {
	if fastPBEnabled() {
		return fastpb.UnmarshalSearchResultData(data, result)
	}
	return proto.Unmarshal(data, result)
}
