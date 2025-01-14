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

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/proto/proxypb"
)

func Test_expireCacheConfig_apply(t *testing.T) {
	c := proxyutil.DefaultExpireCacheConfig()
	req := &proxypb.InvalidateCollMetaCacheRequest{}
	c.Apply(req)
	assert.Equal(t, commonpb.MsgType_Undefined, req.GetBase().GetMsgType())
	opt := proxyutil.SetMsgType(commonpb.MsgType_DropCollection)
	opt(&c)
	c.Apply(req)
	assert.Equal(t, commonpb.MsgType_DropCollection, req.GetBase().GetMsgType())
}
