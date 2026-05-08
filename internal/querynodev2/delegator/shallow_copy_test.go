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

package delegator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/shallowcopy"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestShallowCopySearchRequest_EntityTtlPhysicalTime(t *testing.T) {
	req := &internalpb.SearchRequest{
		Base:                    &commonpb.MsgBase{TargetID: 1},
		CollectionID:            1000,
		MvccTimestamp:           100000,
		GuaranteeTimestamp:      99999,
		CollectionTtlTimestamps: 50000,
		EntityTtlPhysicalTime:   1773682085500000,
	}

	copied := shallowcopy.ShallowCopySearchRequest(req, 2)

	assert.Equal(t, req.EntityTtlPhysicalTime, copied.EntityTtlPhysicalTime,
		"EntityTtlPhysicalTime must be preserved in shallow copy")
	assert.Equal(t, int64(2), copied.Base.TargetID)
	assert.Equal(t, req.CollectionID, copied.CollectionID)
	assert.Equal(t, req.MvccTimestamp, copied.MvccTimestamp)
	assert.Equal(t, req.CollectionTtlTimestamps, copied.CollectionTtlTimestamps)
}
