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

package checkers

import (
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// expectRecoveryInfo mocks GetRecoveryInfoV2 and registers the returned
// segments in the shared MetaStore. TargetManager takes only segment IDs
// from the broker and resolves segment details from the MetaStore, so a
// segment missing there is dropped from the target.
func expectRecoveryInfo(
	broker *meta.MockBroker,
	metaStore metacache.MetaStore,
	collectionID interface{},
	channels []*datapb.VchannelInfo,
	segments []*datapb.SegmentInfo,
	err error,
) *meta.MockBroker_GetRecoveryInfoV2_Call {
	for _, segment := range segments {
		metaStore.PutSegment(segment)
	}
	return broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(channels, segments, err)
}
