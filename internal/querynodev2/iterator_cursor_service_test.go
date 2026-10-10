// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package querynodev2

import (
	"context"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/iteratorutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Execute the real RPC entry point: replacing status after searchChannel would
// silently erase worker capability even when the complete reduction ACKed it.
func (suite *ServiceSuite) TestSearch_PreservesIteratorCursorStatus() {
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	suite.node.manager.Collection.PutOrRef(suite.collectionID, schema, nil, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection, CollectionID: suite.collectionID, PartitionIDs: suite.partitionIDs,
	})
	marked := merr.Success()
	marked.ExtraInfo = map[string]string{"report_value": "7"}
	iteratorutil.MarkPKCursor(marked)
	for _, originalStatus := range []*commonpb.Status{nil, marked} {
		func() {
			channelResult := &internalpb.SearchResults{Status: originalStatus}
			patch := mockey.Mock((*QueryNode).searchChannel).Return(channelResult, nil).Build()
			defer patch.UnPatch()
			result, err := suite.node.Search(context.Background(), &querypb.SearchRequest{
				Req: &internalpb.SearchRequest{CollectionID: suite.collectionID}, DmlChannels: []string{suite.vchannel},
			})
			suite.NoError(err)
			suite.Same(channelResult, result)
			suite.True(merr.Ok(result.GetStatus()))
			if originalStatus == nil {
				suite.NotContains(result.Status.ExtraInfo, iteratorutil.CursorVersionKey)
			} else {
				suite.Same(originalStatus, result.Status)
				suite.Equal("2", result.Status.ExtraInfo[iteratorutil.CursorVersionKey])
				suite.Equal("7", result.Status.ExtraInfo["report_value"])
			}
		}()
	}
}
