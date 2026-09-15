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

package cmek

import (
	"context"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// prepareRawDataCampaign returns the complete nonempty output of this flush.
// Compaction is disabled before the cluster starts, so the same segments are
// inspected and then read after release/reload.
func (s *rawDataSuite) prepareRawDataCampaign(ctx context.Context, c rawDataCampaign) (*milvuspb.DescribeCollectionResponse, []*datapb.SegmentInfo) {
	collection := "cmek_raw_" + c.name + "_" + funcutil.GenRandomStr()
	c.schema.Name = collection
	encoded, err := proto.Marshal(c.schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: s.dbName, CollectionName: collection, Schema: encoded, ShardsNum: common.DefaultShardsNum,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.T().Cleanup(func() { s.cleanupRawCollection(collection) })
	description, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(description, err))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(description.GetProperties(), common.EncryptionEzIDKey))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(description.GetSchema().GetProperties(), common.EncryptionEzIDKey))
	if description.GetSchema().GetEnableDynamicField() {
		// The public schema omits $meta. Keep its user-facing struct names,
		// and obtain the dynamic field ID from the coordinator's full schema.
		internal, err := s.Cluster.MixCoordClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base:   &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			DbName: s.dbName, CollectionName: collection,
		})
		s.Require().NoError(merr.CheckRPCCall(internal, err))
		s.Require().Equal(description.GetCollectionID(), internal.GetCollectionID())
		for _, field := range internal.GetSchema().GetFields() {
			if field.GetIsDynamic() {
				description.Schema.Fields = append(description.Schema.Fields, field)
			}
		}
	}
	s.bindRawDataFieldIDs(description.GetSchema(), c.fields)
	if c.index {
		// Finish logical-index broadcasts before insert/flush starts the segment lifecycle.
		s.createRawVectorIndexes(ctx, collection, description.GetSchema())
	}
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName: s.dbName, CollectionName: collection, FieldsData: c.fields,
		HashKeys: integration.GenerateHashKeys(rawDataRows), NumRows: rawDataRows,
	})
	s.Require().NoError(merr.CheckRPCCall(insert, err))
	s.Require().Equal(int64(rawDataRows), insert.GetInsertCnt())
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{DbName: s.dbName, CollectionNames: []string{collection}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	ids := flush.GetCollSegIDs()[collection].GetData()
	s.Require().NotEmpty(ids)
	s.WaitForFlush(ctx, ids, flush.GetCollFlushTs()[collection], s.dbName, collection)
	segments := s.rawFlushedSegments(collection, ids)
	var rows int64
	for _, segment := range segments {
		s.Require().Equal(description.GetCollectionID(), segment.GetCollectionID())
		s.Require().False(segment.GetCompacted())
		s.Require().False(segment.GetIsInvisible())
		rows += segment.GetNumOfRows()
	}
	s.Require().Equal(int64(rawDataRows), rows)
	s.T().Logf("stage=flush campaign=%s collection=%d segments=%v rows=%d", c.name, description.GetCollectionID(), ids, rows)
	return description, segments
}

func (s *rawDataSuite) bindRawDataFieldIDs(schema *schemapb.CollectionSchema, data []*schemapb.FieldData) {
	ids := make(map[string]int64)
	for _, field := range schema.GetFields() {
		ids[field.GetName()] = field.GetFieldID()
	}
	for _, field := range schema.GetStructArrayFields() {
		ids[field.GetName()] = field.GetFieldID()
		for _, child := range field.GetFields() {
			ids[typeutil.ConcatStructFieldName(field.GetName(), child.GetName())] = child.GetFieldID()
		}
	}
	for _, field := range data {
		field.FieldId = ids[field.GetFieldName()]
		s.Require().Positive(field.FieldId, "field %s is missing from accepted schema", field.GetFieldName())
		for _, child := range field.GetStructArrays().GetFields() {
			name := typeutil.ConcatStructFieldName(field.GetFieldName(), child.GetFieldName())
			child.FieldId = ids[name]
			s.Require().Positive(child.FieldId, "field %s is missing from accepted schema", name)
		}
	}
}

func requestedFieldIDs(schema *schemapb.CollectionSchema, names []string) []int64 {
	fields := make(map[string][]int64)
	for _, field := range schema.GetFields() {
		fields[field.GetName()] = []int64{field.GetFieldID()}
	}
	for _, field := range schema.GetStructArrayFields() {
		for _, child := range field.GetFields() {
			fields[field.GetName()] = append(fields[field.GetName()], child.GetFieldID())
		}
	}
	var ids []int64
	for _, name := range names {
		ids = append(ids, fields[name]...)
	}
	return ids
}
