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

package importv2

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

// ImportV3BinlogSuite exercises the backup/binlog-origin path under the V3 gate:
// a size-only preimport (no hashing, no ranges), then Reshard + a single final
// merge. The source PK/RowID/timestamps carved from the source collection must
// survive end to end.
type ImportV3BinlogSuite struct {
	importTestBase
}

func (s *ImportV3BinlogSuite) SetupSuite() {
	// The source collection lives in this same cluster and is read back through
	// the legacy per-field binlog reader, so the cluster stays on Storage V2.
	configureImportV3Gate(&s.MiniClusterSuite)
	s.MiniClusterSuite.SetupSuite()
}

func (s *ImportV3BinlogSuite) TestBinlogImportV3() {
	const dim = 128

	source := s.PrepareSourceCollection(dim, &DMLGroup{insertRowNums: []int{500, 500, 500}})
	collectionID := source.collectionID
	partitionID := source.partitionID
	segmentIDs := source.SegmentIDs
	insertedIDs := source.insertedIDs

	c := s.Cluster
	ctx := c.GetContext()
	totalInsertRowNum := 1500

	collectionName := "ImportV3Binlog_B_" + funcutil.RandomString(8)
	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

	describeCollectionResp, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(describeCollectionResp, err))
	newCollectionID := describeCollectionResp.GetCollectionID()

	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	files := make([]*internalpb.ImportFile, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		files = append(files, &internalpb.ImportFile{Paths: []string{fmt.Sprintf("%s/insert_log/%d/%d/%d",
			s.Cluster.RootPath(), collectionID, partitionID, segmentID)}})
	}

	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:          files,
		Options: []*commonpb.KeyValuePair{
			{Key: "backup", Value: "true"},
			{Key: importutilv2.StorageVersion, Value: strconv.Itoa(source.storageVersion)},
		},
	})
	s.NoError(merr.CheckRPCCall(importResp, err))
	s.NoError(WaitForImportDone(ctx, c, importResp.GetJobID()))

	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	segments = lo.Filter(segments, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetCollectionID() == newCollectionID
	})
	mlog.Info(ctx, "v3 binlog import segments", mlog.Any("segments", segments))
	s.NotEmpty(segments)

	flushed, ok := lo.Find(segments, func(segment *datapb.SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Flushed
	})
	s.True(ok)
	// A backup import materializes the source timestamps, so the produced
	// segment's position must match the source collection's timestamp bounds.
	s.Equal(source.l1MinTs, flushed.GetStartPosition().GetTimestamp(),
		"V3 backup segment StartPosition must preserve the source min timestamp")
	s.Equal(source.l1MaxTs, flushed.GetDmlPosition().GetTimestamp(),
		"V3 backup segment DmlPosition must preserve the source max timestamp")
	s.Greater(flushed.GetCommitTimestamp(), uint64(0))

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collectionName})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)

	// Every restored primary key comes from the source collection: backup keeps
	// the source PK/RowID instead of re-assigning AutoID.
	insertedIDsMap := lo.SliceToMap(insertedIDs.GetIntId().GetData(), func(id int64) (int64, struct{}) {
		return id, struct{}{}
	})
	searchReq := integration.ConstructSearchRequest("", collectionName, fmt.Sprintf("%s > 0", integration.Int64Field),
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2,
		integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2), 10, dim, 10, -1)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Eventually
	searchResult, err := c.MilvusClient.Search(ctx, searchReq)
	s.NoError(merr.CheckRPCCall(searchResult, err))
	for _, id := range searchResult.GetResults().GetIds().GetIntId().GetData() {
		_, ok := insertedIDsMap[id]
		s.True(ok, "restored id %d is not a source primary key", id)
	}

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             fmt.Sprintf("%s >= 0", integration.Int64Field),
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
	})
	s.NoError(merr.CheckRPCCall(queryResult, err))
	s.Equal(int64(totalInsertRowNum), queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
}

func TestImportV3BinlogSuite(t *testing.T) {
	suite.Run(t, new(ImportV3BinlogSuite))
}
