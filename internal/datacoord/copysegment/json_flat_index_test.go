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

package copysegment

import (
	"context"

	"github.com/bytedance/mockey"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func (s *CopySegmentTaskSuite) newCopiedJSONFlatManifestFixture() (CopySegmentTask, CopySegmentMeta, *meta.Meta, *index.IndexMeta, *datapb.QueryCopySegmentResponse, []packed.ManifestIndexInfo) {
	task, copies, m, im, resp := s.newCopiedManifestReadBackFixture()
	im.Indexes[100][301] = &model.Index{
		CollectionID: 100, FieldID: 102, IndexID: 301, IndexName: "json_flat",
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "INVERTED"},
			{Key: common.JSONCastTypeKey, Value: "JSON"},
			{Key: common.JSONPathKey, Value: ""},
		},
	}
	im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, DataType: schemapb.DataType_JSON},
		}}
	})
	result := resp.GetSegmentResults()[0]
	result.IndexInfos[9002] = &datapb.VectorScalarIndexInfo{
		FieldId: 102, BuildId: 9002, IndexName: "json_flat", IndexFilePaths: []string{"json-flat.bin"},
	}
	entries := []packed.ManifestIndexInfo{copiedManifestReviewEntry(), {
		IndexID: 301, BuildID: 9002, FieldID: 102, IndexName: "json_flat", IndexType: "INVERTED",
		Properties: map[string]string{common.IndexTypeKey: "INVERTED", common.JSONCastTypeKey: "JSON", common.JSONPathKey: ""},
		Path:       "files/index_files/9002/0/10/2001", IndexFileKeys: []string{"json-flat.bin"},
	}}
	return task, copies, m, im, resp, entries
}

func (s *CopySegmentTaskSuite) TestCopyManifestIgnoresHistoricalJSONFlatArtifacts() {
	meta.DrainBuildIndexChForTest()
	defer meta.DrainBuildIndexChForTest()
	meta.DrainStatsBuildChForTest()
	defer meta.DrainStatsBuildChForTest()
	meta.MockManifestIndexSubmissions(s.T())
	for _, scenario := range []struct {
		name         string
		acknowledged bool
		admitted     bool
	}{
		{name: "legacy result"},
		{name: "acknowledged result", acknowledged: true},
		{name: "restart after partial publication", acknowledged: true, admitted: true},
	} {
		s.Run(scenario.name, func() {
			ctx := context.Background()
			task, copies, m, im, resp, entries := s.newCopiedJSONFlatManifestFixture()
			result := resp.GetSegmentResults()[0]
			if scenario.acknowledged {
				result.ManifestIndexRewritten = proto.Bool(true)
				result.ManifestIndexBuildIds = []int64{9001, 9002}
			}
			if scenario.admitted {
				s.Require().NoError(copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskCleanup(true)))
				s.Require().NoError(m.UpdateSegmentsInfo(ctx,
					meta.UpdateStatusOperator(2001, commonpb.SegmentState_Flushed),
					meta.UpdateManifest(2001, result.GetManifestPath()),
				))
				var err error
				copies, err = NewCopySegmentMeta(ctx, m.Catalog, m, im, nil, nil)
				s.Require().NoError(err)
				task = copies.GetTask(ctx, task.GetTaskId())
			}
			defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return(entries, nil).Build().UnPatch()

			s.Require().NoError(SyncCopySegmentTask(task, resp, copies, m, im))
			s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
			s.False(task.GetCleanupRequired())
			s.Equal(commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState())
			s.Len(im.GetIndexesForCollection(100, "json_flat"), 1)
			_, flatInstalled := im.GetIndexJob(9002)
			s.False(flatInstalled, "historical flat files must not become a physical SegmentIndex")
			_, vectorInstalled := im.GetIndexJob(9001)
			s.True(vectorInstalled, "ordinary indexes must still be installed")
		})
	}
}

func (s *CopySegmentTaskSuite) TestCopyManifestJSONFlatKeepsOwnershipChecks() {
	for _, scenario := range []string{"foreign definition", "changed parameters", "wrong files", "unacknowledged build"} {
		s.Run(scenario, func() {
			task, _, m, im, resp, entries := s.newCopiedJSONFlatManifestFixture()
			result := resp.GetSegmentResults()[0]
			result.ManifestIndexRewritten = proto.Bool(true)
			result.ManifestIndexBuildIds = []int64{9001, 9002}
			switch scenario {
			case "foreign definition":
				entries[1].IndexID = 999
			case "changed parameters":
				entries[1].Properties[common.JSONCastTypeKey] = "DOUBLE"
			case "wrong files":
				entries[1].IndexFileKeys = []string{"other.bin"}
			case "unacknowledged build":
				result.ManifestIndexBuildIds = []int64{9001}
			}
			_, err := verifyCopiedManifestIndexOwnership(result, task, m, im, m.GetSegment(context.Background(), 2001), "files", entries)
			s.Error(err)
		})
	}
}
