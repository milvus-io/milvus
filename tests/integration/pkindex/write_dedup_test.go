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

package pkindex

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

// The suite writes the same primary keys twice and checks what a reader sees.
// It turns on streaming.pkindex.enabled, so the streaming node deduplicates
// them: the WAL recovery opens the index of the pchannel at the WAL open.
//
// The suite also checks that a flush hands the index over to object
// storage (the manifest pointer appears in the meta kv), and that the
// deduplication survives a restart of the streaming node.
const (
	dedupDim      = 8
	dedupTagField = "tag"
	dedupPKField  = integration.Int64Field

	dedupPKIndexName = "pk_index"
)

type WriteDedupSuite struct {
	integration.MiniClusterSuite
}

func (s *WriteDedupSuite) SetupSuite() {
	s.WithMilvusConfig("streaming.pkindex.enabled", "true")
	s.MiniClusterSuite.SetupSuite()
}

func TestWriteDedup(t *testing.T) {
	suite.Run(t, new(WriteDedupSuite))
}

// TestDuplicateInsertDoesNotInflateCount checks that inserting a key a second
// time replaces the first row instead of adding one.
func (s *WriteDedupSuite) TestDuplicateInsertDoesNotInflateCount() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collectionName := "TestWriteDedup" + funcutil.GenRandomStr()
	s.createAndLoadCollection(ctx, collectionName)

	const firstRows = 100
	const secondRows = 50
	s.insert(ctx, collectionName, 0, firstRows, 1)
	s.insert(ctx, collectionName, 0, secondRows, 2)

	s.Equal(int64(firstRows), s.queryCount(ctx, collectionName), "the second insert of a key must not add a row")
	s.Equal(map[int64]int64{0: 2, 1: 2, 2: 2}, s.queryTags(ctx, collectionName, []int64{0, 1, 2}), "the row written last is the one that survives")

	s.flush(ctx, collectionName)

	s.Equal(int64(firstRows), s.queryCount(ctx, collectionName), "a flush must not change the number of rows")
	s.Equal(map[int64]int64{0: 2, 1: 2, 2: 2}, s.queryTags(ctx, collectionName, []int64{0, 1, 2}), "a flush must not bring the old rows back")
}

// TestDeleteOfAbsentKeyIsHarmless checks that a delete of keys that were never
// written does not hide a later insert of the same keys.
func (s *WriteDedupSuite) TestDeleteOfAbsentKeyIsHarmless() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collectionName := "TestWriteDedupDelete" + funcutil.GenRandomStr()
	s.createAndLoadCollection(ctx, collectionName)

	deleteResp, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collectionName,
		Expr:           fmt.Sprintf("%s in [1000, 1001]", dedupPKField),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(deleteResp.GetStatus()), deleteResp.GetStatus().GetReason())

	s.insert(ctx, collectionName, 1000, 1, 1)

	s.Equal(int64(1), s.queryCount(ctx, collectionName))
	s.Equal(map[int64]int64{1000: 1}, s.queryTags(ctx, collectionName, []int64{1000}), "the insert after the delete of the same key is visible")
}

// TestFlushCommitsManifest checks that a flush hands the index over to object
// storage: the manifest pointer of the index appears in the meta kv.
func (s *WriteDedupSuite) TestFlushCommitsManifest() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collectionName := "TestWriteDedupManifest" + funcutil.GenRandomStr()
	s.createAndLoadCollection(ctx, collectionName)
	s.insert(ctx, collectionName, 0, 100, 1)
	s.flush(ctx, collectionName)
	versions := s.waitForManifest(ctx, collectionName)
	for vchannel, version := range versions {
		s.Positive(version, "vchannel %s", vchannel)
	}
}

// TestDedupSurvivesStreamingNodeRestart checks that the deduplication
// survives a restart of the streaming node after a handover: the new node
// must know the keys that were handed over to object storage and the keys
// that were still in the WAL. The test does not tell apart a recovery that
// reads the manifest from one that replays the whole WAL; the log of the
// new node shows which one happened.
func (s *WriteDedupSuite) TestDedupSurvivesStreamingNodeRestart() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collectionName := "TestWriteDedupRestart" + funcutil.GenRandomStr()
	s.createAndLoadCollection(ctx, collectionName)

	const handedOver = 100
	const inWAL = 50
	s.insert(ctx, collectionName, 0, handedOver, 1)
	s.flush(ctx, collectionName)
	s.waitForManifest(ctx, collectionName)
	s.insert(ctx, collectionName, handedOver, inWAL, 1)

	for _, sn := range s.Cluster.GetAllStreamingNodes() {
		// Stop sends SIGTERM and escalates to SIGKILL after 10 s. Either way
		// the node is gone; a kill only makes the recovery start from an
		// older checkpoint.
		_ = sn.Stop()
	}
	s.Cluster.AddStreamingNode()

	// the same keys again, from the handed over part and from the WAL part
	s.insert(ctx, collectionName, 0, handedOver+inWAL, 2)
	s.waitUntilQueryable(ctx, collectionName)

	s.Equal(int64(handedOver+inWAL), s.queryCount(ctx, collectionName), "a second insert after the restart must not add a row")
	s.Equal(map[int64]int64{0: 2, handedOver - 1: 2, handedOver: 2, handedOver + inWAL - 1: 2},
		s.queryTags(ctx, collectionName, []int64{0, handedOver - 1, handedOver, handedOver + inWAL - 1}),
		"the row written after the restart is the one that survives")
}

// createAndLoadCollection creates a collection with one user provided int64
// primary key, one float vector and one int64 tag, on a single shard, creates
// the primary key index on the primary key while the collection is empty, and
// loads it.
func (s *WriteDedupSuite) createAndLoadCollection(ctx context.Context, collectionName string) {
	schema := integration.ConstructSchema(collectionName, dedupDim, false,
		&schemapb.FieldSchema{
			FieldID:      100,
			Name:         dedupPKField,
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		},
		&schemapb.FieldSchema{
			FieldID:  101,
			Name:     integration.FloatVecField,
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: strconv.Itoa(dedupDim)},
			},
		},
		&schemapb.FieldSchema{
			FieldID:  102,
			Name:     dedupTagField,
			DataType: schemapb.DataType_Int64,
		},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createResp), createResp.GetReason())

	pkIndexResp, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      dedupPKField,
		IndexName:      dedupPKIndexName,
		ExtraParams:    []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: common.PrimaryKeyIndexType}},
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(pkIndexResp), pkIndexResp.GetReason())
	s.WaitForIndexBuiltWithIndexName(ctx, collectionName, dedupPKField, dedupPKIndexName)

	indexResp, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dedupDim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(indexResp), indexResp.GetReason())
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	loadResp, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(loadResp), loadResp.GetReason())
	s.WaitForLoad(ctx, collectionName)
}

// insert writes rowNum rows whose primary keys start at startPK, all tagged with tag.
func (s *WriteDedupSuite) insert(ctx context.Context, collectionName string, startPK int64, rowNum int, tag int64) {
	insertResp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collectionName,
		FieldsData: []*schemapb.FieldData{
			integration.NewInt64FieldDataWithStart(dedupPKField, rowNum, startPK),
			integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dedupDim),
			integration.NewInt64SameFieldData(dedupTagField, rowNum, tag),
		},
		HashKeys: integration.GenerateHashKeys(rowNum),
		NumRows:  uint32(rowNum),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(insertResp.GetStatus()), insertResp.GetStatus().GetReason())
	s.Require().EqualValues(rowNum, insertResp.GetInsertCnt())
}

func (s *WriteDedupSuite) flush(ctx context.Context, collectionName string) {
	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collectionName},
	})
	s.Require().NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.Require().True(has)
	s.WaitForFlush(ctx, segmentIDs.GetData(), flushTs, "", collectionName)
}

// manifestVersions returns the manifest version of the primary key index on
// every vchannel of the collection, keyed by vchannel. A vchannel without a
// manifest is absent. It returns an error instead of failing the test, so a
// poll can retry it.
func (s *WriteDedupSuite) manifestVersions(ctx context.Context, collectionName string) (map[string]int64, error) {
	descResp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(descResp.GetStatus()); err != nil {
		return nil, err
	}
	indexResp, err := s.Cluster.MilvusClient.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
		CollectionName: collectionName,
		FieldName:      dedupPKField,
		IndexName:      dedupPKIndexName,
	})
	if err != nil {
		return nil, err
	}
	if err := merr.Error(indexResp.GetStatus()); err != nil {
		return nil, err
	}
	if len(indexResp.GetIndexDescriptions()) != 1 {
		return nil, fmt.Errorf("expected 1 description of index %s, got %d", dedupPKIndexName, len(indexResp.GetIndexDescriptions()))
	}
	indexID := indexResp.GetIndexDescriptions()[0].GetIndexID()

	versions := make(map[string]int64)
	for _, vchannel := range descResp.GetVirtualChannelNames() {
		key := path.Join(s.Cluster.RootPath(), "meta", manifest.PointerKey(descResp.GetCollectionID(), indexID, vchannel))
		resp, err := s.Cluster.EtcdCli.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			continue
		}
		var p pkindexpb.ManifestPointer
		if err := proto.Unmarshal(resp.Kvs[0].Value, &p); err != nil {
			return nil, err
		}
		versions[vchannel] = p.GetVersion()
	}
	return versions, nil
}

// waitForManifest waits until every vchannel of the collection has a manifest
// of the primary key index: the handover after a flush commits it
// asynchronously. It returns the manifest version of each vchannel.
func (s *WriteDedupSuite) waitForManifest(ctx context.Context, collectionName string) map[string]int64 {
	descResp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(descResp.GetStatus()), descResp.GetStatus().GetReason())
	vchannels := len(descResp.GetVirtualChannelNames())

	var versions map[string]int64
	s.Require().Eventually(func() bool {
		got, err := s.manifestVersions(ctx, collectionName)
		if err != nil || len(got) != vchannels {
			return false
		}
		versions = got
		return true
	}, 60*time.Second, 500*time.Millisecond, "the flush must commit a manifest of the primary key index on every vchannel")
	return versions
}

// waitUntilQueryable waits until a query of the collection succeeds again:
// after a streaming node restart the delegator of the channel moves, and the
// query path is not serviceable until the new one is ready.
func (s *WriteDedupSuite) waitUntilQueryable(ctx context.Context, collectionName string) {
	s.Require().Eventually(func() bool {
		queryResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName:   collectionName,
			OutputFields:     []string{"count(*)"},
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		return err == nil && merr.Ok(queryResp.GetStatus())
	}, 60*time.Second, 500*time.Millisecond, "the collection must become queryable after the streaming node restart")
}

// queryCount reads the number of rows of the collection with strong consistency.
func (s *WriteDedupSuite) queryCount(ctx context.Context, collectionName string) int64 {
	queryResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(queryResp.GetStatus()), queryResp.GetStatus().GetReason())
	for _, field := range queryResp.GetFieldsData() {
		if field.GetFieldName() == "count(*)" {
			return field.GetScalars().GetLongData().GetData()[0]
		}
	}
	s.Require().Fail("the query response carries no count(*) field")
	return 0
}

// queryTags reads the tag of every primary key of pks that exists.
func (s *WriteDedupSuite) queryTags(ctx context.Context, collectionName string, pks []int64) map[int64]int64 {
	values := make([]string, 0, len(pks))
	for _, pk := range pks {
		values = append(values, strconv.FormatInt(pk, 10))
	}
	queryResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             fmt.Sprintf("%s in [%s]", dedupPKField, strings.Join(values, ", ")),
		OutputFields:     []string{dedupPKField, dedupTagField},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(queryResp.GetStatus()), queryResp.GetStatus().GetReason())

	var keys, tags []int64
	for _, field := range queryResp.GetFieldsData() {
		switch field.GetFieldName() {
		case dedupPKField:
			keys = field.GetScalars().GetLongData().GetData()
		case dedupTagField:
			tags = field.GetScalars().GetLongData().GetData()
		}
	}
	s.Require().Len(tags, len(keys))
	tagOf := make(map[int64]int64, len(keys))
	for i, key := range keys {
		tagOf[key] = tags[i]
	}
	return tagOf
}
