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

package querynode

import (
	"context"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"

	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

const (
	benchmarkMaxNQ = 100
	nb             = 10000
)

func benchmarkQueryCollectionSearch(nq int64, b *testing.B) {
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(zapcore.DebugLevel)

	tx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queryShardObj, err := genSimpleQueryShard(tx)
	assert.NoError(b, err)

	// search only one segment
	assert.Equal(b, 0, queryShardObj.metaReplica.getSegmentNum(segmentTypeSealed))
	assert.Equal(b, 0, queryShardObj.metaReplica.getSegmentNum(segmentTypeGrowing))

	segment, err := genSimpleSealedSegment(nb)
	assert.NoError(b, err)
	err = queryShardObj.metaReplica.setSegment(segment)
	assert.NoError(b, err)

	// segment check
	assert.Equal(b, 1, queryShardObj.metaReplica.getSegmentNum(segmentTypeSealed))
	assert.Equal(b, 0, queryShardObj.metaReplica.getSegmentNum(segmentTypeGrowing))
	seg, err := queryShardObj.metaReplica.getSegmentByID(defaultSegmentID, segmentTypeSealed)
	assert.NoError(b, err)
	assert.Equal(b, int64(nb), seg.getRowCount())

	// TODO:: check string data in segcore
	//sizePerRecord, err := typeutil.EstimateSizePerRecord(genTestCollectionSchema(schemapb.DataType_Int64))
	//assert.NoError(b, err)
	//expectSize := sizePerRecord * nb
	//assert.Equal(b, seg.getMemSize(), int64(expectSize))

	// warming up

	collection, err := queryShardObj.metaReplica.getCollectionByID(defaultCollectionID)
	assert.NoError(b, err)

	iReq, _ := genSearchRequest(nq, IndexFaissIDMap, collection.schema)
	queryReq := &queryPb.SearchRequest{
		Req:             iReq,
		DmlChannels:     []string{defaultDMLChannel},
		SegmentIDs:      []UniqueID{defaultSegmentID},
		FromShardLeader: true,
		Scope:           queryPb.DataScope_Historical,
	}
	searchReq, err := newSearchRequest(collection, queryReq, queryReq.Req.GetPlaceholderGroup())
	assert.NoError(b, err)
	for j := 0; j < 10000; j++ {
		_, _, _, err := searchHistorical(context.TODO(), queryShardObj.metaReplica, searchReq, defaultCollectionID, nil, queryReq.GetSegmentIDs())
		assert.NoError(b, err)
	}

	reqs := make([]*searchRequest, benchmarkMaxNQ/nq)
	for i := 0; i < benchmarkMaxNQ/int(nq); i++ {
		sReq, err := genSearchPlanAndRequests(collection, IndexFaissIDMap, nq)
		assert.NoError(b, err)
		reqs[i] = sReq
	}

	f, err := os.Create("nq_" + strconv.Itoa(int(nq)) + ".perf")
	if err != nil {
		panic(err)
	}
	if err = pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	// start benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := int64(0); j < benchmarkMaxNQ/nq; j++ {
			_, _, _, err := searchHistorical(context.TODO(), queryShardObj.metaReplica, searchReq, defaultCollectionID, nil, queryReq.GetSegmentIDs())
			assert.NoError(b, err)
		}
	}
}

func benchmarkQueryCollectionSearchIndex(nq int64, indexType string, b *testing.B) {
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(zapcore.DebugLevel)

	tx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queryShardObj, err := genSimpleQueryShard(tx)
	assert.NoError(b, err)

	assert.Equal(b, 0, queryShardObj.metaReplica.getSegmentNum(segmentTypeSealed))
	assert.Equal(b, 0, queryShardObj.metaReplica.getSegmentNum(segmentTypeGrowing))

	node, err := genSimpleQueryNode(tx)
	assert.NoError(b, err)
	node.loader.metaReplica = queryShardObj.metaReplica

	err = loadIndexForSegment(tx, node, defaultSegmentID, nb, indexType, L2, schemapb.DataType_Int64)
	assert.NoError(b, err)

	// segment check
	assert.Equal(b, 1, queryShardObj.metaReplica.getSegmentNum(segmentTypeSealed))
	assert.Equal(b, 0, queryShardObj.metaReplica.getSegmentNum(segmentTypeGrowing))
	seg, err := queryShardObj.metaReplica.getSegmentByID(defaultSegmentID, segmentTypeSealed)
	assert.NoError(b, err)
	assert.Equal(b, int64(nb), seg.getRowCount())
	//TODO:: check string data in segcore
	//sizePerRecord, err := typeutil.EstimateSizePerRecord(genSimpleSegCoreSchema())
	//assert.NoError(b, err)
	//expectSize := sizePerRecord * nb
	//assert.Equal(b, seg.getMemSize(), int64(expectSize))

	// warming up
	collection, err := queryShardObj.metaReplica.getCollectionByID(defaultCollectionID)
	assert.NoError(b, err)

	//ollection *Collection, indexType string, nq int32

	searchReq, _ := genSearchPlanAndRequests(collection, indexType, nq)
	for j := 0; j < 10000; j++ {
		_, _, _, err := searchHistorical(context.TODO(), queryShardObj.metaReplica, searchReq, defaultCollectionID, nil, []UniqueID{defaultSegmentID})
		assert.NoError(b, err)
	}

	reqs := make([]*searchRequest, benchmarkMaxNQ/nq)
	for i := int64(0); i < benchmarkMaxNQ/nq; i++ {
		req, err := genSearchPlanAndRequests(collection, indexType, defaultNQ)
		//msg, err := genSearchMsg(collection.schema, nq, indexType)
		assert.NoError(b, err)
		reqs[i] = req
	}

	f, err := os.Create(indexType + "_nq_" + strconv.Itoa(int(nq)) + ".perf")
	if err != nil {
		panic(err)
	}
	if err = pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	// start benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < benchmarkMaxNQ/int(nq); j++ {
			_, _, _, err := searchHistorical(context.TODO(), queryShardObj.metaReplica, searchReq, defaultCollectionID, nil, []UniqueID{defaultSegmentID})
			assert.NoError(b, err)
		}
	}
}

func BenchmarkSearch_NQ1(b *testing.B) { benchmarkQueryCollectionSearch(1, b) }

//func BenchmarkSearch_NQ10(b *testing.B)    { benchmarkQueryCollectionSearch(10, b) }
//func BenchmarkSearch_NQ100(b *testing.B)   { benchmarkQueryCollectionSearch(100, b) }
//func BenchmarkSearch_NQ1000(b *testing.B)  { benchmarkQueryCollectionSearch(1000, b) }
//func BenchmarkSearch_NQ10000(b *testing.B) { benchmarkQueryCollectionSearch(10000, b) }

func BenchmarkSearch_HNSW_NQ1(b *testing.B) {
	benchmarkQueryCollectionSearchIndex(1, IndexHNSW, b)
}

func BenchmarkSearch_IVFFLAT_NQ1(b *testing.B) {
	benchmarkQueryCollectionSearchIndex(1, IndexFaissIVFFlat, b)
}

/*
func BenchmarkSearch_IVFFLAT_NQ10(b *testing.B) {
	benchmarkQueryCollectionSearchIndex(10, IndexFaissIVFFlat, b)
}
func BenchmarkSearch_IVFFLAT_NQ100(b *testing.B) {
	benchmarkQueryCollectionSearchIndex(100, IndexFaissIVFFlat, b)
}
func BenchmarkSearch_IVFFLAT_NQ1000(b *testing.B) {
	benchmarkQueryCollectionSearchIndex(1000, IndexFaissIVFFlat, b)
}
func BenchmarkSearch_IVFFLAT_NQ10000(b *testing.B) {
	benchmarkQueryCollectionSearchIndex(10000, IndexFaissIVFFlat, b)
}
*/
