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

package materialize

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

const (
	materializeKey = "proxy.materialize.enabled"

	pkField      = "pk"
	vecField     = "vec"
	varcharField = "varchar"
	jsonField    = "json"

	dim       = 128
	rowNum    = 3000
	dupNum    = 200
	topK      = 50
	nq        = 2
	indexType = integration.IndexFaissIvfFlat
	metricTyp = metric.L2

	fallbackMetric = "milvus_proxy_materialize_fallback_total"
	shardMetric    = "milvus_proxy_materialize_shard_total"
)

type MaterializeSuite struct {
	integration.MiniClusterSuite
}

// SetupSuite starts the cluster with metrics ports, which proxyCounters
// scrapes.
func (s *MaterializeSuite) SetupSuite() {
	s.WithOptions(integration.WithClusterOptions(cluster.WithMetricsPorts()))
	s.MiniClusterSuite.SetupSuite()
}

// varcharValue is the VarChar of row i in insert round (1 or 2).
func varcharValue(round, i int) string {
	return fmt.Sprintf("v%d_%d", round, i)
}

// varcharValid reports whether the VarChar of row i in round 1 is non-null.
// Round 2 (the duplicates) is always valid so the newest value is checkable.
func varcharValid(round, i int) bool {
	return round == 2 || i%5 != 0
}

func (s *MaterializeSuite) buildRows(round, start, n int) []*schemapb.FieldData {
	pks := make([]int64, n)
	strs := make([]string, 0, n)
	valid := make([]bool, n)
	jsons := make([][]byte, n)
	for i := 0; i < n; i++ {
		pk := start + i
		pks[i] = int64(pk)
		valid[i] = varcharValid(round, pk)
		if valid[i] {
			// nullable columns are inserted compact: valid values only.
			strs = append(strs, varcharValue(round, pk))
		}
		jsons[i] = []byte(fmt.Sprintf(`{"round":%d,"pk":%d,"tag":"t%d"}`, round, pk, pk%7))
	}
	pkData := &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldName: pkField,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
		}},
	}
	varcharData := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar, FieldName: varcharField,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: strs}},
		}},
	}
	typeutil.SetFieldDataValidData(varcharData, valid)
	jsonData := &schemapb.FieldData{
		Type: schemapb.DataType_JSON, FieldName: jsonField,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: jsons}},
		}},
	}
	vecData := integration.NewFloatVectorFieldData(vecField, n, dim)
	return []*schemapb.FieldData{pkData, varcharData, jsonData, vecData}
}

func (s *MaterializeSuite) insert(ctx context.Context, collection string, round, start, n int) {
	resp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     s.buildRows(round, start, n),
		HashKeys:       integration.GenerateHashKeys(n),
		NumRows:        uint32(n),
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
}

// setupCollection creates the collection, inserts rowNum rows, re-inserts the
// first dupNum pks with new VarChar values (newer timestamps, so duplicates
// exist), flushes, builds the index and loads.
func (s *MaterializeSuite) setupCollection(ctx context.Context) string {
	collection := "TestMaterialize" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{
		Name: collection,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: pkField, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{
				FieldID: 101, Name: vecField, DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(dim)}},
			},
			{
				FieldID: 102, Name: varcharField, DataType: schemapb.DataType_VarChar, Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}},
			},
			{FieldID: 103, Name: jsonField, DataType: schemapb.DataType_JSON},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaled,
		ShardsNum:      2,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))

	s.insert(ctx, collection, 1, 0, rowNum)
	s.insert(ctx, collection, 2, 0, dupNum)

	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{collection}})
	s.Require().NoError(err)
	segIDs, has := flushResp.GetCollSegIDs()[collection]
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[collection]
	s.Require().True(has)
	s.WaitForFlush(ctx, segIDs.GetData(), flushTs, "", collection)

	status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collection,
		FieldName:      vecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, indexType, metricTyp),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, collection, vecField)

	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, collection)
	return collection
}

// searchRequest is built once and reused for both runs: the placeholder
// group holds random query vectors, so rebuilding it would change the query.
func (s *MaterializeSuite) searchRequest(collection string) *milvuspb.SearchRequest {
	return integration.ConstructSearchRequest("", collection, pkField+" >= 0", vecField,
		schemapb.DataType_FloatVector, []string{varcharField, jsonField, vecField},
		metricTyp, integration.GetSearchParams(indexType, metricTyp), nq, dim, topK, -1)
}

// Nullable scalar columns come back row-dense: Data has one slot per row
// (zero value under a null) and ValidData one bit per row; segcore may omit
// Data entirely for an all-null column, hence the bounds guards below.
//
// normalizeNulls zeroes every value under a false validity bit, the one
// place Materialize may differ from Query (design doc 20261019 section 7).
// Values under a true bit are untouched, so real differences still fail.
func normalizeNulls(fds []*schemapb.FieldData) {
	for _, fd := range fds {
		valid := fd.GetValidData()
		if len(valid) == 0 {
			continue
		}
		if sd := fd.GetScalars().GetStringData(); sd != nil {
			for i, v := range valid {
				if !v && i < len(sd.Data) {
					sd.Data[i] = ""
				}
			}
		}
		if jd := fd.GetScalars().GetJsonData(); jd != nil {
			for i, v := range valid {
				if !v && i < len(jd.Data) {
					jd.Data[i] = nil
				}
			}
		}
	}
}

// proxyCounters scrapes the default proxy's own metrics endpoint and returns
// the summed materialize_shard_total and materialize_fallback_total. The
// counter vectors print nothing until incremented, so a missing line is 0;
// the scrape must contain other milvus_proxy_ series to prove it reached the
// proxy. Any failure fails the test.
func (s *MaterializeSuite) proxyCounters() (shards, fallbacks float64) {
	addr := s.Cluster.DefaultProxy().GetMetricsAddr()
	s.Require().NotEmpty(addr, "proxy has no metrics port")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("http://" + addr + "/metrics")
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	s.Require().NoError(err)
	sawProxy := false
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "milvus_proxy_") {
			sawProxy = true
		}
		var dst *float64
		switch {
		case strings.HasPrefix(line, shardMetric):
			dst = &shards
		case strings.HasPrefix(line, fallbackMetric):
			dst = &fallbacks
		default:
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		s.Require().NoError(err)
		*dst += v
	}
	s.Require().True(sawProxy, "metrics endpoint %s exposes no proxy series", addr)
	return shards, fallbacks
}

func (s *MaterializeSuite) query(ctx context.Context, collection string) *milvuspb.QueryResults {
	q, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collection,
		Expr:           pkField + " >= 0",
		OutputFields:   []string{"*"},
	})
	s.Require().NoError(merr.CheckRPCCall(q.GetStatus(), err))
	normalizeNulls(q.GetFieldsData())
	return q
}

func (s *MaterializeSuite) search(ctx context.Context, req *milvuspb.SearchRequest) *milvuspb.SearchResults {
	sr, err := s.Cluster.MilvusClient.Search(ctx, req)
	s.Require().NoError(merr.CheckRPCCall(sr.GetStatus(), err))
	normalizeNulls(sr.GetResults().GetFieldsData())
	return sr
}

// pkIndex maps each pk to its row, failing on a pk returned twice.
func (s *MaterializeSuite) pkIndex(q *milvuspb.QueryResults) map[int64]int {
	var pks []int64
	for _, fd := range q.GetFieldsData() {
		if fd.GetFieldName() == pkField {
			pks = fd.GetScalars().GetLongData().GetData()
		}
	}
	idx := make(map[int64]int, len(pks))
	for i, pk := range pks {
		_, dup := idx[pk]
		s.Require().False(dup, "pk %d returned twice", pk)
		idx[pk] = i
	}
	return idx
}

func column(q *milvuspb.QueryResults, name string) *schemapb.FieldData {
	return fieldByName(q.GetFieldsData(), name)
}

func fieldByName(fds []*schemapb.FieldData, name string) *schemapb.FieldData {
	for _, fd := range fds {
		if fd.GetFieldName() == name {
			return fd
		}
	}
	return nil
}

// equalQuery compares the two Query results column by column, matching rows
// by pk: Materialize returns a filter query's rows in arrival order, not
// sorted by pk (design doc 20261019 §7), so only the row sets must match.
// Both results are rebuilt in a's pk order through the same copy, so the
// comparison is exact per column, validity included.
func (s *MaterializeSuite) equalQuery(a, b *milvuspb.QueryResults) {
	ia, ib := s.pkIndex(a), s.pkIndex(b)
	s.Require().Len(ia, rowNum)
	s.Require().Len(ib, rowNum)
	if len(a.GetFieldsData()) != len(b.GetFieldsData()) {
		s.FailNow("query column count differs",
			"query=%d materialize=%d", len(a.GetFieldsData()), len(b.GetFieldsData()))
	}
	order := column(a, pkField).GetScalars().GetLongData().GetData()
	ra, rb := s.inPKOrder(a, ia, order), s.inPKOrder(b, ib, order)
	for i, fa := range ra {
		fb := rb[i]
		s.Require().Equal(fa.GetFieldName(), fb.GetFieldName(), "column %d", i)
		s.Require().True(proto.Equal(fa, fb),
			"column %s differs between Query and Materialize", fa.GetFieldName())
	}
}

// inPKOrder copies q's columns, sorted by name, with their rows in the pk
// order given.
func (s *MaterializeSuite) inPKOrder(q *milvuspb.QueryResults, idx map[int64]int, order []int64) []*schemapb.FieldData {
	src := append([]*schemapb.FieldData(nil), q.GetFieldsData()...)
	sort.Slice(src, func(i, j int) bool { return src[i].GetFieldName() < src[j].GetFieldName() })
	dst := typeutil.PrepareResultFieldData(src, int64(len(order)))
	// The per-column payload index of a row, as organize computes it.
	idxs := typeutil.NewFieldDataIdxComputer(src)
	for _, pk := range order {
		i, ok := idx[pk]
		s.Require().True(ok, "pk %d missing", pk)
		typeutil.AppendFieldData(dst, src, int64(i), idxs.Compute(int64(i))...)
	}
	return dst
}

// equalSearch compares ids, scores, topks and every requeried column, by
// name: the requery asks for its output fields in set order, so their column
// order varies from one search to the next on either path.
func (s *MaterializeSuite) equalSearch(a, b *milvuspb.SearchResults) {
	ra, rb := a.GetResults(), b.GetResults()
	s.Require().NotEmpty(ra.GetTopks())
	s.Equal(ra.GetTopks(), rb.GetTopks())
	s.True(proto.Equal(ra.GetIds(), rb.GetIds()), "search ids differ")
	s.Equal(ra.GetScores(), rb.GetScores())
	s.Require().Len(rb.GetFieldsData(), len(ra.GetFieldsData()))
	for _, fa := range ra.GetFieldsData() {
		fb := fieldByName(rb.GetFieldsData(), fa.GetFieldName())
		s.Require().NotNil(fb, "search requery column %s missing", fa.GetFieldName())
		s.True(proto.Equal(fa, fb), "search requery column %s differs", fa.GetFieldName())
	}
}

// assertNewestVersion checks the dupNum re-inserted pks carry the second
// insert's VarChar and every other pk its first insert's.
func (s *MaterializeSuite) assertNewestVersion(q *milvuspb.QueryResults) {
	idx := s.pkIndex(q)
	strs := column(q, varcharField).GetScalars().GetStringData().GetData()
	valid := column(q, varcharField).GetValidData()
	for pk := 0; pk < rowNum; pk++ {
		i, ok := idx[int64(pk)]
		s.Require().True(ok, "pk %d missing", pk)
		round := 1
		if pk < dupNum {
			round = 2
		}
		if varcharValid(round, pk) {
			s.Require().True(len(valid) == 0 || valid[i], "pk %d should be valid", pk)
			s.Require().Equal(varcharValue(round, pk), strs[i], "pk %d is not the newest version", pk)
		} else {
			s.Require().Len(valid, rowNum)
			s.Require().False(valid[i], "pk %d should be null", pk)
		}
	}
}

func (s *MaterializeSuite) TestMaterializeEqualsQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	collection := s.setupCollection(ctx)
	searchReq := s.searchRequest(collection)

	// The proxy is a separate process: flip its config through etcd.
	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{materializeKey: "false"})
	defer revert()
	shards0, _ := s.proxyCounters()
	q1 := s.query(ctx, collection)
	shards1, _ := s.proxyCounters()
	sr1 := s.search(ctx, searchReq)
	shards2, _ := s.proxyCounters()
	s.Equal(shards0, shards1, "Query used Materialize while it was off")
	s.Equal(shards1, shards2, "search requery used Materialize while it was off")

	revert2 := s.Cluster.MustModifyMilvusConfig(map[string]string{materializeKey: "true"})
	defer revert2()
	q2 := s.query(ctx, collection)
	shards3, _ := s.proxyCounters()
	sr2 := s.search(ctx, searchReq)
	shards4, fallbacks := s.proxyCounters()
	// Equal results alone prove nothing if both runs used Query, so each
	// call must have served at least one shard through Materialize.
	s.Greater(shards3, shards2, "Query did not go through Materialize")
	s.Greater(shards4, shards3, "search requery did not go through Materialize")
	s.Zero(fallbacks, "Materialize fell back to Query")

	s.equalQuery(q1, q2)
	s.equalSearch(sr1, sr2)
	s.assertNewestVersion(q1)
	s.assertNewestVersion(q2)

	s.upsertThenQuery(ctx, collection)
}

// upsertThenQuery upserts a few loaded pks with a new VarChar while
// Materialize is on, and checks a strong query returns the new values.
func (s *MaterializeSuite) upsertThenQuery(ctx context.Context, collection string) {
	const start, n = 1, 4 // pks 1..4: varcharValid holds for all of them
	before, _ := s.proxyCounters()
	resp, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		CollectionName: collection,
		FieldsData:     s.buildRows(3, start, n),
		HashKeys:       integration.GenerateHashKeys(n),
		NumRows:        uint32(n),
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))

	q, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:        collection,
		Expr:                  fmt.Sprintf("%s in [1, 2, 3, 4]", pkField),
		OutputFields:          []string{pkField, varcharField},
		ConsistencyLevel:      commonpb.ConsistencyLevel_Strong,
		UseDefaultConsistency: false,
	})
	s.Require().NoError(merr.CheckRPCCall(q.GetStatus(), err))
	idx := s.pkIndex(q)
	s.Require().Len(idx, n)
	strs := column(q, varcharField).GetScalars().GetStringData().GetData()
	for pk := start; pk < start+n; pk++ {
		i, ok := idx[int64(pk)]
		s.Require().True(ok, "pk %d missing after upsert", pk)
		s.Require().Equal(varcharValue(3, pk), strs[i], "pk %d does not carry the upserted value", pk)
	}
	after, fallbacks := s.proxyCounters()
	s.Greater(after, before, "upsert and its query did not go through Materialize")
	s.Zero(fallbacks, "Materialize fell back to Query")
}

func TestMaterialize(t *testing.T) {
	suite.Run(t, new(MaterializeSuite))
}
