package dql

import (
	"context"
	"slices"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// mzRequeryNode runs a requery's QueryTask through Execute and PostExecute
// against mocked shards: Query replies per shard (already deduplicated and
// pk-sorted, as a delegator returns them) and Materialize replies per shard
// (the workers' batches, duplicates across workers included).
type mzRequeryNode struct {
	namespaceRequeryMockNode
	t     *testing.T
	lb    shardclient.LBPolicy
	mgr   shardclient.ShardClientMgr
	setup func(*QueryTask)
}

func (n *mzRequeryNode) LBPolicy() shardclient.LBPolicy       { return n.lb }
func (n *mzRequeryNode) ShardMgr() shardclient.ShardClientMgr { return n.mgr }

// ExecuteQuery stands in for Proxy.query without PreExecute: it sets what
// PreExecute would and runs the task.
func (n *mzRequeryNode) ExecuteQuery(ctx context.Context, qt taskmodel.Task, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	q := qt.(*QueryTask)
	n.setup(q)
	if err := q.Execute(ctx); err != nil {
		return nil, segcore.StorageCost{}, err
	}
	if err := q.PostExecute(ctx); err != nil {
		return nil, segcore.StorageCost{}, err
	}
	return q.result, q.StorageCost(), nil
}

// mzShardRows is one shard's rows, per worker; the Query reply is their
// newest-timestamp merge.
type mzShardRows struct {
	channel string
	workers []mzRows
}

func (s mzShardRows) queryReply() *internalpb.RetrieveResults {
	type row struct {
		ts   int64
		text string
	}
	newest := make(map[int64]row)
	for _, w := range s.workers {
		for i, pk := range w.intPKs {
			if r, ok := newest[pk]; !ok || w.ts[i] > r.ts {
				newest[pk] = row{w.ts[i], *w.texts[i]}
			}
		}
	}
	pks := make([]int64, 0, len(newest))
	for pk := range newest {
		pks = append(pks, pk)
	}
	slices.Sort(pks)
	texts := make([]string, len(pks))
	for i, pk := range pks {
		texts[i] = newest[pk].text
	}
	return &internalpb.RetrieveResults{
		Status: merr.Success(),
		Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
		FieldsData: []*schemapb.FieldData{
			{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
			}}},
			{FieldId: 101, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: texts}},
			}}},
		},
	}
}

func (s mzShardRows) materializeReply(t *testing.T) *internalpb.MaterializeResults {
	replies := make([]*internalpb.MaterializeResults, len(s.workers))
	for i, w := range s.workers {
		texts := make([]string, len(w.texts))
		for j := range texts {
			texts[j] = *w.texts[j]
		}
		replies[i] = mzWorkerReplyWithPK(t, w.intPKs, w.ts, texts)
	}
	return relay(replies...)
}

// runRequery runs searchWithRequeryPipe's merge, requery, gen_ids and organize
// nodes on a search result with the given per-query hits, against shards.
func runRequery(t *testing.T, materialize bool, shards []mzShardRows, hits []int64) (opMsg, error) {
	search := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: hits}}},
	}}
	msg := opMsg{"reduced": []*milvuspb.SearchResults{search}, pipelineStorageCost: segcore.StorageCost{}}
	return runPipeRequery(t, materialize, shards, searchWithRequeryPipe, []string{"merge", "requery", "gen_ids", "organize"}, msg)
}

// runRankedRequery runs the requery and organize nodes of a pipe that
// requeries a rank result (searchWithRerankRequeryPipe,
// hybridSearchWithRequeryPipe): the rank result's ids, every query's hits
// concatenated and not deduplicated, are both what the requery asks for and
// what organize aligns the fields with.
func runRankedRequery(t *testing.T, materialize bool, shards []mzShardRows, pipe *pipelineDef, hits []int64, topks []int64) (opMsg, error) {
	rank := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		NumQueries: int64(len(topks)),
		Topks:      topks,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: hits}}},
	}}
	var nodes []string
	for _, def := range pipe.nodes {
		switch def.name {
		case "pick_ids", "requery", "to_ids_list", "organize":
			nodes = append(nodes, def.name)
		}
	}
	msg := opMsg{"rank_result": rank, pipelineStorageCost: segcore.StorageCost{}}
	return runPipeRequery(t, materialize, shards, pipe, nodes, msg)
}

// runPipeRequery runs the named nodes of pipe from msg against shards,
// routing the requery to Materialize or Query.
func runPipeRequery(t *testing.T, materialize bool, shards []mzShardRows, pipe *pipelineDef, names []string, msg opMsg) (opMsg, error) {
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.MaterializeEnabled.Key, map[bool]string{true: "true", false: "false"}[materialize])
	defer pt.Reset(pt.ProxyCfg.MaterializeEnabled.Key)

	qns := make(map[string]types.QueryNodeClient, len(shards))
	for _, sh := range shards {
		qn := mocks.NewMockQueryNodeClient(t)
		if materialize {
			qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(sh.materializeReply(t), nil).Once()
		} else {
			qn.EXPECT().Query(mock.Anything, mock.Anything).Return(sh.queryReply(), nil).Once()
		}
		qns[sh.channel] = qn
	}
	lb := shardclient.NewMockLBPolicy(t)
	lb.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything).Maybe()
	lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, w shardclient.CollectionWorkLoad) error {
			for _, sh := range shards {
				if err := w.Exec(ctx, 1, qns[sh.channel], sh.channel); err != nil {
					return err
				}
			}
			return nil
		}).Once()
	mgr := shardclient.NewMockShardClientManager(t)
	mgr.EXPECT().InvalidateShardLeaderCache(mock.Anything).Maybe()

	schema := mzSchema()
	node := &mzRequeryNode{t: t, lb: lb, mgr: mgr, setup: func(q *QueryTask) {
		q.queryParams = &queryParams{limit: typeutil.Unlimited}
		q.schema = mustNewSchemaInfo(schema)
		q.OutputFieldsId = []int64{100, 101}
		q.userOutputFields = []string{"pk", "text"}
	}}
	pkField := schema.GetFields()[0]
	task := &SearchTask{
		ctx:                    context.Background(),
		SearchRequest:          &internalpb.SearchRequest{Base: &commonpb.MsgBase{}},
		request:                &milvuspb.SearchRequest{CollectionName: "c"},
		schema:                 &schemaInfo{CollectionSchema: schema, PkField: pkField},
		translatedOutputFields: []string{"pk", "text"},
		node:                   node,
	}
	var nodes []*Node
	for _, def := range pipe.nodes {
		if slices.Contains(names, def.name) {
			n, err := NewNode(def, task)
			require.NoError(t, err)
			nodes = append(nodes, n)
		}
	}
	require.Len(t, nodes, len(names))
	for _, n := range nodes {
		var err error
		if msg, err = n.Run(context.Background(), nil, msg); err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// A Materialize-routed requery organizes to exactly the fields Query's does:
// one query, several queries sharing hits, duplicates across workers, and a
// requested id no shard returns.
func TestRequery_MaterializeOrganizesLikeQuery(t *testing.T) {
	paramtable.Init()
	int64s := func(v ...int64) []int64 { return v }
	shards := []mzShardRows{
		{"ch1", []mzRows{
			{intPKs: int64s(1, 4, 7), ts: int64s(10, 10, 10), texts: strs("a1", "d1", "g1")},
			{intPKs: int64s(4, 9), ts: int64s(20, 10), texts: strs("d2", "i1")},
		}},
		{"ch2", []mzRows{
			{intPKs: int64s(2, 5), ts: int64s(10, 10), texts: strs("b1", "e1")},
			{intPKs: int64s(3, 5, 8), ts: int64s(10, 5, 10), texts: strs("c1", "e0", "h1")},
		}},
	}
	cases := []struct {
		name     string
		hits     []int64
		fastPath bool
		// queryFast: Query's pk-sorted result happens to be in hit order.
		queryFast bool
		wantErr   error
	}{
		{name: "one query", hits: int64s(9, 4, 2, 7, 1, 5, 3, 8), fastPath: true},
		{name: "one query, hits in pk order", hits: int64s(1, 2, 3, 4, 5, 7, 8, 9), fastPath: true, queryFast: true},
		{name: "several queries, repeated hits", hits: int64s(9, 4, 2 /* q2 */, 4, 5, 1 /* q3 */, 8, 9, 3, 7)},
		{name: "a requested id no shard returns", hits: int64s(9, 4, 6, 2), wantErr: merr.ErrInconsistentRequery},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withMaterializeSchemas(t, materializeSchemaCacheSize)
			q, qErr := runRequery(t, false, shards, c.hits)
			m, mErr := runRequery(t, true, shards, c.hits)
			if c.wantErr != nil {
				require.ErrorIs(t, qErr, c.wantErr)
				require.ErrorIs(t, mErr, c.wantErr)
				return
			}
			require.NoError(t, qErr)
			require.NoError(t, mErr)
			qOut := q["organized_fields"].([][]*schemapb.FieldData)
			mOut := m["organized_fields"].([][]*schemapb.FieldData)
			require.Len(t, mOut, 1)
			require.Equal(t, len(qOut[0]), len(mOut[0]))
			for i := range qOut[0] {
				require.True(t, proto.Equal(qOut[0][i], mOut[0][i]), "field %d:\n%v\n%v", qOut[0][i].GetFieldId(), qOut[0][i], mOut[0][i])
			}
			// Materialize's result is always in requery order; Query's is in
			// pk order, so it is aligned only when the hits are too. Either
			// takes the fast path exactly when the hits are the requery's ids.
			fields := m["fields"].([]*schemapb.FieldData)
			require.Equal(t, c.fastPath, &fields[0] == &mOut[0][0])
			qFields := q["fields"].([]*schemapb.FieldData)
			require.Equal(t, c.queryFast, &qFields[0] == &qOut[0][0])
			// The fast path returns what the general path (PickFieldData)
			// would have.
			ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: c.hits}}}
			general, err := (&organizeOperator{traceCtx: context.Background(), primaryFieldSchema: mzSchema().GetFields()[0], schema: mzSchema()}).
				run(context.Background(), nil, qFields, ids)
			require.NoError(t, err)
			for i, f := range general[0].([][]*schemapb.FieldData)[0] {
				require.True(t, proto.Equal(f, qOut[0][i]), "field %d", f.GetFieldId())
			}
		})
	}
}

// The rerank and hybrid requery pipes requery the rank result's ids, which
// are not deduplicated, and organize by the same ids: with several queries
// hitting the same pk the fill already holds the repeated rows in their
// order, so organize takes its fast path and returns what Query's
// PickFieldData returns. A requested pk no shard returns takes the general
// path and fails the same way.
func TestRequery_MaterializeRankedPipes(t *testing.T) {
	paramtable.Init()
	int64s := func(v ...int64) []int64 { return v }
	shards := []mzShardRows{
		{"ch1", []mzRows{
			{intPKs: int64s(1, 4, 7), ts: int64s(10, 10, 10), texts: strs("a1", "d1", "g1")},
			{intPKs: int64s(4, 9), ts: int64s(20, 10), texts: strs("d2", "i1")},
		}},
		{"ch2", []mzRows{
			{intPKs: int64s(2, 5), ts: int64s(10, 10), texts: strs("b1", "e1")},
			{intPKs: int64s(3, 5, 8), ts: int64s(10, 5, 10), texts: strs("c1", "e0", "h1")},
		}},
	}
	cases := []struct {
		name    string
		hits    []int64
		topks   []int64
		wantErr error
	}{
		{name: "one query", hits: int64s(9, 4, 2, 7), topks: int64s(4)},
		{name: "nq=3, shared pks", hits: int64s(9, 4, 2 /* q2 */, 4, 5, 9 /* q3 */, 2, 4, 3), topks: int64s(3, 3, 3)},
		{name: "nq=2, shared pks, one missing", hits: int64s(9, 4 /* q2 */, 4, 6), topks: int64s(2, 2), wantErr: merr.ErrInconsistentRequery},
	}
	for _, pipe := range []*pipelineDef{searchWithRerankRequeryPipe, hybridSearchWithRequeryPipe} {
		for _, c := range cases {
			t.Run(pipe.name+"/"+c.name, func(t *testing.T) {
				withMaterializeSchemas(t, materializeSchemaCacheSize)
				q, qErr := runRankedRequery(t, false, shards, pipe, c.hits, c.topks)
				m, mErr := runRankedRequery(t, true, shards, pipe, c.hits, c.topks)
				if c.wantErr != nil {
					require.ErrorIs(t, qErr, c.wantErr)
					require.ErrorIs(t, mErr, c.wantErr)
					return
				}
				require.NoError(t, qErr)
				require.NoError(t, mErr)
				qOut := q["organized_fields"].([][]*schemapb.FieldData)
				mOut := m["organized_fields"].([][]*schemapb.FieldData)
				require.Len(t, qOut, 1)
				require.Len(t, mOut, 1)
				require.Equal(t, len(qOut[0]), len(mOut[0]))
				for i := range qOut[0] {
					require.True(t, proto.Equal(qOut[0][i], mOut[0][i]), "field %d:\n%v\n%v", qOut[0][i].GetFieldId(), qOut[0][i], mOut[0][i])
				}
				// One row per hit, repeats included, in hit order.
				pks := mOut[0][0].GetScalars().GetLongData().GetData()
				require.Equal(t, c.hits, pks)
				// Materialize takes organize's fast path: the requery's
				// fields are handed on as they are.
				fields := m["fields"].([]*schemapb.FieldData)
				require.Same(t, &fields[0], &mOut[0][0])
				// Query's pk-sorted result is not in hit order: general path.
				qFields := q["fields"].([]*schemapb.FieldData)
				require.NotSame(t, &qFields[0], &qOut[0][0])
			})
		}
	}
}

// organize's fast path hands its fields input on as it is. Where another
// node also reads those fields (organize_rank_data and organize_result of
// hybridSearchWithRequeryAndRerankByFieldDataPipe), the pipeline turns it off
// (markSharedOrganizeInputs) and the node always gathers a copy.
func TestOrganize_FastPathAndCopyFields(t *testing.T) {
	schema := mzSchema()
	pks := &schemapb.FieldData{FieldId: 100, Type: schemapb.DataType_Int64, FieldName: "pk", Field: &schemapb.FieldData_Scalars{
		Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{3, 1, 2}}}},
	}}
	fields := []*schemapb.FieldData{pks}
	run := func(copyFields bool, ids ...*schemapb.IDs) [][]*schemapb.FieldData {
		op := &organizeOperator{traceCtx: context.Background(), primaryFieldSchema: schema.GetFields()[0], schema: schema, copyFields: copyFields}
		var in any = ids
		if len(ids) == 1 {
			in = ids[0]
		}
		out, err := op.run(context.Background(), nil, fields, in)
		require.NoError(t, err)
		return out[0].([][]*schemapb.FieldData)
	}
	intIDs := func(v ...int64) *schemapb.IDs {
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: v}}}
	}
	cases := []struct {
		name       string
		copyFields bool
		ids        []*schemapb.IDs
		fast       bool
	}{
		{name: "aligned", ids: []*schemapb.IDs{intIDs(3, 1, 2)}, fast: true},
		{name: "aligned, copy_fields", copyFields: true, ids: []*schemapb.IDs{intIDs(3, 1, 2)}},
		{name: "other order", ids: []*schemapb.IDs{intIDs(1, 2, 3)}},
		{name: "subset", ids: []*schemapb.IDs{intIDs(3, 1)}},
		{name: "two lists", ids: []*schemapb.IDs{intIDs(3, 1, 2), intIDs(2)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := run(c.copyFields, c.ids...)
			require.Len(t, out, len(c.ids))
			require.Equal(t, c.fast, &out[0][0] == &fields[0])
			require.Equal(t, c.ids[0].GetIntId().GetData(), out[0][0].GetScalars().GetLongData().GetData())
		})
	}

	// The pipeline decides copyFields from how many nodes read the input.
	build := func(def *pipelineDef, extra ...*nodeDef) map[string]bool {
		p := &pipeline{}
		for _, d := range append(append([]*nodeDef{}, def.nodes...), extra...) {
			n := &Node{name: d.name, opName: d.opName, inputs: d.inputs, outputs: d.outputs}
			if d.opName == organizeOp {
				n.op = &organizeOperator{}
			}
			p.nodes = append(p.nodes, n)
		}
		p.markSharedOrganizeInputs()
		copied := make(map[string]bool)
		for _, n := range p.nodes {
			if op, ok := n.op.(*organizeOperator); ok {
				copied[n.name] = op.copyFields
			}
		}
		return copied
	}
	require.Equal(t, map[string]bool{"organize_rank_data": true, "organize_result": true},
		build(hybridSearchWithRequeryAndRerankByFieldDataPipe))
	for _, def := range []*pipelineDef{searchWithRequeryPipe, searchWithRerankPipe, searchWithRerankRequeryPipe, hybridSearchWithRequeryPipe, searchWithOrderByPipe} {
		for name, c := range build(def) {
			require.False(t, c, "%s/%s", def.name, name)
		}
	}
	// A node added later that also reads fields turns it off.
	for name, c := range build(searchWithRequeryPipe, &nodeDef{name: "peek", inputs: []string{"fields"}, opName: lambdaOp}) {
		require.True(t, c, name)
	}
}

// nullVecSchema is a pk and a nullable 2-dim float vector.
func nullVecSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}},
		},
	}}
}

// nullVecMaterializeFields is a Materialize requery's fields for requested:
// one worker returns every requested pk with its vector null.
func nullVecMaterializeFields(t *testing.T, requested []int64) []*schemapb.FieldData {
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"}, []string{"1,100,102", "102"})
	us := arrow.NewSchema([]arrow.Field{
		{Name: "100", Type: arrow.PrimitiveTypes.Int64},
		{Name: "102", Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Nullable: true},
	}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, us)
	pks := slices.Clone(requested)
	slices.Sort(pks)
	b.Field(0).(*array.Int64Builder).AppendValues(pks, nil)
	pairs := make([][2]int64, len(pks))
	ts := make([]int64, len(pks))
	for i := range pks {
		b.Field(1).AppendNull()
		pairs[i] = [2]int64{0, int64(i)}
		ts[i] = 1
	}
	u := b.NewRecord()
	b.Release()
	defer u.Release()
	header := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
		FieldsData: []*schemapb.FieldData{{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ts}}},
		}}},
	}
	rec, err := materialize.BuildBatch(header, &queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest(pairs)})
	require.NoError(t, err)
	defer rec.Release()
	s, e, err := materialize.EncodeBatch(rec, materialize.CodecNone)
	require.NoError(t, err)
	reply := &internalpb.MaterializeResults{
		Header: &internalpb.MaterializeHeader{
			Status: merr.Success(), BodySchema: s, SchemaFingerprint: materialize.SchemaFingerprint(s),
			BodyRawSize: materialize.RawSize(rec),
		},
		Body: [][]byte{e},
	}
	f := newTestFill(nullVecSchema(), 0, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: requested}}})
	defer f.release()
	require.NoError(t, f.add(0, "a", reply))
	got, err := f.finish()
	require.NoError(t, err)
	return got.GetFieldsData()
}

// organize's fast path hands a requery's fields on as they are, skipping
// PickFieldData's seeding. That seeding mirrors each source column's own
// payload oneof, so with every row picked once, in order, it adds nothing:
// a nullable vector column whose rows are all null comes out the same
// either way, whether its payload oneof is set (Materialize's gather, a
// seeded Query reduce) or unset (a Query result that never seeded it).
func TestOrganize_FastPathNullableVectorAllNull(t *testing.T) {
	schema := nullVecSchema()
	pk := func(v ...int64) *schemapb.FieldData {
		return &schemapb.FieldData{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: v}}},
		}}
	}
	vec := func(data *schemapb.VectorField) *schemapb.FieldData {
		fd := &schemapb.FieldData{FieldId: 102, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: data}}
		typeutil.SetFieldDataValidData(fd, []bool{false, false})
		return fd
	}
	cases := []struct {
		name   string
		fields func(t *testing.T) []*schemapb.FieldData
		ids    []int64
		// oneofSet: the vector column's payload oneof is set.
		oneofSet bool
	}{
		{name: "materialize", ids: []int64{2, 1}, oneofSet: true, fields: func(t *testing.T) []*schemapb.FieldData {
			return nullVecMaterializeFields(t, []int64{2, 1})
		}},
		{name: "query, oneof set", ids: []int64{1, 2}, oneofSet: true, fields: func(*testing.T) []*schemapb.FieldData {
			return []*schemapb.FieldData{pk(1, 2), vec(&schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}}})}
		}},
		{name: "query, oneof unset", ids: []int64{1, 2}, fields: func(*testing.T) []*schemapb.FieldData {
			return []*schemapb.FieldData{pk(1, 2), vec(&schemapb.VectorField{Dim: 2})}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fields := c.fields(t)
			ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: c.ids}}}
			organize := func(copyFields bool) []*schemapb.FieldData {
				op := &organizeOperator{traceCtx: context.Background(), primaryFieldSchema: schema.GetFields()[0], schema: schema, copyFields: copyFields}
				out, err := op.run(context.Background(), nil, fields, ids)
				require.NoError(t, err)
				return out[0].([][]*schemapb.FieldData)[0]
			}
			fast, general := organize(false), organize(true)
			require.Same(t, &fields[0], &fast[0], "fast path")
			require.NotSame(t, &fields[0], &general[0], "general path")
			require.Equal(t, len(general), len(fast))
			for i := range general {
				require.True(t, proto.Equal(general[i], fast[i]), "field %d:\n%v\n%v", general[i].GetFieldId(), general[i], fast[i])
			}
			for _, f := range fast {
				if f.GetFieldId() == 102 {
					require.Equal(t, []bool{false, false}, typeutil.GetFieldDataValidData(f))
					require.Equal(t, c.oneofSet, f.GetVectors().GetData() != nil)
				}
			}
		})
	}
}
