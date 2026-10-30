package dql

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func mzSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
	}}
}

// mzWorkerReply builds one worker's Materialize reply holding rows (pk, ts, text).
func mzWorkerReply(t *testing.T, pks []int64, ts []int64, texts []string) *internalpb.MaterializeResults {
	t.Helper()
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"}, []string{"1,101", ""})
	us := arrow.NewSchema([]arrow.Field{{Name: "101", Type: arrow.BinaryTypes.String}}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, us)
	for _, s := range texts {
		b.Field(0).(*array.StringBuilder).Append(s)
	}
	u := b.NewRecord()
	b.Release()
	defer u.Release()
	pairs := make([][2]int64, len(pks))
	for i := range pks {
		pairs[i] = [2]int64{0, int64(i)}
	}
	sel := &queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest(pairs)}
	header := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
		FieldsData: []*schemapb.FieldData{{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ts}}},
		}}},
	}
	rec, err := materialize.BuildBatch(header, sel)
	require.NoError(t, err)
	defer rec.Release()
	s, e, err := materialize.EncodeBatch(rec, materialize.CodecNone)
	require.NoError(t, err)
	return &internalpb.MaterializeResults{
		Header: &internalpb.MaterializeHeader{
			Status: merr.Success(), BodySchema: s, SchemaFingerprint: materialize.SchemaFingerprint(s),
			BodyRawSize: materialize.RawSize(rec),
		},
		Body: [][]byte{e},
	}
}

func relay(rs ...*internalpb.MaterializeResults) *internalpb.MaterializeResults {
	out := &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success()}}
	for _, r := range rs {
		out.Header.BodySchema = r.GetHeader().GetBodySchema()
		out.Header.SchemaFingerprint = r.GetHeader().GetSchemaFingerprint()
		out.Header.BodyRawSize += r.GetHeader().GetBodyRawSize()
		out.Body = append(out.Body, r.GetBody()...)
	}
	return out
}

func textCol(t *testing.T, r *internalpb.RetrieveResults) []string {
	for _, fd := range r.GetFieldsData() {
		if fd.GetFieldId() == 101 {
			return fd.GetScalars().GetStringData().GetData()
		}
	}
	t.Fatal("no text column")
	return nil
}

// mzWorkerReplyWithPK is mzWorkerReply for a request that also outputs the pk:
// segcore then exports the pk as a user Arrow column, and field_order lists
// the timestamp, the pk and the text field.
func mzWorkerReplyWithPK(t *testing.T, pks []int64, ts []int64, texts []string) *internalpb.MaterializeResults {
	t.Helper()
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"}, []string{"1,100,101", ""})
	us := arrow.NewSchema([]arrow.Field{
		{Name: "100", Type: arrow.PrimitiveTypes.Int64},
		{Name: "101", Type: arrow.BinaryTypes.String},
	}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, us)
	b.Field(0).(*array.Int64Builder).AppendValues(pks, nil)
	b.Field(1).(*array.StringBuilder).AppendValues(texts, nil)
	u := b.NewRecord()
	b.Release()
	defer u.Release()
	pairs := make([][2]int64, len(pks))
	for i := range pks {
		pairs[i] = [2]int64{0, int64(i)}
	}
	sel := &queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest(pairs)}
	header := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
		FieldsData: []*schemapb.FieldData{{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ts}}},
		}}},
	}
	rec, err := materialize.BuildBatch(header, sel)
	require.NoError(t, err)
	defer rec.Release()
	s, e, err := materialize.EncodeBatch(rec, materialize.CodecNone)
	require.NoError(t, err)
	return &internalpb.MaterializeResults{
		Header: &internalpb.MaterializeHeader{
			Status: merr.Success(), BodySchema: s, SchemaFingerprint: materialize.SchemaFingerprint(s),
			BodyRawSize: materialize.RawSize(rec),
		},
		Body: [][]byte{e},
	}
}

// stripSchema drops the schema bytes, as a worker does when the request named
// the schema's fingerprint.
func stripSchema(r *internalpb.MaterializeResults) *internalpb.MaterializeResults {
	r.Header.BodySchema = nil
	return r
}

// mzRows is one worker's rows: int64 or string pks, timestamps, and a text
// column whose nil entries are nulls.
type mzRows struct {
	intPKs []int64
	strPKs []string
	ts     []int64
	texts  []*string
}

func sp(s string) *string { return &s }

func strs(vals ...string) []*string {
	out := make([]*string, len(vals))
	for i := range vals {
		out[i] = sp(vals[i])
	}
	return out
}

// mzSchemaOf is mzSchema with a string pk and/or a nullable text field.
func mzSchemaOf(strPK, nullable bool) *schemapb.CollectionSchema {
	s := mzSchema()
	if strPK {
		s.Fields[0].DataType = schemapb.DataType_VarChar
		s.Fields[0].TypeParams = []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}}
	}
	s.Fields[1].Nullable = nullable
	return s
}

// mzReplyOf builds one worker's Materialize reply for rows; nullable marks
// the text column as carrying a validity bitmap.
func mzReplyOf(t *testing.T, rows mzRows, nullable bool) *internalpb.MaterializeResults {
	t.Helper()
	valid := ""
	if nullable {
		valid = "101"
	}
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"}, []string{"1,101", valid})
	us := arrow.NewSchema([]arrow.Field{{Name: "101", Type: arrow.BinaryTypes.String, Nullable: nullable}}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, us)
	for _, s := range rows.texts {
		if s == nil {
			b.Field(0).(*array.StringBuilder).AppendNull()
		} else {
			b.Field(0).(*array.StringBuilder).Append(*s)
		}
	}
	u := b.NewRecord()
	b.Release()
	defer u.Release()
	pairs := make([][2]int64, len(rows.texts))
	for i := range pairs {
		pairs[i] = [2]int64{0, int64(i)}
	}
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: rows.intPKs}}}
	if rows.strPKs != nil {
		ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: rows.strPKs}}}
	}
	header := &segcorepb.RetrieveResults{
		Ids: ids,
		FieldsData: []*schemapb.FieldData{{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: rows.ts}}},
		}}},
	}
	rec, err := materialize.BuildBatch(header, &queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest(pairs)})
	require.NoError(t, err)
	defer rec.Release()
	s, e, err := materialize.EncodeBatch(rec, materialize.CodecNone)
	require.NoError(t, err)
	return &internalpb.MaterializeResults{
		Header: &internalpb.MaterializeHeader{
			Status: merr.Success(), BodySchema: s, SchemaFingerprint: materialize.SchemaFingerprint(s),
			BodyRawSize: materialize.RawSize(rec), AllRetrieveCount: int64(len(rows.texts)),
		},
		Body: [][]byte{e},
	}
}

// textOf reads the text column back, nil for a null, whether the payload
// holds every row or only the valid ones.
func textOf(t *testing.T, r *internalpb.RetrieveResults) []*string {
	t.Helper()
	for _, fd := range r.GetFieldsData() {
		if fd.GetFieldId() != 101 {
			continue
		}
		data := fd.GetScalars().GetStringData().GetData()
		valid := typeutil.GetFieldDataValidData(fd)
		if len(valid) == 0 {
			return strs(data...)
		}
		out := make([]*string, len(valid))
		compact := len(data) < len(valid)
		j := 0
		for i, ok := range valid {
			switch {
			case !ok:
				if !compact {
					j++
				}
			default:
				out[i] = sp(data[j])
				j++
			}
		}
		return out
	}
	t.Fatal("no text column")
	return nil
}

// mzShardReply is one shard's reply as its delegator relays it.
type mzShardReply struct {
	channel string
	workers []mzRows
}

func TestMaterializeFill(t *testing.T) {
	int64s := func(v ...int64) []int64 { return v }
	cases := []struct {
		name      string
		strPK     bool
		nullable  bool
		requested *schemapb.IDs
		maxOut    int64
		replies   []mzShardReply
		wantInt   []int64
		wantStr   []string
		wantText  []*string
		aligned   bool
		wantErr   error
	}{
		{
			name: "filter query keeps arrival order, not pk order",
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(5, 7), ts: int64s(1, 1), texts: strs("e", "g")}, {intPKs: int64s(1), ts: int64s(1), texts: strs("a")}}},
				{"b", []mzRows{{intPKs: int64s(3), ts: int64s(1), texts: strs("c")}}},
			},
			wantInt: int64s(5, 7, 1, 3), wantText: strs("e", "g", "a", "c"),
		},
		{
			name: "same shard: newest timestamp wins, in the first one's position",
			replies: []mzShardReply{{"a", []mzRows{
				{intPKs: int64s(1, 2), ts: int64s(10, 10), texts: strs("old", "b")},
				{intPKs: int64s(1), ts: int64s(20), texts: strs("new")},
			}}},
			wantInt: int64s(1, 2), wantText: strs("new", "b"),
		},
		{
			name: "same shard: an older timestamp does not replace",
			replies: []mzShardReply{{"a", []mzRows{
				{intPKs: int64s(1), ts: int64s(20), texts: strs("new")},
				{intPKs: int64s(1), ts: int64s(10), texts: strs("old")},
			}}},
			wantInt: int64s(1), wantText: strs("new"),
		},
		{
			name: "same shard: equal timestamps keep the first seen",
			replies: []mzShardReply{{"a", []mzRows{
				{intPKs: int64s(1), ts: int64s(10), texts: strs("first")},
				{intPKs: int64s(1), ts: int64s(10), texts: strs("second")},
			}}},
			wantInt: int64s(1), wantText: strs("first"),
		},
		{
			name: "same shard: a zero timestamp never replaces, a later one replaces it",
			replies: []mzShardReply{{"a", []mzRows{
				{intPKs: int64s(1, 2), ts: int64s(0, 5), texts: strs("zero", "five")},
				{intPKs: int64s(1, 2), ts: int64s(5, 0), texts: strs("five", "zero")},
			}}},
			wantInt: int64s(1, 2), wantText: strs("five", "five"),
		},
		{
			name: "a pk from two shards is a data integrity error",
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(1), ts: int64s(1), texts: strs("a")}}},
				{"b", []mzRows{{intPKs: int64s(1), ts: int64s(2), texts: strs("b")}}},
			},
			wantErr: merr.ErrDataIntegrity,
		},
		{
			name:   "a shard's rows over maxOutputSize fail",
			maxOut: 1,
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(1), ts: int64s(1), texts: strs("aaaaaaaa")}}},
			},
			wantErr: merr.ErrParameterInvalid,
		},
		{
			name:      "requery follows the requested order across shards",
			requested: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: int64s(5, 1, 3)}}},
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(1, 5), ts: int64s(1, 1), texts: strs("a", "e")}}},
				{"b", []mzRows{{intPKs: int64s(3), ts: int64s(1), texts: strs("c")}}},
			},
			wantInt: int64s(5, 1, 3), wantText: strs("e", "a", "c"), aligned: true,
		},
		{
			name:      "requery repeats a pk requested twice",
			requested: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: int64s(3, 5, 3)}}},
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(3, 5), ts: int64s(1, 1), texts: strs("c", "e")}}},
			},
			wantInt: int64s(3, 5, 3), wantText: strs("c", "e", "c"), aligned: true,
		},
		{
			name:      "requery leaves out a missing pk and is not aligned",
			requested: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: int64s(9, 3, 5)}}},
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(3, 5), ts: int64s(1, 1), texts: strs("c", "e")}}},
			},
			wantInt: int64s(3, 5), wantText: strs("c", "e"), aligned: false,
		},
		{
			name:      "requery drops a pk it did not ask for",
			requested: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: int64s(5)}}},
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(4, 5), ts: int64s(1, 1), texts: strs("d", "e")}}},
			},
			wantInt: int64s(5), wantText: strs("e"), aligned: true,
		},
		{
			name:      "requery: newest timestamp wins within a shard",
			requested: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: int64s(2, 1)}}},
			replies: []mzShardReply{{"a", []mzRows{
				{intPKs: int64s(1, 2), ts: int64s(10, 10), texts: strs("old", "b")},
				{intPKs: int64s(1), ts: int64s(20), texts: strs("new")},
			}}},
			wantInt: int64s(2, 1), wantText: strs("b", "new"), aligned: true,
		},
		{
			name:  "string pks, filter query",
			strPK: true,
			replies: []mzShardReply{
				{"a", []mzRows{{strPKs: []string{"x", "y"}, ts: int64s(1, 1), texts: strs("1", "2")}}},
				{"b", []mzRows{{strPKs: []string{"a"}, ts: int64s(1), texts: strs("3")}}},
			},
			wantStr: []string{"x", "y", "a"}, wantText: strs("1", "2", "3"),
		},
		{
			name:      "string pks, requery",
			strPK:     true,
			requested: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"y", "a", "x"}}}},
			replies: []mzShardReply{
				{"a", []mzRows{{strPKs: []string{"x", "y"}, ts: int64s(1, 1), texts: strs("1", "2")}}},
				{"b", []mzRows{{strPKs: []string{"a"}, ts: int64s(1), texts: strs("3")}}},
			},
			wantStr: []string{"y", "a", "x"}, wantText: strs("2", "3", "1"), aligned: true,
		},
		{
			name:  "string pks from two shards",
			strPK: true,
			replies: []mzShardReply{
				{"a", []mzRows{{strPKs: []string{"x"}, ts: int64s(1), texts: strs("1")}}},
				{"b", []mzRows{{strPKs: []string{"x"}, ts: int64s(1), texts: strs("2")}}},
			},
			wantErr: merr.ErrDataIntegrity,
		},
		{
			name:      "requery of int64 pks over a string pk body",
			requested: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: int64s(1)}}},
			replies: []mzShardReply{
				{"a", []mzRows{{strPKs: []string{"x"}, ts: int64s(1), texts: strs("1")}}},
			},
			wantErr: merr.ErrServiceInternal,
		},
		{
			name:     "nullable column",
			nullable: true,
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(4, 2), ts: int64s(1, 1), texts: []*string{nil, sp("b")}}}},
				{"b", []mzRows{{intPKs: int64s(3, 1), ts: int64s(1, 1), texts: []*string{sp("c"), nil}}}},
			},
			wantInt: int64s(4, 2, 3, 1), wantText: []*string{nil, sp("b"), sp("c"), nil},
		},
		{
			name:      "nullable column, requery",
			nullable:  true,
			requested: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: int64s(1, 2, 4)}}},
			replies: []mzShardReply{
				{"a", []mzRows{{intPKs: int64s(2, 4), ts: int64s(1, 1), texts: []*string{sp("b"), nil}}}},
				{"b", []mzRows{{intPKs: int64s(1), ts: int64s(1), texts: []*string{nil}}}},
			},
			wantInt: int64s(1, 2, 4), wantText: []*string{nil, sp("b"), nil}, aligned: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withMaterializeSchemas(t, materializeSchemaCacheSize)
			f := newTestFill(mzSchemaOf(c.strPK, c.nullable), c.maxOut, c.requested)
			defer f.release()
			var addErr error
			for _, r := range c.replies {
				replies := make([]*internalpb.MaterializeResults, len(r.workers))
				for i, w := range r.workers {
					replies[i] = mzReplyOf(t, w, c.nullable)
				}
				if err := f.add(0, r.channel, relay(replies...)); err != nil && addErr == nil {
					addErr = err
				}
			}
			if c.wantErr != nil {
				if addErr == nil {
					_, addErr = f.finish()
				}
				require.ErrorIs(t, addErr, c.wantErr)
				return
			}
			require.NoError(t, addErr)
			got, err := f.finish()
			require.NoError(t, err)
			if c.requested != nil {
				// What requeryOperator checks to take organize's fast path.
				require.Equal(t, c.aligned, proto.Equal(got.GetIds(), c.requested))
			}
			if c.strPK {
				require.Equal(t, c.wantStr, got.GetIds().GetStrId().GetData())
			} else {
				require.Equal(t, c.wantInt, got.GetIds().GetIntId().GetData())
			}
			require.Equal(t, c.wantText, textOf(t, got))
			// The timestamp column travels with the rows.
			require.Equal(t, 1, int(got.GetFieldsData()[0].GetFieldId()))
			require.Len(t, got.GetFieldsData()[0].GetScalars().GetLongData().GetData(), len(c.wantText))
		})
	}
}

// maxOutputSize is checked per shard, as Query checks it on each delegator's
// reduce, on the rows kept: a row here is charged its timestamp (8) and its
// text (8), as the delegator's reduce charges it. Four shards each under the
// limit pass even though their sum is over it; two rows of one shard fail.
func TestMaterializeFill_MaxOutputSizePerShard(t *testing.T) {
	const limit = 24 // a row is 16
	f := newTestFill(mzSchema(), limit, nil)
	defer f.release()
	for s := 0; s < 4; s++ {
		err := f.add(0, strconv.Itoa(s), relay(mzWorkerReply(t, []int64{int64(s)}, []int64{1}, []string{"aaaaaaaa"})))
		require.NoError(t, err)
	}
	got, err := f.finish()
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3}, got.GetIds().GetIntId().GetData())

	f2 := newTestFill(mzSchema(), limit, nil)
	defer f2.release()
	err = f2.add(0, "x", relay(mzWorkerReply(t, []int64{8, 9}, []int64{1, 1}, []string{"aaaaaaaa", "bbbbbbbb"})))
	require.NoError(t, err)
	_, err = f2.finish()
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

// The limit applies to the deduplicated rows, not to the relayed bodies:
// three workers of one shard return the same pk, their raw bodies sum far
// over the limit, the one row kept is under it, and Query would accept it.
func TestMaterializeFill_MaxOutputSizeAfterDedup(t *testing.T) {
	const limit = 16 // the one row kept
	workers := []*internalpb.MaterializeResults{
		mzWorkerReply(t, []int64{1}, []int64{1}, []string{"aaaaaaaa"}),
		mzWorkerReply(t, []int64{1}, []int64{3}, []string{"cccccccc"}),
		mzWorkerReply(t, []int64{1}, []int64{2}, []string{"bbbbbbbb"}),
	}
	shard := relay(workers...)
	require.Greater(t, shard.GetHeader().GetBodyRawSize(), int64(limit))
	f := newTestFill(mzSchema(), limit, nil)
	defer f.release()
	err := f.add(0, "a", shard)
	require.NoError(t, err)
	got, err := f.finish()
	require.NoError(t, err)
	require.Equal(t, []int64{1}, got.GetIds().GetIntId().GetData())
	require.Equal(t, []string{"cccccccc"}, textCol(t, got))
}

// A requery's rows are charged once per deduplicated slot, as Query's reduce
// counts deduplicated rows: a pk requested twice counts once.
func TestMaterializeFill_MaxOutputSizeRequerySlots(t *testing.T) {
	const limit = 40 // a row is 16
	cases := []struct {
		name      string
		requested []int64
		wantErr   bool
	}{
		{name: "two positions", requested: []int64{1, 2}},
		{name: "a pk twice counts once", requested: []int64{1, 2, 1}},
		{name: "three pks over the limit", requested: []int64{1, 2, 3}, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newTestFill(mzSchema(), limit, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: c.requested}}})
			defer f.release()
			require.NoError(t, f.add(0, "a", relay(mzWorkerReply(t, []int64{1, 2, 3}, []int64{1, 1, 1}, []string{"aaaaaaaa", "bbbbbbbb", "cccccccc"}))))
			got, err := f.finish()
			if c.wantErr {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.requested, got.GetIds().GetIntId().GetData())
		})
	}
}

// Once the request falls back to Query, later replies are not read: a body
// that would fail to decode is released without an error.
func TestMaterializeFill_AddAfterFallback(t *testing.T) {
	f := newTestFill(mzSchema(), 0, nil)
	defer f.release()
	f.fallBack(fallbackUnimplemented, merr.WrapErrServiceUnimplemented(errors.New("old node")))
	r := relay(mzWorkerReply(t, []int64{1, 2}, []int64{1, 1}, []string{"x", "y"}))
	r.Body[0] = r.Body[0][:len(r.Body[0])/2]
	require.NoError(t, f.add(0, "a", r))
	require.Empty(t, f.batches)
	reason, err := f.failure()
	require.Equal(t, fallbackUnimplemented, reason)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
}

func TestMaterializeFill_Stats(t *testing.T) {
	f := newTestFill(mzSchema(), 0, nil)
	defer f.release()
	for _, ch := range []string{"a", "b"} {
		r := relay(mzWorkerReply(t, []int64{int64(len(ch))}, []int64{1}, []string{ch}))
		r.Header.AllRetrieveCount = 3
		r.Header.ScannedRemoteBytes = 5
		r.Header.ScannedTotalBytes = 7
		r.Header.CostAggregation = &internalpb.CostAggregation{TotalRelatedDataSize: 11}
		r.Header.FeatureBits = 1 << (ch[0] - 'a')
		if ch == "b" {
			r.Header.BodySchema = nil
			r.Body = nil
		}
		err := f.add(0, ch, r)
		require.NoError(t, err)
	}
	got, err := f.finish()
	require.NoError(t, err)
	require.EqualValues(t, 6, got.GetAllRetrieveCount())
	require.EqualValues(t, 10, got.GetScannedRemoteBytes())
	require.EqualValues(t, 14, got.GetScannedTotalBytes())
	require.EqualValues(t, 22, got.GetCostAggregation().GetTotalRelatedDataSize())
	require.EqualValues(t, 0b11, got.GetFeatureBits())
	require.Len(t, f.accepted, 2)
}

func TestMaterializeFill_Empty(t *testing.T) {
	for _, requested := range []*schemapb.IDs{nil, {IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}} {
		f := newTestFill(mzSchema(), 0, requested)
		err := f.add(0, "a", &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success()}})
		require.NoError(t, err)
		got, err := f.finish()
		require.NoError(t, err)
		require.Empty(t, got.GetFieldsData())
		require.Zero(t, typeutil.GetSizeOfIDs(got.GetIds()))
		f.release()
	}
}

// Shards whose replies carry different schemas are version skew: the
// request falls back to Query.
func TestMaterializeFill_SchemaSkew(t *testing.T) {
	f := newTestFill(mzSchema(), 0, nil)
	defer f.release()
	err := f.add(0, "a", relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"a"})))
	require.NoError(t, err)
	err = f.add(0, "b", relay(mzWorkerReplyWithPK(t, []int64{2}, []int64{1}, []string{"b"})))
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
}

// A reply arriving after the fill was released (a fallback or a failed
// fan-out) is dropped, its batches with it.
func TestMaterializeFill_AddAfterRelease(t *testing.T) {
	f := newTestFill(mzSchema(), 0, nil)
	f.release()
	err := f.add(0, "a", relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"a"})))
	require.NoError(t, err)
	require.Empty(t, f.batches)
}

// The pk can also be an output column; it is then one more user column.
func TestMaterializeFill_PKInOutput(t *testing.T) {
	f := newTestFill(mzSchema(), 0, nil)
	defer f.release()
	err := f.add(0, "a", relay(
		mzWorkerReplyWithPK(t, []int64{1, 3}, []int64{10, 30}, []string{"old", "c"}),
		mzWorkerReplyWithPK(t, []int64{1, 2}, []int64{20, 15}, []string{"new", "b"}),
	))
	require.NoError(t, err)
	got, err := f.finish()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 2}, got.GetIds().GetIntId().GetData())
	byID := make(map[int64]*schemapb.FieldData)
	for _, fd := range got.GetFieldsData() {
		byID[fd.GetFieldId()] = fd
	}
	require.Len(t, byID, 3)
	require.Equal(t, []int64{20, 30, 15}, byID[1].GetScalars().GetLongData().GetData())
	require.Equal(t, []int64{1, 3, 2}, byID[100].GetScalars().GetLongData().GetData())
	require.Equal(t, []string{"new", "c", "b"}, byID[101].GetScalars().GetStringData().GetData())
}

func TestMaterializeBodySchema(t *testing.T) {
	full := relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))
	parsed, err := materialize.ParseSchema(full.GetHeader().GetBodySchema())
	require.NoError(t, err)
	known := materializeSchemaRef{fingerprint: full.GetHeader().GetSchemaFingerprint(), schema: parsed}
	other := materializeSchemaRef{fingerprint: 7, schema: parsed}
	cases := []struct {
		name    string
		res     *internalpb.MaterializeResults
		known   materializeSchemaRef
		wantErr error
	}{
		{"bytes in the reply", relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})), materializeSchemaRef{}, nil},
		{"bytes in the reply, other schema known", relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})), other, nil},
		{"bytes in the reply, same schema known", relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})), known, nil},
		{"omitted, known schema", stripSchema(relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))), known, nil},
		{"omitted, nothing known", stripSchema(relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))), materializeSchemaRef{}, merr.ErrServiceInternal},
		{"omitted, another schema known", stripSchema(relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))), other, merr.ErrServiceInternal},
		{"bytes do not match the fingerprint", func() *internalpb.MaterializeResults {
			r := relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))
			r.Header.SchemaFingerprint++
			return r
		}(), materializeSchemaRef{}, merr.ErrServiceInternal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withMaterializeSchemas(t, materializeSchemaCacheSize)
			s, err := materializeBodySchema(c.res, c.known)
			if c.wantErr != nil {
				require.ErrorIs(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, parsed, s)
		})
	}
}

// Replies decoded concurrently, as the load balancer's callbacks do, give the
// same rows as one after another.
func TestMaterializeFill_Concurrent(t *testing.T) {
	const shards = 8
	f := newTestFill(mzSchema(), 0, nil)
	defer f.release()
	replies := make([]*internalpb.MaterializeResults, shards)
	for s := range replies {
		replies[s] = relay(
			mzWorkerReply(t, []int64{int64(s*10 + 1), int64(s*10 + 2)}, []int64{1, 1}, []string{"a", "b"}),
			mzWorkerReply(t, []int64{int64(s*10 + 2), int64(s*10 + 3)}, []int64{2, 1}, []string{"B", "c"}),
		)
	}
	var wg sync.WaitGroup
	for s := range replies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := f.add(0, strconv.Itoa(s), replies[s])
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	got, err := f.finish()
	require.NoError(t, err)
	texts := make(map[int64]string)
	ids := got.GetIds().GetIntId().GetData()
	for i, s := range textCol(t, got) {
		texts[ids[i]] = s
	}
	require.Len(t, texts, 3*shards)
	for s := 0; s < shards; s++ {
		require.Equal(t, "a", texts[int64(s*10+1)])
		require.Equal(t, "B", texts[int64(s*10+2)])
		require.Equal(t, "c", texts[int64(s*10+3)])
	}
}

// receivedOver is r as the proxy's codec delivers it from gRPC: unmarshaled
// from pbFrames buffers of chunk bytes, which are then freed, as gRPC does.
func receivedOver(t *testing.T, r *internalpb.MaterializeResults, chunk int) *internalpb.MaterializeResults {
	t.Helper()
	wire, err := proto.Marshal(r)
	require.NoError(t, err)
	var in mem.BufferSlice
	for b := wire; len(b) > 0; {
		n := min(chunk, len(b))
		pb := pbFrames.Get(n)
		copy(*pb, b[:n])
		in = append(in, mem.NewBuffer(pb, &pbFrames))
		b = b[n:]
	}
	out := &internalpb.MaterializeResults{}
	require.NoError(t, encoding.GetCodecV2("proto").Unmarshal(in, out))
	in.Free()
	return out
}

// The fill reads replies straight from the received buffers -- entries cut
// anywhere, rows straddling buffers, empty replies -- with the same result
// as from contiguous entries; the buffers stay referenced until release and
// are poisoned once freed, so a value still read from them would differ.
func TestMaterializeFill_ReceivedBodies(t *testing.T) {
	pbPoison.Store(true)
	defer pbPoison.Store(false)
	text := func(i int) *string {
		if i%4 == 0 {
			return nil
		}
		return sp(strings.Repeat(string(rune('a'+i%26)), i%37))
	}
	shard := func(base int, strPK bool) []*internalpb.MaterializeResults {
		var out []*internalpb.MaterializeResults
		for w := 0; w < 3; w++ {
			var rows mzRows
			for i := 0; i < 60; i++ {
				pk := int64(base + w*100 + i)
				if strPK {
					rows.strPKs = append(rows.strPKs, fmt.Sprintf("pk%06d", pk))
				} else {
					rows.intPKs = append(rows.intPKs, pk)
				}
				rows.ts = append(rows.ts, 1)
				rows.texts = append(rows.texts, text(i+w))
			}
			out = append(out, mzReplyOf(t, rows, true))
		}
		return out
	}
	for _, strPK := range []bool{false, true} {
		for _, chunk := range []int{1, 7, 100, 16 << 10} {
			t.Run(fmt.Sprintf("strPK=%v/chunk=%d", strPK, chunk), func(t *testing.T) {
				before := pbFrames.outstanding()
				shards := [][]*internalpb.MaterializeResults{shard(0, strPK), shard(1000, strPK)}
				run := func(received bool) *internalpb.RetrieveResults {
					f := newTestFill(mzSchemaOf(strPK, true), 0, nil)
					defer f.release()
					for s, workers := range shards {
						r := relay(workers...)
						if received {
							r = receivedOver(t, r, chunk)
						}
						err := f.add(0, strconv.Itoa(s), r)
						require.NoError(t, err)
					}
					// An empty reply, as from a shard with no match.
					empty := &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success()}}
					if received {
						empty = receivedOver(t, empty, chunk)
					}
					err := f.add(0, "empty", empty)
					require.NoError(t, err)
					if received {
						require.Greater(t, pbFrames.outstanding(), before, "the fill holds the received buffers")
					}
					got, err := f.finish()
					require.NoError(t, err)
					return got
				}
				want := run(false)
				got := run(true)
				require.Zero(t, pbFrames.outstanding()-before, "release frees every received buffer")
				require.True(t, proto.Equal(want, got))
				require.Len(t, textOf(t, got), 360)
			})
		}
	}
}

// A fallback (Unimplemented) replaces any other failure, so one old node
// makes the request rerun with Query, and the first fallback keeps its
// reason; otherwise the first failure wins.
func TestMaterializeFill_FailPrecedence(t *testing.T) {
	unimpl := merr.WrapErrServiceUnimplemented(errors.New("old node"))
	unimpl2 := merr.WrapErrServiceUnimplemented(errors.New("second"))
	internal := merr.WrapErrServiceInternalMsg("corrupt body")
	integrity := merr.WrapErrDataIntegrityMsg("duplicate pk")
	type rec struct {
		err    error
		reason string // "" for fail
	}
	cases := []struct {
		name       string
		recs       []rec
		want       error
		wantReason string
	}{
		{"none", nil, nil, ""},
		{"first error wins", []rec{{internal, ""}, {integrity, ""}}, internal, ""},
		{"fallback after another error", []rec{{internal, ""}, {unimpl, fallbackUnimplemented}}, unimpl, fallbackUnimplemented},
		{"fallback before another error", []rec{{unimpl, fallbackUnsupported}, {integrity, ""}}, unimpl, fallbackUnsupported},
		{"first fallback wins", []rec{{unimpl, fallbackSchemaSkew}, {unimpl2, fallbackUnimplemented}}, unimpl, fallbackSchemaSkew},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newTestFill(mzSchema(), 0, nil)
			for _, r := range c.recs {
				if r.reason == "" {
					f.fail(r.err)
				} else {
					f.fallBack(r.reason, r.err)
				}
			}
			reason, err := f.failure()
			require.Equal(t, c.want, err)
			require.Equal(t, c.wantReason, reason)
		})
	}
}

// newTestFill is newMaterializeFill for schema, naming no schema and caching
// none.
func newTestFill(schema *schemapb.CollectionSchema, maxOutputSize int64, requested *schemapb.IDs) *materializeFill {
	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		panic(err)
	}
	return newMaterializeFill(helper, maxOutputSize, requested, materializeSchemaRef{}, nil)
}
