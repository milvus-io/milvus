package materialize

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func userRecord(t *testing.T, vals []string) arrow.Record {
	t.Helper()
	md := arrow.NewMetadata(
		[]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{"1,101", ""})
	schema := arrow.NewSchema([]arrow.Field{{Name: "101", Type: arrow.BinaryTypes.String}}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, v := range vals {
		b.Field(0).(*array.StringBuilder).Append(v)
	}
	return b.NewRecord()
}

func tsField(ts ...int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: 1, Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ts}},
		}},
	}
}

func intIDs(ids ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

// readBatches parses schemaMsg and reads every entry as one piece.
func readBatches(t *testing.T, schemaMsg []byte, entries ...[]byte) (*Schema, []*Batch) {
	t.Helper()
	s, err := ParseSchema(schemaMsg)
	require.NoError(t, err)
	var out []*Batch
	for i, e := range entries {
		b, err := s.ReadEntry([][]byte{e}, i, 0)
		require.NoError(t, err)
		out = append(out, b)
	}
	return s, out
}

func textSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
	}}
}

func TestBuildRead_RoundTrip(t *testing.T) {
	u := userRecord(t, []string{"a", "b", "c"})
	defer u.Release()
	sel := &queryutil.ArrowSelection{Records: []arrow.Record{u}}
	sel.Rows = queryutil.RowRefsForTest([][2]int64{{0, 2}, {0, 0}})
	header := &segcorepb.RetrieveResults{Ids: intIDs(30, 10), FieldsData: []*schemapb.FieldData{tsField(300, 100)}}

	rec, err := BuildBatch(header, sel)
	require.NoError(t, err)
	defer rec.Release()
	require.Equal(t, int64(2), rec.NumRows())
	require.Equal(t, "$pk", rec.ColumnName(0))
	require.Equal(t, "$sys:1", rec.ColumnName(1))

	for _, codec := range []Codec{CodecNone, CodecZstd} {
		t.Run(string(codec), func(t *testing.T) {
			schemaMsg, entry, err := EncodeBatch(rec, codec)
			require.NoError(t, err)
			s, batches := readBatches(t, schemaMsg, entry, entry)
			require.Equal(t, []int64{1}, s.sysIDs)
			require.False(t, s.StringPK())
			for _, b := range batches {
				require.Equal(t, 2, b.Rows)
				require.Equal(t, []int64{30, 10}, b.IntPKs)
				require.Equal(t, []int64{300, 100}, b.Timestamps(s))
				require.Len(t, b.cols, 1)
			}
			ids, fields, err := s.Gather(batches, []RowRef{{1, 1}, {0, 0}}, fieldsOf(t, textSchema()))
			require.NoError(t, err)
			require.Equal(t, []int64{10, 30}, ids.GetIntId().GetData())
			require.Len(t, fields, 2) // field order "1,101": the timestamp, the text
			require.Equal(t, int64(1), fields[0].GetFieldId())
			require.Equal(t, []int64{100, 300}, fields[0].GetScalars().GetLongData().GetData())
			require.Equal(t, int64(101), fields[1].GetFieldId())
			require.Equal(t, []string{"a", "c"}, fields[1].GetScalars().GetStringData().GetData())
		})
	}
}

func TestBuildBatch_StringPK(t *testing.T) {
	u := userRecord(t, []string{"x"})
	defer u.Release()
	sel := &queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest([][2]int64{{0, 0}})}
	header := &segcorepb.RetrieveResults{
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"k1"}}}},
		FieldsData: []*schemapb.FieldData{tsField(5)},
	}
	rec, err := BuildBatch(header, sel)
	require.NoError(t, err)
	defer rec.Release()
	schemaMsg, entry, err := EncodeBatch(rec, CodecNone)
	require.NoError(t, err)
	s, batches := readBatches(t, schemaMsg, entry)
	require.True(t, s.StringPK())
	require.Equal(t, []string{"k1"}, batches[0].StrPKs)
	ids, _, err := s.Gather(batches, []RowRef{{0, 0}}, fieldsOf(t, textSchema()))
	require.NoError(t, err)
	require.Equal(t, []string{"k1"}, ids.GetStrId().GetData())
}

func TestBuildBatch_Empty(t *testing.T) {
	rec, err := BuildBatch(&segcorepb.RetrieveResults{}, nil)
	require.NoError(t, err)
	require.Nil(t, rec)
}

func TestBuildBatch_RejectsUserColumnInHeader(t *testing.T) {
	u := userRecord(t, []string{"x"})
	defer u.Release()
	sel := &queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest([][2]int64{{0, 0}})}
	arrayCol := &schemapb.FieldData{FieldId: 102, Type: schemapb.DataType_Array}
	header := &segcorepb.RetrieveResults{Ids: intIDs(1), FieldsData: []*schemapb.FieldData{tsField(5), arrayCol}}
	_, err := BuildBatch(header, sel)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
}

func TestBuildBatch_RowsWithoutSelection(t *testing.T) {
	header := &segcorepb.RetrieveResults{Ids: intIDs(1), FieldsData: []*schemapb.FieldData{tsField(5)}}
	_, err := BuildBatch(header, nil)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

// A batch has no room for ElementIndices, so an element-level result must
// fail closed and send the request back to Query.
func TestBuildBatch_RejectsElementLevel(t *testing.T) {
	cases := []struct {
		name   string
		header *segcorepb.RetrieveResults
	}{
		{"element level flag", &segcorepb.RetrieveResults{Ids: intIDs(1), ElementLevel: true}},
		{"element indices", &segcorepb.RetrieveResults{
			Ids:            intIDs(1),
			ElementIndices: []*segcorepb.ElementIndices{{Indices: []int32{0}}},
		}},
		{"element level without rows", &segcorepb.RetrieveResults{ElementLevel: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := userRecord(t, []string{"x"})
			defer u.Release()
			sel := &queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest([][2]int64{{0, 0}})}
			c.header.FieldsData = []*schemapb.FieldData{tsField(5)}
			rec, err := BuildBatch(c.header, sel)
			require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
			require.Nil(t, rec)
			// The class survives the status the worker replies with.
			require.ErrorIs(t, merr.Error(merr.Status(err)), merr.ErrServiceUnimplemented)
		})
	}
}

// schemaMsgOf encodes the Schema message of a batch with these columns.
func schemaMsgOf(t *testing.T, fields ...arrow.Field) []byte {
	t.Helper()
	cols := make([]arrow.Array, len(fields))
	for i, f := range fields {
		cols[i] = array.MakeArrayOfNull(memory.DefaultAllocator, f.Type, 1)
		defer cols[i].Release()
	}
	rec := array.NewRecord(arrow.NewSchema(fields, nil), cols, 1)
	defer rec.Release()
	s, _, err := EncodeBatch(rec, CodecNone)
	require.NoError(t, err)
	return s
}

func TestParseSchema_Rejects(t *testing.T) {
	u := userRecord(t, []string{"a"})
	defer u.Release()
	plain, _, err := EncodeBatch(u, CodecNone)
	require.NoError(t, err)
	i64 := arrow.PrimitiveTypes.Int64
	pk := arrow.Field{Name: pkColumnName, Type: i64}
	cases := []struct {
		name string
		msg  []byte
	}{
		{"no pk column", plain},
		{"garbage", []byte("garbage")},
		{"corrupt metadata", append([]byte{0xff, 0xff, 0xff, 0xff, 8, 0, 0, 0}, 1, 2, 3, 4, 5, 6, 7, 8)},
		{"bad system column name", schemaMsgOf(t, pk, arrow.Field{Name: sysColumnName + "x", Type: i64})},
		{"system column not int64", schemaMsgOf(t, pk, arrow.Field{Name: sysColumnName + "1", Type: arrow.BinaryTypes.String})},
		// The fused writer and the reader share colLayoutOf: a type the
		// export never produces is read by neither.
		{"uint8 column", schemaMsgOf(t, pk, arrow.Field{Name: "u", Type: arrow.PrimitiveTypes.Uint8})},
		{"float16 column", schemaMsgOf(t, pk, arrow.Field{Name: "h", Type: arrow.FixedWidthTypes.Float16})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseSchema(c.msg)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

// The system columns are the $sys:<id> columns right after the pk; the
// timestamp is the one with common.TimeStampField's id.
func TestParseSchema_SystemColumns(t *testing.T) {
	i64 := arrow.PrimitiveTypes.Int64
	s, err := ParseSchema(schemaMsgOf(t,
		arrow.Field{Name: pkColumnName, Type: arrow.BinaryTypes.String},
		arrow.Field{Name: sysColumnName + "0", Type: i64},
		arrow.Field{Name: sysColumnName + "1", Type: i64},
		arrow.Field{Name: "100", Type: i64}))
	require.NoError(t, err)
	require.True(t, s.StringPK())
	require.Equal(t, []int64{0, 1}, s.sysIDs)
	require.Equal(t, 1, s.tsCol)
	require.Equal(t, 3, s.lead())

	s, err = ParseSchema(schemaMsgOf(t, arrow.Field{Name: pkColumnName, Type: i64}, arrow.Field{Name: "100", Type: i64}))
	require.NoError(t, err)
	require.Empty(t, s.sysIDs)
	require.Equal(t, -1, s.tsCol)
}
