package dql

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// withMaterializeSchemas swaps the process-wide cache for a fresh one.
func withMaterializeSchemas(t *testing.T, capacity int) *materializeSchemaCache {
	old := materializeSchemas
	materializeSchemas = newMaterializeSchemaCache(capacity)
	t.Cleanup(func() { materializeSchemas = old })
	return materializeSchemas
}

func TestMaterializeSchemaKey(t *testing.T) {
	k := newMaterializeSchemaKey(1, 2, []int64{100, 101})
	require.Equal(t, k, newMaterializeSchemaKey(1, 2, []int64{100, 101}))
	require.NotEqual(t, k, newMaterializeSchemaKey(1, 2, []int64{101, 100}))
	require.NotEqual(t, k, newMaterializeSchemaKey(1, 3, []int64{100, 101}))
	require.NotEqual(t, k, newMaterializeSchemaKey(2, 2, []int64{100, 101}))
	require.NotEqual(t, newMaterializeSchemaKey(1, 2, []int64{1, 23}), newMaterializeSchemaKey(1, 2, []int64{12, 3}))
}

// parsedSchema returns a distinct parsed batch schema per i, and its
// message: its user column is named after i.
func parsedSchema(t *testing.T, i int64) (*materialize.Schema, []byte) {
	t.Helper()
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"}, []string{"1," + strconv.FormatInt(i, 10), ""})
	us := arrow.NewSchema([]arrow.Field{{Name: strconv.FormatInt(i, 10), Type: arrow.BinaryTypes.String}}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, us)
	b.Field(0).(*array.StringBuilder).Append("x")
	u := b.NewRecord()
	b.Release()
	defer u.Release()
	header := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		FieldsData: []*schemapb.FieldData{{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}},
		}}},
	}
	rec, err := materialize.BuildBatch(header, &queryutil.ArrowSelection{Records: []arrow.Record{u}, Rows: queryutil.RowRefsForTest([][2]int64{{0, 0}})})
	require.NoError(t, err)
	defer rec.Release()
	msg, _, err := materialize.EncodeBatch(rec, materialize.CodecNone)
	require.NoError(t, err)
	s, err := materialize.ParseSchema(msg)
	require.NoError(t, err)
	return s, msg
}

func TestMaterializeSchemaCache_LRU(t *testing.T) {
	c := newMaterializeSchemaCache(2)
	key := func(i int64) materializeSchemaKey { return newMaterializeSchemaKey(i, 0, nil) }
	refs := make(map[int64]materializeSchemaRef)
	ref := func(i int64) materializeSchemaRef {
		if r, ok := refs[i]; ok {
			return r
		}
		s, msg := parsedSchema(t, i)
		refs[i] = materializeSchemaRef{fingerprint: materialize.SchemaFingerprint(msg), schema: s}
		return refs[i]
	}
	require.Equal(t, materializeSchemaRef{}, c.resolve(key(1)))
	c.store(key(1), ref(1).fingerprint, ref(1).schema)
	c.store(key(2), ref(2).fingerprint, ref(2).schema)
	require.Equal(t, ref(1), c.resolve(key(1)))        // 1 is now the most recent
	c.store(key(3), ref(3).fingerprint, ref(3).schema) // evicts 2
	require.Equal(t, materializeSchemaRef{}, c.resolve(key(2)))
	require.Equal(t, ref(1), c.resolve(key(1)))
	require.Equal(t, ref(3), c.resolve(key(3)))
	require.Equal(t, 2, c.byKey.Len())
	require.Equal(t, 2, c.byFingerprint.Len())

	// A key whose schema bytes were evicted is a miss.
	c.byFingerprint.Remove(ref(1).fingerprint)
	require.Equal(t, materializeSchemaRef{}, c.resolve(key(1)))
}

// The first request names no schema and caches the one the reply carries;
// the next names it, and its reply without schema bytes still decodes.
func TestQueryTask_MaterializeShard_SchemaCache(t *testing.T) {
	paramtable.Init()
	cache := withMaterializeSchemas(t, materializeSchemaCacheSize)
	full := relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))
	fp := full.GetHeader().GetSchemaFingerprint()

	var sent []uint64
	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Materialize(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (*internalpb.MaterializeResults, error) {
			sent = append(sent, req.GetMaterializeSchemaFingerprint())
			r := relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))
			if req.GetMaterializeSchemaFingerprint() == r.GetHeader().GetSchemaFingerprint() {
				r.Header.BodySchema = nil
			}
			return r, nil
		})

	newTask := func() *QueryTask {
		task := newMaterializeTestTask(t)
		task.CollectionID = 7
		task.OutputFieldsId = []int64{100, 101}
		return task
	}
	key := newMaterializeSchemaKey(7, 0, []int64{100, 101})

	require.NoError(t, materializeAndFinish(newTask(), qn))
	ref := cache.resolve(key)
	require.Equal(t, fp, ref.fingerprint)
	wantSchema, err := materialize.ParseSchema(full.GetHeader().GetBodySchema())
	require.NoError(t, err)
	require.Equal(t, wantSchema, ref.schema)

	task := newTask()
	require.NoError(t, materializeAndFinish(task, qn))
	require.Equal(t, []uint64{0, fp}, sent)
	var got []*internalpb.RetrieveResults
	task.resultBuf.Range(func(r *internalpb.RetrieveResults) bool { got = append(got, r); return true })
	require.Len(t, got, 1)
	require.Equal(t, []string{"x"}, textCol(t, got[0]))
}

func TestQueryTask_MaterializeShard_SchemaErrors(t *testing.T) {
	paramtable.Init()
	cases := []struct {
		name  string
		reply func() *internalpb.MaterializeResults
	}{
		{"omitted schema the request did not name", func() *internalpb.MaterializeResults {
			return stripSchema(relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"})))
		}},
		{"schema bytes do not match the fingerprint", func() *internalpb.MaterializeResults {
			r := relay(mzWorkerReply(t, []int64{1}, []int64{1}, []string{"x"}))
			r.Header.SchemaFingerprint++
			return r
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cache := withMaterializeSchemas(t, materializeSchemaCacheSize)
			qn := mocks.NewMockQueryNodeClient(t)
			qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(c.reply(), nil)
			task := newMaterializeTestTask(t)
			task.OutputFieldsId = []int64{100, 101}
			err := materializeAndFinish(task, qn)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, 0, cache.byKey.Len())
			require.Equal(t, 0, cache.byFingerprint.Len())
		})
	}
}

// A reply with an empty body is not decoded, so its schema bytes are never
// checked by the reduce; mismatching bytes must still not reach the cache.
func TestQueryTask_MaterializeShard_EmptyBodyBadSchemaNotCached(t *testing.T) {
	paramtable.Init()
	cache := withMaterializeSchemas(t, materializeSchemaCacheSize)
	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Materialize(mock.Anything, mock.Anything).Return(&internalpb.MaterializeResults{
		Header: &internalpb.MaterializeHeader{Status: merr.Success(), BodySchema: []byte("garbage"), SchemaFingerprint: 1},
	}, nil)
	task := newMaterializeTestTask(t)
	task.OutputFieldsId = []int64{100, 101}
	require.NoError(t, materializeAndFinish(task, qn))
	require.Equal(t, 0, cache.byKey.Len())
	require.Equal(t, 0, cache.byFingerprint.Len())
}
