package idf

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// A text-only WAL insert must use the same resource-backed runner both in live
// consumption and recovery. No legacy global file inventory is installed here.
func TestOracleOwnsRemoteSynonymResources(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, analyzer.InitOptions())
	cm := storage.NewLocalChunkManager()
	file := filepath.Join(root, "synonyms.txt")
	require.NoError(t, cm.Write(t.Context(), file, []byte("search, retrieval\n")))
	var fetches atomic.Int32
	fileresource.SetResourceFetcher(cm, func(_ context.Context, ids []int64, names []string) ([]*internalpb.FileResourceInfo, error) {
		fetches.Add(1)
		return []*internalpb.FileResourceInfo{{Id: 88001, Name: "idf_synonyms", Path: file}}, nil
	})
	defer fileresource.SetResourceFetcher(nil, nil)
	schema := &schemapb.CollectionSchema{Version: 1, FileResourceIds: []int64{88001}, Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
			{Key: "enable_analyzer", Value: "true"},
			{Key: "analyzer_params", Value: `{"tokenizer":"standard","filter":[{"type":"synonym","expand":true,"synonyms_file":{"type":"remote","resource_name":"idf_synonyms","file_name":"synonyms.txt"}}]}`},
		}},
		{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
	}, Functions: []*schemapb.FunctionSchema{{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}}}
	view := walview.VChannelWALView{CollectionID: 88001, Schema: schema, RequiredFields: testRequiredFields(schema)}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := newOracleRuntime(canceled, nil, view, nil, true)
	require.ErrorIs(t, err, context.Canceled)
	_, registered, err := function.GetManager().TryMaterialize(t.Context(), view.CollectionID, schema.GetVersion(), &msgpb.InsertRequest{})
	require.NoError(t, err)
	require.False(t, registered, "failed preparation must release its schema registration")
	require.Zero(t, fetches.Load())

	first, err := newOracleRuntime(t.Context(), nil, view, nil, true)
	require.NoError(t, err)
	defer first.Close()
	require.Same(t, schema, first.schema)
	require.Positive(t, fetches.Load(), "prepare resources even before the first insert")
	second, err := newOracleRuntime(t.Context(), nil, view, nil, true)
	require.NoError(t, err)
	defer second.Close()
	first.Close()
	available, err := function.GetManager().RunWithRunner(t.Context(), view.CollectionID, first.functionKey, 102, func(function.FunctionRunner) error {
		t.Error("closed oracle must release its runner registration")
		return nil
	})
	require.Error(t, err)
	require.False(t, available)

	original := message.MustAsImmutableInsertMessageV1(bm25Insert(t, 20, 2))
	body := proto.Clone(original.MustBody()).(*msgpb.InsertRequest)
	body.FieldsData = body.FieldsData[:1]
	body.FieldsData[0].GetScalars().GetStringData().Data = []string{"search"}
	raw := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(original.Header()).WithBody(body).
		MustBuildMutable().WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(10))
	second.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: raw})
	stats := second.growingStore.segments[20].stats[102]
	require.EqualValues(t, 1, stats.NumRow())
	require.EqualValues(t, 2, stats.GetAvgdl(), "synonym expansion contributes both tokens")
	require.Len(t, message.MustAsImmutableInsertMessageV1(raw).MustBody().GetFieldsData(), 1)
	second.Close()

	view.SegmentSnapshot.Segments = []walview.VisibleSegment{{SegmentID: 20, PartitionID: 10, Schema: schema, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{raw}}}}
	recovered, err := newOracleRuntime(t.Context(), &Provider{}, view, nil, true)
	require.NoError(t, err)
	defer recovered.Close()
	require.EqualValues(t, 1, recovered.growingStore.segments[20].stats[102].NumRow())
	require.EqualValues(t, 2, recovered.growingStore.segments[20].stats[102].GetAvgdl())
}
