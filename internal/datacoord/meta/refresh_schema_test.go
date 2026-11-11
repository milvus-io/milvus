package meta

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRefreshBuilderSchemaGate(t *testing.T) {
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 2, SchemaVersion: 1}}
	schema := &schemapb.CollectionSchema{Version: 2, Fields: []*schemapb.FieldSchema{{ExternalField: "id"}}}
	target := &broadcaster.SchemaTarget{RefreshJobID: 3}
	var targetErr error
	patch := mockey.Mock(broadcast.GetPendingSchemaTarget).To(func(int64) (*broadcaster.SchemaTarget, error) { return target, targetErr }).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, CheckRefreshSchema(segment, schema), merr.ErrServiceNotReady)
	segment.SchemaVersion = 2
	require.NoError(t, CheckRefreshSchema(segment, schema))
	segment.SchemaVersion = 1
	target = nil
	require.NoError(t, CheckRefreshSchema(segment, schema))
	targetErr = merr.ErrServiceNotReady
	require.ErrorIs(t, CheckRefreshSchema(segment, schema), targetErr)
	schema.Fields = nil
	require.NoError(t, CheckRefreshSchema(segment, schema))
}
