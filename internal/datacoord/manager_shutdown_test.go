package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/gc"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestStopClosesPartiallyInitializedManagers(t *testing.T) {
	paramtable.Init()
	for _, state := range []commonpb.StateCode{commonpb.StateCode_Initializing, commonpb.StateCode_Abnormal} {
		t.Run(state.String(), func(t *testing.T) {
			collector := gc.NewGarbageCollector(nil, nil, gc.GcOption{})
			exports := snapshot.NewSnapshotExportManager(context.Background(), nil, nil)
			s := &Server{garbageCollector: collector, snapshotExportManager: exports}
			s.stateCode.Store(state)
			require.NoError(t, s.Stop())
			require.ErrorIs(t, collector.ContextForTest().Err(), context.Canceled)
			require.ErrorIs(t, exports.ContextForTest().Err(), context.Canceled)
			require.NoError(t, s.Stop())
		})
	}
}
