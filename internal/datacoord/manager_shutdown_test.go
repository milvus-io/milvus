package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestStopClosesPartiallyInitializedManagers(t *testing.T) {
	paramtable.Init()
	for _, state := range []commonpb.StateCode{commonpb.StateCode_Initializing, commonpb.StateCode_Abnormal} {
		t.Run(state.String(), func(t *testing.T) {
			gc := newGarbageCollector(nil, nil, GcOption{})
			exports := newSnapshotExportManager(context.Background(), nil, nil)
			s := &Server{garbageCollector: gc, snapshotExportManager: exports}
			s.stateCode.Store(state)
			require.NoError(t, s.Stop())
			require.ErrorIs(t, gc.ctx.Err(), context.Canceled)
			require.ErrorIs(t, exports.ctx.Err(), context.Canceled)
			require.NoError(t, s.Stop())
		})
	}
}
