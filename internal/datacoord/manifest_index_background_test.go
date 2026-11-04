package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
)

func TestStartManifestIndexRecovery(t *testing.T) {
	t.Run("wait returns the recovery result", func(t *testing.T) {
		injected := errors.New("injected")
		mocker := mockey.Mock((*meta.Meta).ReloadSegmentIndexesFromManifests).Return(injected).Build()
		defer mocker.UnPatch()

		recovery := startManifestIndexRecovery(context.Background(), &meta.Meta{})
		require.ErrorIs(t, recovery.wait(), injected)
		recovery.abort()
		require.ErrorIs(t, recovery.wait(), injected)
	})

	t.Run("abort cancels a running recovery and joins it", func(t *testing.T) {
		started := make(chan struct{})
		stopped := false
		mocker := mockey.Mock((*meta.Meta).ReloadSegmentIndexesFromManifests).To(func(_ *meta.Meta, ctx context.Context) error {
			close(started)
			<-ctx.Done()
			stopped = true
			return ctx.Err()
		}).Build()
		defer mocker.UnPatch()

		recovery := startManifestIndexRecovery(context.Background(), &meta.Meta{})
		<-started
		recovery.abort()
		require.True(t, stopped)
		require.ErrorIs(t, recovery.wait(), context.Canceled)
	})
}
