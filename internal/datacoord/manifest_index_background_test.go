package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStartManifestIndexRecovery(t *testing.T) {
	t.Run("wait returns the recovery result", func(t *testing.T) {
		injected := errors.New("injected")
		mocker := mockey.Mock((*meta).reloadSegmentIndexesFromManifests).Return(injected).Build()
		defer mocker.UnPatch()

		recovery := startManifestIndexRecovery(context.Background(), &meta{})
		require.ErrorIs(t, recovery.wait(), injected)
		recovery.abort()
		require.ErrorIs(t, recovery.wait(), injected)
	})

	t.Run("abort cancels a running recovery and joins it", func(t *testing.T) {
		started := make(chan struct{})
		stopped := false
		mocker := mockey.Mock((*meta).reloadSegmentIndexesFromManifests).To(func(_ *meta, ctx context.Context) error {
			close(started)
			<-ctx.Done()
			stopped = true
			return ctx.Err()
		}).Build()
		defer mocker.UnPatch()

		recovery := startManifestIndexRecovery(context.Background(), &meta{})
		<-started
		recovery.abort()
		require.True(t, stopped)
		require.ErrorIs(t, recovery.wait(), context.Canceled)
	})
}
