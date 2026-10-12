package segment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestL1PublicationClassifiesWALOwnership(t *testing.T) {
	for _, tc := range []struct {
		name              string
		err               error
		ignored, terminal bool
	}{
		{name: "channel retired", err: merr.WrapErrChannelNotFound("v1"), ignored: true},
		{name: "owner changed", err: merr.WrapErrChannelMisrouted("v1"), terminal: true},
		{name: "assignment unavailable", err: merr.WrapErrChannelNotAvailable("v1")},
		{name: "lookup timed out", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coord := &coordStub{}
			patch := mockey.Mock((*coordStub).SaveBinlogPaths).Return(merr.Status(tc.err), nil).Build()
			defer patch.UnPatch()
			writer := NewSegmentLifecycleWriter(coord, 1)
			meta := newCommitL1SegmentTestMeta()
			_, commitErr := writer.CommitL1Segment(context.Background(), meta)
			persistErr := writer.PersistGrowingSegment(context.Background(), meta, nil, nil)
			for _, err := range []error{commitErr, persistErr} {
				if tc.ignored {
					require.NoError(t, err)
					continue
				}
				require.Error(t, err)
				if tc.terminal {
					require.True(t, errors.Is(err, merr.ErrChannelMisrouted))
				} else {
					require.Equal(t, merr.Code(tc.err), merr.Code(err))
				}
				require.Equal(t, !tc.terminal, retry.IsRecoverable(err))
			}
		})
	}
}
