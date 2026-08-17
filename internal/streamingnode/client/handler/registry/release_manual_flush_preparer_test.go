package registry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeReleaseManualFlushPreparer struct{}

func (fakeReleaseManualFlushPreparer) PrepareReleaseManualFlush(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string, releaseSegmentIDs []int64) error {
	return nil
}

func (fakeReleaseManualFlushPreparer) PrepareReleaseSegments(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string, segmentIDs []int64) (bool, error) {
	return false, nil
}

// TestLocalReleaseManualFlushPreparerRegistry covers the registry lifecycle in
// one sequential test because the underlying future is process-global:
// not-registered, registered, and double-registration must be observed in this
// order on a state this test owns via ResetRegisterLocalWALManager.
func TestLocalReleaseManualFlushPreparerRegistry(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)
	ResetRegisterLocalWALManager()
	// Leave clean state for other tests in the package; they register whatever
	// they need themselves.
	t.Cleanup(ResetRegisterLocalWALManager)

	// Before registration: a typed sentinel, not a block. The caller
	// (PrepareReleaseManualFlushIfLocal) relies on getting an error here
	// instead of waiting on the future.
	preparer, err := GetLocalReleaseManualFlushPreparer()
	assert.Nil(t, preparer)
	assert.ErrorIs(t, err, ErrNoReleaseManualFlushPreparer)

	// After registration the exact registered instance is returned.
	registered := fakeReleaseManualFlushPreparer{}
	RegisterLocalReleaseManualFlushPreparer(registered)
	preparer, err = GetLocalReleaseManualFlushPreparer()
	assert.NoError(t, err)
	assert.Equal(t, registered, preparer)

	// Double registration panics (syncutil.Future can only be set once); the
	// streaming node must register exactly once per process.
	assert.Panics(t, func() {
		RegisterLocalReleaseManualFlushPreparer(registered)
	})
}
