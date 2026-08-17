package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestLoadResourceReservationReleaseIsIdempotent(t *testing.T) {
	paramtable.Init()
	loader := &segmentLoader{
		committedResource:         LoadResource{MemorySize: 10, DiskSize: 20},
		committedResourceNotifier: syncutil.NewVersionedNotifier(),
	}
	reservation := &loadResourceReservation{
		loader: loader,
		result: requestResourceResult{Resource: LoadResource{MemorySize: 3, DiskSize: 4}},
	}

	reservation.Release()
	reservation.Release()

	assert.Equal(t, LoadResource{MemorySize: 7, DiskSize: 16}, loader.committedResource)
}
