package indexcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobalMetaBroker(t *testing.T) {
	ctx := context.Background()

	t.Run("getFieldSchemaByID fail", func(t *testing.T) {
		rootCoord := &RootCoordMock{
			Fail: true,
			Err:  false,
		}
		broker, err := newGlobalMetaBroker(ctx, nil, rootCoord)
		assert.NoError(t, err)

		_, err = broker.getFieldSchemaByID(1, 1000)
		assert.Error(t, err)
	})

	t.Run("getFieldSchemaByID error", func(t *testing.T) {
		rootCoord := &RootCoordMock{
			Fail: false,
			Err:  true,
		}
		broker, err := newGlobalMetaBroker(ctx, nil, rootCoord)
		assert.NoError(t, err)

		_, err = broker.getFieldSchemaByID(1, 1000)
		assert.Error(t, err)
	})

	t.Run("getFieldSchemaByID fail", func(t *testing.T) {
		dataCoord := &DataCoordMock{
			Fail: true,
			Err:  false,
		}
		broker, err := newGlobalMetaBroker(ctx, dataCoord, nil)
		assert.NoError(t, err)

		_, err = broker.getDataSegmentInfosByIDs([]int64{1000})
		assert.Error(t, err)
	})

	t.Run("getFieldSchemaByID error", func(t *testing.T) {
		dataCoord := &DataCoordMock{
			Fail: false,
			Err:  true,
		}
		broker, err := newGlobalMetaBroker(ctx, dataCoord, nil)
		assert.NoError(t, err)

		_, err = broker.getDataSegmentInfosByIDs([]int64{1000})
		assert.Error(t, err)
	})
}
