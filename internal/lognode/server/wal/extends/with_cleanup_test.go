package extends

import (
	"testing"

	"github.com/cockroachdb/errors"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	"github.com/stretchr/testify/assert"
)

func TestScannerWithCleanup(t *testing.T) {
	mockScanner := mock_wal.NewMockScanner(t)
	var closed bool
	mockScanner.EXPECT().Close().RunAndReturn(func() error {
		closed = true
		return nil
	})
	calledCount := 0
	cleanup := func() {
		calledCount++
	}
	scanner := ScannerWithCleanup(mockScanner, cleanup)
	assert.Nil(t, scanner.Close())
	assert.Equal(t, 1, calledCount)
	assert.True(t, closed)

	closed = false
	err := errors.New("test")
	mockScanner.ExpectedCalls = nil
	mockScanner.EXPECT().Close().RunAndReturn(func() error {
		closed = true
		return err
	})

	scanner = ScannerWithCleanup(mockScanner, cleanup)
	assert.Equal(t, err, scanner.Close())
	assert.Equal(t, 2, calledCount)
	assert.True(t, closed)
}

func TestWALWithCleanup(t *testing.T) {
	mockWAL := mock_wal.NewMockWAL(t)
	var closed bool
	mockWAL.EXPECT().Close().Run(func() {
		closed = true
	})
	calledCount := 0
	cleanup := func() {
		calledCount++
	}
	wal := WALWithCleanup(mockWAL, cleanup)
	wal.Close()
	assert.Equal(t, 1, calledCount)
	assert.True(t, closed)

	closed = false
	mockWAL.ExpectedCalls = nil
	mockWAL.EXPECT().Close().Run(func() {
		closed = true
	})

	wal = WALWithCleanup(mockWAL, cleanup)
	wal.Close()
	assert.Equal(t, 2, calledCount)
	assert.True(t, closed)
}
