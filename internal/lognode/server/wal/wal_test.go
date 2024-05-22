package wal_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	"github.com/stretchr/testify/assert"
)

func TestRegister(t *testing.T) {
	name := "mock"
	b := mock_wal.NewMockOpenerBuilder(t)
	b.EXPECT().Name().Return(name)

	wal.RegisterBuilder(b)
	b2 := wal.MustGetBuilder(name)
	assert.Equal(t, b, b2)

	// Panic if register twice.
	assert.Panics(t, func() {
		wal.RegisterBuilder(b)
	})

	// Panic if get not exist builder.
	assert.Panics(t, func() {
		wal.MustGetBuilder("not exist")
	})

	// Test concurrent.
	wg := sync.WaitGroup{}
	count := 10
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("mock_%d", i)
			b := mock_wal.NewMockOpenerBuilder(t)
			b.EXPECT().Name().Return(name)
			wal.RegisterBuilder(b)
			b2 := wal.MustGetBuilder(name)
			assert.Equal(t, b, b2)
		}(i)
	}
	wg.Wait()
}
