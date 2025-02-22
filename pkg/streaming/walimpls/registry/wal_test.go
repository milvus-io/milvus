package registry

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/mock_walimpls"
)

func TestRegister(t *testing.T) {
	name := "mock"
	b := mock_walimpls.NewMockOpenerBuilderImpls(t)
	b.EXPECT().Name().Return(name)

	RegisterBuilder(b)
	b2 := MustGetBuilder(name)
	assert.Equal(t, b.Name(), b2.Name())

	// Panic if register twice.
	assert.Panics(t, func() {
		RegisterBuilder(b)
	})

	// Panic if get not exist builder.
	assert.Panics(t, func() {
		MustGetBuilder("not exist")
	})

	// Test concurrent.
	wg := sync.WaitGroup{}
	count := 10
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("mock_%d", i)
			b := mock_walimpls.NewMockOpenerBuilderImpls(t)
			b.EXPECT().Name().Return(name)
			RegisterBuilder(b)
			b2 := MustGetBuilder(name)
			assert.Equal(t, b.Name(), b2.Name())
		}(i)
	}
	wg.Wait()
}
