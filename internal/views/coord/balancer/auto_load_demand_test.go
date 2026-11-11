package balancer

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAutoLoadDemandLifetime(t *testing.T) {
	var d AutoLoadDemand
	require.False(t, d.Pending(1))
	first, second, other := d.Begin(1), d.Begin(1), d.Begin(2)
	first()
	require.True(t, d.Pending(1))
	second()
	require.False(t, d.Pending(1))
	require.True(t, d.Pending(2))
	other()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			finish := d.Begin(1)
			require.True(t, d.Pending(1))
			finish()
		}()
	}
	wg.Wait()
	require.Empty(t, d.waiting)
}
