package rootcoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCatalogMigrationGateDrainsInFlightAndRejectsNewWrites(t *testing.T) {
	gate := newCatalogMigrationGate()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done, err := gate.BeginMetadataWrite(ctx)
	require.NoError(t, err)

	gate.StartDraining()
	_, err = gate.BeginMetadataWrite(ctx)
	require.Error(t, err)

	drained := make(chan error, 1)
	go func() {
		drained <- gate.WaitDrained(ctx)
	}()

	select {
	case err := <-drained:
		require.NoError(t, err)
		t.Fatal("gate reported drained while a write was still in flight")
	case <-time.After(20 * time.Millisecond):
	}

	done()
	require.NoError(t, <-drained)
}

func TestCatalogMigrationGateCanResumeNormalWrites(t *testing.T) {
	gate := newCatalogMigrationGate()
	gate.StartDraining()
	gate.Resume()

	done, err := gate.BeginMetadataWrite(context.Background())
	require.NoError(t, err)
	done()
}
