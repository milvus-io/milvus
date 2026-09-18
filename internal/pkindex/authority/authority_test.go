package authority

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func newTestAuthority(t *testing.T) Authority {
	t.Helper()
	a, err := New("v1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func TestAuthorityIsReadyAfterNew(t *testing.T) {
	a := newTestAuthority(t)
	select {
	case <-a.Ready():
	default:
		t.Fatal("authority should be ready once the engine is created")
	}
}

func TestAuthorityGetAndWrite(t *testing.T) {
	ctx := context.Background()
	a := newTestAuthority(t)

	entry, err := a.Get(ctx, Int64PK(1))
	require.NoError(t, err)
	require.Nil(t, entry)

	b := a.NewBatch()
	b.Put(Int64PK(1), Entry{SegmentID: 100})
	b.Put(Int64PK(2), Entry{SegmentID: 200})
	require.Equal(t, 2, b.Len())

	// nothing is visible before Write.
	entry, err = a.Get(ctx, Int64PK(1))
	require.NoError(t, err)
	require.Nil(t, entry)

	require.NoError(t, a.Write(ctx, b))
	entries, err := a.MultiGet(ctx, []PK{Int64PK(2), Int64PK(3), Int64PK(1)})
	require.NoError(t, err)
	require.Equal(t, []*Entry{{SegmentID: 200}, nil, {SegmentID: 100}}, entries)
}

func TestAuthorityOverwriteAndDelete(t *testing.T) {
	ctx := context.Background()
	a := newTestAuthority(t)

	b := a.NewBatch()
	b.Put(VarCharPK("a"), Entry{SegmentID: 1})
	require.NoError(t, a.Write(ctx, b))

	b = a.NewBatch()
	b.Put(VarCharPK("a"), Entry{SegmentID: 2})
	b.Delete(VarCharPK("b")) // deleting an absent key is fine
	require.NoError(t, a.Write(ctx, b))
	entry, err := a.Get(ctx, VarCharPK("a"))
	require.NoError(t, err)
	require.Equal(t, &Entry{SegmentID: 2}, entry)

	b = a.NewBatch()
	b.Delete(VarCharPK("a"))
	require.NoError(t, a.Write(ctx, b))
	entry, err = a.Get(ctx, VarCharPK("a"))
	require.NoError(t, err)
	require.Nil(t, entry)
}

func TestAuthorityEmptyBatchAndEmptyMultiGet(t *testing.T) {
	ctx := context.Background()
	a := newTestAuthority(t)
	require.NoError(t, a.Write(ctx, a.NewBatch()))
	entries, err := a.MultiGet(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestAuthorityRejectsCorruptValue(t *testing.T) {
	ctx := context.Background()
	engine, err := NewMemoryEngine("v1")
	require.NoError(t, err)
	require.NoError(t, engine.Write(ctx, []Mutation{{Key: Int64PK(1).Encode(), Value: []byte{1, 2, 3}}}))
	a := newAuthority("v1", engine)

	_, err = a.Get(ctx, Int64PK(1))
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrNoEngineFactory))
}

func TestAuthorityAfterClose(t *testing.T) {
	ctx := context.Background()
	a, err := New("v1")
	require.NoError(t, err)
	require.NoError(t, a.Close())
	_, err = a.Get(ctx, Int64PK(1))
	require.Error(t, err)
}

// fakeLengthEngine returns a MultiGet result of a fixed length, regardless of
// the number of keys asked for.
type fakeLengthEngine struct {
	Engine
	returnLen int
}

func (e *fakeLengthEngine) MultiGet(_ context.Context, _ [][]byte) ([][]byte, error) {
	return make([][]byte, e.returnLen), nil
}

func TestAuthorityMultiGetRejectsWrongLength(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		returnLen int
	}{
		{"too few values", 1},
		{"too many values", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newAuthority("v1", &fakeLengthEngine{returnLen: tc.returnLen})
			_, err := a.MultiGet(ctx, []PK{Int64PK(1), Int64PK(2)})
			require.Error(t, err)
		})
	}
}

func TestNewWithoutEngineFactory(t *testing.T) {
	RegisterEngineFactory(nil)
	defer RegisterEngineFactory(NewMemoryEngine)

	require.False(t, HasEngineFactory())
	_, err := New("v1")
	require.True(t, errors.Is(err, ErrNoEngineFactory))
}
