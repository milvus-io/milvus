package authority

import (
	"context"
	"encoding/binary"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ErrNoEngineFactory is returned by New when no engine factory is registered.
// It is a plain identity sentinel, not a merr error, so that errors.Is matches
// only this error and not every merr service-internal error.
var ErrNoEngineFactory = errors.New("no primary key index engine factory is registered")

// Entry is what the index stores for one primary key.
type Entry struct {
	SegmentID int64
}

func (e Entry) encode() []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(e.SegmentID))
	return value
}

func decodeEntry(value []byte) (*Entry, error) {
	if len(value) != 8 {
		return nil, merr.WrapErrServiceInternalMsg("corrupted primary key index entry of %d bytes", len(value))
	}
	return &Entry{SegmentID: int64(binary.BigEndian.Uint64(value))}, nil
}

// Authority is the primary key index of one vchannel. It has key-value semantics
// only: it knows nothing about the WAL or about deduplication.
//
// TODO: the index starts empty on every WAL open. Recovery and lazy loading are not designed yet.
type Authority interface {
	// Get returns the entry of pk, nil if pk is absent.
	Get(ctx context.Context, pk PK) (*Entry, error)
	// MultiGet returns the entries aligned with pks, nil for absent keys.
	MultiGet(ctx context.Context, pks []PK) ([]*Entry, error)
	NewBatch() *Batch
	// Write applies the whole batch atomically.
	Write(ctx context.Context, b *Batch) error
	// Ready is closed once the index of this vchannel can be used.
	// It gates this vchannel only, never the whole pchannel.
	Ready() <-chan struct{}
	Close() error
}

// New creates the Authority of a vchannel with the registered engine factory.
func New(vchannel string) (Authority, error) {
	factory := getEngineFactory()
	if factory == nil {
		return nil, ErrNoEngineFactory
	}
	engine, err := factory(vchannel)
	if err != nil {
		return nil, errors.Wrapf(err, "create primary key index engine of vchannel %s", vchannel)
	}
	return newAuthority(vchannel, engine), nil
}

func newAuthority(vchannel string, engine Engine) *authorityImpl {
	ready := make(chan struct{})
	close(ready)
	return &authorityImpl{vchannel: vchannel, engine: engine, ready: ready}
}

type authorityImpl struct {
	vchannel string
	engine   Engine
	ready    chan struct{}
}

func (a *authorityImpl) Get(ctx context.Context, pk PK) (*Entry, error) {
	entries, err := a.MultiGet(ctx, []PK{pk})
	if err != nil {
		return nil, err
	}
	return entries[0], nil
}

func (a *authorityImpl) MultiGet(ctx context.Context, pks []PK) ([]*Entry, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	keys := make([][]byte, len(pks))
	for i, pk := range pks {
		keys[i] = pk.Encode()
	}
	values, err := a.engine.MultiGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(values) != len(keys) {
		return nil, merr.WrapErrServiceInternalMsg("primary key index engine returned %d values for %d keys", len(values), len(keys))
	}
	entries := make([]*Entry, len(pks))
	for i, value := range values {
		if value == nil {
			continue
		}
		if entries[i], err = decodeEntry(value); err != nil {
			return nil, err
		}
	}
	return entries, nil
}

func (a *authorityImpl) NewBatch() *Batch {
	return &Batch{}
}

func (a *authorityImpl) Write(ctx context.Context, b *Batch) error {
	if b == nil || len(b.muts) == 0 {
		return nil
	}
	return a.engine.Write(ctx, b.muts)
}

func (a *authorityImpl) Ready() <-chan struct{} {
	return a.ready
}

func (a *authorityImpl) Close() error {
	return a.engine.Close()
}
