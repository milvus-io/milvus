package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type SnapshotBuilder struct{}

func (p SnapshotBuilder) NewRuntime() (*Runtime, error) {
	return newRuntime(), nil
}

func (r *Runtime) Prepare(ctx context.Context, view walview.VChannelWALView) error {
	if r == nil {
		return nil
	}
	if err := validateWALViewSnapshot(view); err != nil {
		return err
	}
	collection, err := newCollection(view)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		if collection != nil {
			collection.Release()
		}
		return context.Canceled
	}
	r.collection = collection
	r.mu.Unlock()

	prepared := false
	defer func() {
		if !prepared {
			r.Close()
		}
	}()
	for _, visible := range view.SegmentSnapshot.Segments {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		segment, err := newGrowingSegmentFromVisible(ctx, collection, visible)
		if err != nil {
			return err
		}
		if !r.addSegment(segment) {
			segment.release()
			return context.Canceled
		}
	}
	deleteEntries, err := drainDeleteReplay(ctx, view.DeleteReplay)
	if err != nil {
		return err
	}
	for _, entry := range deleteEntries {
		if err := r.applyTransformLogEntry(ctx, entry); err != nil {
			return err
		}
	}
	r.markGrowingTimeTick(view.BaseGrowingTimeTick)
	r.markTransformTimeTick(view.BaseTransformTimeTick)
	prepared = true
	return nil
}

func newCollection(view walview.VChannelWALView) (*segcore.CCollection, error) {
	if view.Schema == nil {
		return nil, nil
	}
	return segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID:  view.CollectionID,
		Schema:        view.Schema,
		LoadFieldList: settingsFromWALView(view).GetRequiredFields(),
	})
}

func validateWALViewSnapshot(view walview.VChannelWALView) error {
	snapshot := view.SegmentSnapshot
	if snapshot.CollectionID != 0 && snapshot.CollectionID != view.CollectionID {
		return errors.Errorf(
			"wal view snapshot mismatch: view collection %d, snapshot collection %d",
			view.CollectionID,
			snapshot.CollectionID,
		)
	}
	if snapshot.VChannel != "" && snapshot.VChannel != view.VChannel {
		return errors.Errorf(
			"wal view snapshot mismatch: view vchannel %s, snapshot vchannel %s",
			view.VChannel,
			snapshot.VChannel,
		)
	}
	return nil
}

func drainDeleteReplay(ctx context.Context, scanner wal.TransformLogScanner) ([]*streamingpb.TransformLogEntry, error) {
	if scanner == nil {
		return nil, nil
	}
	entries := make([]*streamingpb.TransformLogEntry, 0)
	for {
		select {
		case event, ok := <-scanner.Chan():
			if !ok {
				return entries, scanner.Close()
			}
			if event.Entry != nil {
				entries = append(entries, event.Entry)
			}
			if event.CaughtUp != nil {
				return entries, scanner.Close()
			}
		case <-scanner.Done():
			return entries, scanner.Close()
		case <-ctx.Done():
			_ = scanner.Close()
			return nil, ctx.Err()
		}
	}
}
