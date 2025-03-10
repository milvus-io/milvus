package shardview

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

var (
	ErrShardReleased     = errors.New("shard is on-releasing")
	ErrDataVersionTooOld = errors.New("data version is too old")
)

// NewShardView creates a new ShardView.
func NewShardView(shardID qviews.ShardID) *shardViewImpl {
	return &shardViewImpl{
		shardID:            shardID,
		released:           false,
		onPreparingVersion: nil,
		lastestVersion:     nil,
		queryViews:         make(map[qviews.QueryViewVersion]*queryView),
	}
}

// RecoverShardView recovers the shard views from data in recovery storage.
func RecoverShardView(viewProtos []*viewpb.QueryViewOfShard) *shardViewImpl {
	if len(viewProtos) == 0 {
		panic("the views should not be empty when recovery")
	}
	var onPreparingVersion *qviews.QueryViewVersion
	var upVersion *qviews.QueryViewVersion
	var lastestVersion *qviews.QueryViewVersion
	shardID := qviews.NewShardIDFromQVMeta(viewProtos[0].Meta)
	qvs := make(map[qviews.QueryViewVersion]*queryView, len(viewProtos))
	for _, viewProto := range viewProtos {
		qv := newRecoveredQueryView(viewProto)
		if qv.ShardID() != shardID {
			panic(fmt.Sprintf("inconsistency of the recovery data, the shard id is not the same, %s, %s",
				qv.ShardID().String(),
				shardID.String(),
			))
		}
		v := qv.Version()
		switch qv.State() {
		case qviews.QueryViewStatePreparing:
			if onPreparingVersion != nil {
				panic(fmt.Sprintf("inconsistency of the recovery data, there's multiple on-preparing view: %s, %s, %s",
					shardID.String(),
					onPreparingVersion.String(),
					v.String(),
				))
			}
			onPreparingVersion = &v
		case qviews.QueryViewStateUp:
			if upVersion != nil {
				panic(fmt.Sprintf("inconsistency of the recovery data, there's multiple up view: %s, %s, %s",
					shardID.String(),
					upVersion.String(),
					v.String(),
				))
			}
			upVersion = &v
		}
		if lastestVersion == nil || v.GT(*lastestVersion) {
			lastestVersion = &v
		}
		qvs[qv.Version()] = qv
	}
	if onPreparingVersion != nil && lastestVersion != nil && !onPreparingVersion.EQ(*lastestVersion) {
		panic(fmt.Sprintf("inconsistency of the recovery data, the on-preparing view version must keep same as the latest view version: %s, %s, %s",
			shardID.String(),
			onPreparingVersion.String(),
			lastestVersion.String(),
		))
	}

	s := &shardViewImpl{
		shardID:            shardID,
		released:           false,
		onPreparingVersion: onPreparingVersion,
		upVersion:          upVersion,
		lastestVersion:     lastestVersion,
		queryViews:         qvs,
	}
	if s.onPreparingVersion != nil {
		// If there's a on-preparing view, start to deal all the unrecoverable views after recovery directly.
		s.dealAllUnrecoverableView()
		s.consumePendingAckViews()
	}
	return s
}

// shardViewImpl is a stable view of a shard, once some operation is applied to the view, it will become a unstable view.
// There should be at most one on-preparing query view and at most one up query view in one shard view.
type shardViewImpl struct {
	shardID  qviews.ShardID
	released bool // released is a flag to indicate that the shard is released.

	onPreparingVersion *qviews.QueryViewVersion // onPreparingQueryViewVersion is the version of the query view that is on-preparing.
	// use it to promise there is always only zero or one on-preparing query view.
	upVersion *qviews.QueryViewVersion // upVersion is the version of the query view that is up.
	// use it to promise there is always only zero or one up query view at coordinator.

	lastestVersion *qviews.QueryViewVersion               // lastestVersion is the latest version of the query view.
	queryViews     map[qviews.QueryViewVersion]*queryView // A map to store the query view of the shard.
}

// ApplyNewQueryView applies a new query view into the query views of the shard.
// It will replace the on-working preparing view if exists.
func (qvs *shardViewImpl) ApplyNewQueryView(ctx context.Context, b *QueryViewAtCoordBuilder) (*qviews.QueryViewVersion, error) {
	if qvs.released {
		return nil, ErrShardReleased
	}

	// if the latest up version is not nil and the data version is too old, return error directly.
	dataVersion := b.DataVersion()
	if qvs.lastestVersion != nil && qvs.lastestVersion.DataVersion > dataVersion {
		return nil, errors.Wrapf(ErrDataVersionTooOld, "the data version is too old, last: %d, incoming: %d", qvs.lastestVersion.DataVersion, dataVersion)
	}

	// Assign a new query version for new incoming query view and make a swap.
	newQueryVersion := qvs.getMaxQueryVerion(dataVersion)
	newQueryView := b.withQueryVersion(newQueryVersion).Build()

	persists := make([]*viewpb.QueryViewOfShard, 0, 2)
	persists = append(persists, newQueryView.ProtoWaitForPersist())
	if previousQueryView := qvs.swapPreparing(newQueryView); previousQueryView != nil {
		previousQueryView.EnterUnrecoverable()
		persists = append(persists, previousQueryView.ProtoWaitForPersist())
	}

	if err := recoveryStorage.Save(ctx, persists...); err != nil {
		return nil, err
	}

	// There's already a new preparing view, so the previous unavailable view can be dropped to release the related resources.
	qvs.dealAllUnrecoverableView()
	qvs.consumePendingAckViews()
	return qvs.onPreparingVersion, nil
}

// RequestRelease releases the shard views.
func (qvs *shardViewImpl) RequestRelease(ctx context.Context) error {
	if qvs.released {
		return nil
	}
	// make a fence by released flag.
	qvs.released = true

	persists := make([]*viewpb.QueryViewOfShard, 0, len(qvs.queryViews))
	for _, qv := range qvs.queryViews {
		// Only the unstable state need to change the states.
		switch qv.State() {
		case qviews.QueryViewStateUp:
			qv.EnterDown()
		case qviews.QueryViewStateReady, qviews.QueryViewStatePreparing:
			qv.EnterUnrecoverable()
		default:
			continue
		}
		persists = append(persists, qv.ProtoWaitForPersist())
	}
	if err := recoveryStorage.Save(ctx, persists...); err != nil {
		return err
	}
	qvs.dealAllUnrecoverableView()
	qvs.consumePendingAckViews()
	return nil
}

// ApplyViewFromWorkNode is called when the work node acknowledged the query view.
func (qvs *shardViewImpl) ApplyViewFromWorkNode(ctx context.Context, w qviews.QueryViewAtWorkNode) error {
	qv, ok := qvs.queryViews[w.Version()]
	if !ok {
		if w.State() != qviews.QueryViewStateDropped {
			panic("the query view is not found in shard, a critical bug in query view state machine, some resource is leaked")
		}
		return nil
	}
	qv.ApplyViewFromWorkNode(w)

	// Check if there's a new up view.
	if qvs.onPreparingVersion != nil && qv.Version() == *qvs.onPreparingVersion {
		if qv.State() == qviews.QueryViewStateUp {
			persists := make([]*viewpb.QueryViewOfShard, 0, 2)
			if previousUpView := qvs.swapUpView(); previousUpView != nil {
				previousUpView.EnterDown()
				persists = append(persists, previousUpView.ProtoWaitForPersist())
			}
			persists = append(persists, qv.ProtoWaitForPersist())
			if err := recoveryStorage.Save(ctx, persists...); err != nil {
				return err
			}
		} else if qv.State() == qviews.QueryViewStateUnrecoverable {
			if err := recoveryStorage.Save(ctx, qv.ProtoWaitForPersist()); err != nil {
				return err
			}
			qvs.onPreparingVersion = nil
		}
	}

	// Check if there's a new dropped view.
	if qv.State() == qviews.QueryViewStateDropped {
		if err := recoveryStorage.Save(ctx, qv.ProtoWaitForPersist()); err != nil {
			return err
		}
		delete(qvs.queryViews, qv.Version())
	}

	// If there're some pending ack views, consume them.
	qvs.consumePendingAckViews()
	return nil
}

// consumePendingAckViews consumes the pending ack views from the query views.
func (qvs *shardViewImpl) consumePendingAckViews() {
	g := syncer.SyncGroup{}
	for _, qv := range qvs.queryViews {
		for _, view := range qv.ConsumePendingAckViews() {
			g.AddView(view)
		}
	}
	coordSyncer.Sync(g)
}

// swapPreparing swaps the on-preparing query view with new query view.
func (qvs *shardViewImpl) swapPreparing(newQueryView *queryView) *queryView {
	var previousQueryView *queryView
	if qvs.onPreparingVersion != nil {
		previousQueryView = qvs.queryViews[*qvs.onPreparingVersion]
	}
	newVersion := newQueryView.Version()
	qvs.queryViews[newVersion] = newQueryView
	qvs.lastestVersion = &newVersion
	qvs.onPreparingVersion = &newVersion
	return previousQueryView
}

// swapUpView swaps the up view with the on-preparing view.
func (qvs *shardViewImpl) swapUpView() *queryView {
	if qvs.onPreparingVersion == nil {
		panic("the on-preparing version should not be nil when swap up view")
	}
	// Check if there's a new up view, down the old up view and up the new incoming view.
	var previousUpView *queryView
	if qvs.upVersion != nil {
		previousUpView = qvs.queryViews[*qvs.upVersion]
	}
	qvs.upVersion = qvs.onPreparingVersion
	qvs.onPreparingVersion = nil
	return previousUpView
}

// dealAllUnrecoverableView drops all the unrecoverable query views.
func (qvs *shardViewImpl) dealAllUnrecoverableView() {
	for _, qv := range qvs.queryViews {
		if qv.State() == qviews.QueryViewStateUnrecoverable {
			qv.DealUnrecoverable()
		}
	}
}

// getMaxQueryVersion returns the max query version of the data version.
func (qvs *shardViewImpl) getMaxQueryVerion(dataVersion int64) int64 {
	if qvs.lastestVersion != nil && qvs.lastestVersion.DataVersion == dataVersion {
		return qvs.lastestVersion.QueryVersion + 1
	}
	return 1
}
