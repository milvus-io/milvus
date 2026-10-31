package coordview

import (
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const (
	initialPreparationBackoff = 100 * time.Millisecond
	maxPreparationBackoff     = 5 * time.Second
)

// preparationBackoff belongs to the shard-manager lifecycle and is protected
// by its mutex. It throttles publication, not in-memory admission. Only actual
// failed preparations count; preemption, release and node loss do not.
// The history survives DataView changes and accepted replacement views. It is
// ephemeral: coordinator recovery starts with no delay.
type preparationBackoff struct {
	lastFailure qviews.QueryViewVersion
	failed      bool
	delay       time.Duration
	notBefore   time.Time
}

func (b *preparationBackoff) onFailure(version qviews.QueryViewVersion, now time.Time) {
	if b.failed && !version.GT(b.lastFailure) {
		return
	}
	b.lastFailure, b.failed = version, true
	if b.delay == 0 {
		b.delay = initialPreparationBackoff
	} else {
		b.delay = min(maxPreparationBackoff, b.delay*2)
	}
	b.notBefore = now.Add(b.delay)
}

func (b *preparationBackoff) onUp(version qviews.QueryViewVersion) {
	// An older published-schema view can become Up while the target schema is
	// failing. It must not erase the newer preparation's failure history.
	if b.failed && !version.GT(b.lastFailure) {
		return
	}
	*b = preparationBackoff{}
}

func preparationNodesChanged(previous *CoordQueryViewStateMachine, next *viewpb.QueryViewOfShard) bool {
	if len(previous.qnStates) != len(next.GetQueryNode()) {
		return true
	}
	for _, node := range next.GetQueryNode() {
		if _, exists := previous.qnStates[node.GetNodeId()]; !exists {
			return true
		}
	}
	return false
}
