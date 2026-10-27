package queryclient

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LoadInfoClient is implemented by the Coordinator client. Version zero must
// resolve the current load configuration, not a retained historical version.
type LoadInfoClient interface {
	GetQueryViewLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoRequest, ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error)
}

const loadStateCheckInterval = time.Second

// collectionLoadChecks shares probes across active queries and shards. Entries
// live only while requests use them; it owns no background goroutines.
type collectionLoadChecks struct {
	mu          sync.Mutex
	client      LoadInfoClient
	collections map[int64]*collectionLoadCheck
}

type collectionLoadCheck struct {
	mu           sync.Mutex
	owner        *collectionLoadChecks
	collectionID int64
	refs         int // protected by owner.mu
	inflight     *loadStateCall
	nextCheck    time.Time // only successful checks are cached
}

type loadStateCall struct {
	done      chan struct{}
	err       error
	nextCheck time.Time
}

func newCollectionLoadChecks(client LoadInfoClient) *collectionLoadChecks {
	return &collectionLoadChecks{client: client, collections: make(map[int64]*collectionLoadCheck)}
}

func (s *collectionLoadChecks) acquire(collectionID int64) *collectionLoadCheck {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.collections[collectionID]
	if state == nil {
		state = &collectionLoadCheck{owner: s, collectionID: collectionID}
		s.collections[collectionID] = state
	}
	state.refs++
	return state
}

func (s *collectionLoadCheck) release() {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	s.refs--
	if s.refs == 0 {
		delete(s.owner.collections, s.collectionID)
	}
}

// check returns when another check is due. It never turns an unavailable
// Coordinator into CollectionNotLoaded or retains a negative result for reuse.
func (s *collectionLoadCheck) check(ctx context.Context) (time.Time, error) {
	for {
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
		s.mu.Lock()
		if call := s.inflight; call != nil {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return time.Time{}, ctx.Err()
			case <-call.done:
				// The caller owning the RPC can be canceled independently of
				// other queries sharing this probe. Let those queries try again.
				if merr.IsCanceledOrTimeout(call.err) && ctx.Err() == nil {
					continue
				}
				return call.nextCheck, call.err
			}
		}
		if time.Now().Before(s.nextCheck) {
			next := s.nextCheck
			s.mu.Unlock()
			return next, nil
		}
		call := &loadStateCall{done: make(chan struct{})}
		s.inflight = call
		s.mu.Unlock()

		if s.owner.client == nil {
			call.err = merr.WrapErrServiceInternalMsg("query client requires a Coordinator load-info client")
		} else {
			probeCtx, cancel := context.WithTimeout(ctx, loadStateCheckInterval)
			info, err := s.owner.client.GetQueryViewLoadInfo(probeCtx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: s.collectionID})
			call.err = merr.CheckRPCCall(info, err)
			cancel()
		}
		s.mu.Lock()
		if call.err == nil {
			s.nextCheck = time.Now().Add(loadStateCheckInterval)
		}
		call.nextCheck = s.nextCheck
		s.inflight = nil
		close(call.done)
		s.mu.Unlock()
		return call.nextCheck, call.err
	}
}
