package recovery

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v2/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

func TestRecoveryStore(t *testing.T) {
	events.InitRecordAllEventsNotifier()

	sv := map[string]string{}
	useErr := false
	kv := mock_kv.NewMockMetaKv(t)
	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) ([]string, []string, error) {
		if useErr {
			return nil, nil, errors.New("load failed")
		}
		keys := make([]string, 0, len(sv))
		vals := make([]string, 0, len(sv))
		for k, v := range sv {
			keys = append(keys, k)
			vals = append(vals, v)
		}
		return keys, vals, nil
	})
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, m map[string]string, s []string, p ...predicates.Predicate) error {
		if rand.Int31n(10) < 3 || useErr {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("save failed")
		}
		time.Sleep(time.Duration(rand.Int31n(2)) * time.Millisecond)
		for k, v := range m {
			sv[k] = v
		}
		for _, k := range s {
			delete(sv, k)
		}
		return nil
	})

	rs := NewRecoveryStorage(kv)
	views, err := rs.List(context.Background())
	assert.Len(t, views, 0)
	assert.NoError(t, err)
	assert.NoError(t, rs.Save(context.Background(), &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    1,
			Vchannel:     "v1",
			Version:      &viewpb.QueryViewVersion{DataVersion: 1, QueryVersion: 2},
			State:        viewpb.QueryViewState_QueryViewStatePreparing,
		},
	}, &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    1,
			Vchannel:     "v2",
			Version:      &viewpb.QueryViewVersion{DataVersion: 1, QueryVersion: 3},
			State:        viewpb.QueryViewState_QueryViewStateUp,
		},
	}))
	views, err = rs.List(context.Background())
	assert.NoError(t, err)
	assert.Len(t, views, 2)
	views[0].Meta.State = viewpb.QueryViewState_QueryViewStateDropped
	assert.NoError(t, rs.Save(context.Background(), views[0]))

	views, err = rs.List(context.Background())
	assert.NoError(t, err)
	assert.Len(t, views, 1)

	useErr = true

	_, err = rs.List(context.Background())
	assert.Error(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err = rs.Save(ctx, views[0])
	assert.Error(t, err)
}
