package inspector_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestInsepctor(t *testing.T) {
	paramtable.Init()

	i := inspector.NewTimeTickSyncInspector()
	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	pchannel := types.PChannelInfo{
		Name: "test",
		Term: 1,
	}
	operator.EXPECT().Channel().Return(pchannel)
	operator.EXPECT().Sync(mock.Anything, mock.Anything).Run(func(ctx context.Context, forcePersisted bool) {})

	i.RegisterSyncOperator(operator)
	assert.Panics(t, func() {
		i.RegisterSyncOperator(operator)
	})
	i.TriggerSync(pchannel, false)
	o := i.MustGetOperator(pchannel)
	assert.NotNil(t, o)
	time.Sleep(250 * time.Millisecond)
	i.UnregisterSyncOperator(operator)

	assert.Panics(t, func() {
		i.UnregisterSyncOperator(operator)
	})
	assert.Panics(t, func() {
		i.MustGetOperator(pchannel)
	})
	i.Close()
}
