package recovery

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestTruncator(t *testing.T) {
	w := mock_walimpls.NewMockWALImpls(t)
	w.EXPECT().Truncate(mock.Anything, mock.Anything).Return(nil)
	paramtable.Get().Save(paramtable.Get().StreamingCfg.WALTruncateSampleInterval.Key, "1ms")
	paramtable.Get().Save(paramtable.Get().StreamingCfg.WALTruncateRetentionInterval.Key, "2ms")

	truncator := newSamplingTruncator(&WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
		Magic:     recoveryMagicStreamingInitialized,
	}, w, newRecoveryStorageMetrics(types.PChannelInfo{Name: "test", Term: 1}))

	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Millisecond)
		for rand.Int31n(3) < 1 {
			truncator.SampleCheckpoint(&WALCheckpoint{
				MessageID: rmq.NewRmqID(int64(i)),
				TimeTick:  tsoutil.ComposeTSByTime(time.Now(), 0),
				Magic:     recoveryMagicStreamingInitialized,
			})
		}
	}
	truncator.Close()
}
