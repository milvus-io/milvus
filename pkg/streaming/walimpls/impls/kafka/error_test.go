package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestValidateKafkaTopicMetadata(t *testing.T) {
	t.Run("topic exists", func(t *testing.T) {
		err := validateKafkaTopicMetadata(&kafka.Metadata{
			Topics: map[string]kafka.TopicMetadata{
				"existing-topic": {Topic: "existing-topic"},
			},
		}, "existing-topic")
		require.NoError(t, err)
	})

	t.Run("topic is absent from metadata snapshot", func(t *testing.T) {
		err := validateKafkaTopicMetadata(&kafka.Metadata{
			Topics: map[string]kafka.TopicMetadata{},
		}, "missing-topic")
		require.ErrorIs(t, err, merr.ErrMqTopicNotFound)
	})

	t.Run("target topic missing error is unavailable", func(t *testing.T) {
		expected := kafka.NewError(kafka.ErrUnknownTopicOrPart, "unknown topic", false)
		err := validateKafkaTopicMetadata(&kafka.Metadata{
			Topics: map[string]kafka.TopicMetadata{
				"existing-topic": {
					Topic: "existing-topic",
					Error: expected,
				},
			},
		}, "existing-topic")
		require.ErrorIs(t, err, merr.ErrMqTopicNotFound)
	})

	t.Run("nil metadata is an internal error", func(t *testing.T) {
		err := validateKafkaTopicMetadata(nil, "topic")
		require.ErrorIs(t, err, merr.ErrMqInternal)
	})
}

func TestValidateKafkaTopicExistsPreservesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, validateKafkaTopicExists(ctx, nil, "topic"), context.Canceled)
}

type testKafkaMetadataGetter struct {
	testingT *testing.T
	metadata *kafka.Metadata
	err      error
}

func (g *testKafkaMetadataGetter) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	require.NotNil(g.testingT, topic)
	require.Equal(g.testingT, "topic", *topic)
	require.False(g.testingT, allTopics)
	require.Equal(g.testingT, int((3 * time.Second).Milliseconds()), timeoutMs)
	return g.metadata, g.err
}

func TestValidateKafkaTopicExistsQueriesOnlyTargetTopic(t *testing.T) {
	oldValue := paramtable.Get().KafkaCfg.ReadTimeout.SwapTempValue("3")
	defer paramtable.Get().KafkaCfg.ReadTimeout.SwapTempValue(oldValue)

	getter := &testKafkaMetadataGetter{
		testingT: t,
		metadata: &kafka.Metadata{Topics: map[string]kafka.TopicMetadata{
			"topic": {Topic: "topic"},
		}},
	}
	require.NoError(t, validateKafkaTopicExists(context.Background(), getter, "topic"))
}

func TestConsumerConfigForRead(t *testing.T) {
	baseConfig := kafka.ConfigMap{"allow.auto.create.topics": true}

	readOnlyWAL := &walImpl{
		WALHelper: helper.NewWALHelper(&walimpls.OpenOption{
			Channel: types.PChannelInfo{Name: "read-only", AccessMode: types.AccessModeRO},
		}),
		consumerConfig: baseConfig,
	}
	require.Equal(t, false, readOnlyWAL.consumerConfigForRead()["allow.auto.create.topics"])
	require.Equal(t, true, baseConfig["allow.auto.create.topics"])

	readWriteWAL := &walImpl{
		WALHelper: helper.NewWALHelper(&walimpls.OpenOption{
			Channel: types.PChannelInfo{Name: "read-write", AccessMode: types.AccessModeRW},
		}),
		consumerConfig: baseConfig,
	}
	require.Equal(t, true, readWriteWAL.consumerConfigForRead()["allow.auto.create.topics"])
}
