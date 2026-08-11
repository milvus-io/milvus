package kafka

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestConvertKafkaReadError(t *testing.T) {
	t.Run("non-fatal topic error remains recoverable", func(t *testing.T) {
		expected := kafka.NewError(kafka.ErrUnknownTopicOrPart, "unknown topic", false)
		err := convertKafkaReadError(
			expected,
			"missing-topic",
		)
		require.Equal(t, expected, err)
		require.NotErrorIs(t, err, merr.ErrMqTopicNotFound)
	})

	t.Run("fatal topic error is unavailable", func(t *testing.T) {
		err := convertKafkaReadError(
			kafka.NewError(kafka.ErrUnknownTopicOrPart, "unknown topic", true),
			"missing-topic",
		)
		require.ErrorIs(t, err, merr.ErrMqTopicNotFound)
	})

	t.Run("unrelated error", func(t *testing.T) {
		expected := errors.New("unrelated error")
		require.Same(t, expected, convertKafkaReadError(expected, "topic"))
	})
}

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

	t.Run("transient topic metadata error remains recoverable", func(t *testing.T) {
		expected := kafka.NewError(kafka.ErrUnknownTopicOrPart, "metadata is propagating", false)
		err := validateKafkaTopicMetadata(&kafka.Metadata{
			Topics: map[string]kafka.TopicMetadata{
				"existing-topic": {
					Topic: "existing-topic",
					Error: expected,
				},
			},
		}, "existing-topic")
		require.Equal(t, expected, err)
		require.NotErrorIs(t, err, merr.ErrMqTopicNotFound)
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
