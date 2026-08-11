package kafka

import (
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
	t.Run("topic not found", func(t *testing.T) {
		err := convertKafkaReadError(
			kafka.NewError(kafka.ErrUnknownTopicOrPart, "unknown topic", false),
			"missing-topic",
		)
		require.ErrorIs(t, err, merr.ErrMqTopicNotFound)
	})

	t.Run("unrelated error", func(t *testing.T) {
		expected := errors.New("unrelated error")
		require.Same(t, expected, convertKafkaReadError(expected, "topic"))
	})
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
