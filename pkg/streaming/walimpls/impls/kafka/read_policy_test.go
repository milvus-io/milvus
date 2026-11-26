package kafka

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
)

func TestSupportedReadPoliciesPreservePosition(t *testing.T) {
	mockey.PatchConvey("deprecating Latest preserves explicit offsets and exclusion", t, func() {
		consumer := &kafka.Consumer{}
		mockey.Mock(kafka.NewConsumer).Return(consumer, nil).Build()
		var assigned kafka.TopicPartition
		mockey.Mock((*kafka.Consumer).Assign).To(func(_ *kafka.Consumer, partitions []kafka.TopicPartition) error {
			assigned = partitions[0]
			topic := *partitions[0].Topic
			assigned.Topic = &topic
			return nil
		}).Build()
		mockey.Mock(newScanner).To(func(_ string, exclude *kafkaID, c *kafka.Consumer) *scannerImpl {
			s := &scannerImpl{consumer: c}
			if exclude != nil {
				id := *exclude
				s.exclude = &id
			}
			return s
		}).Build()
		w := &walImpl{WALHelper: helper.NewWALHelper(&walimpls.OpenOption{Channel: types.PChannelInfo{Name: "policy-test"}})}
		for _, tc := range []struct {
			policy  options.DeliverPolicy
			offset  kafka.Offset
			exclude bool
		}{
			{options.DeliverPolicyAll(), kafka.OffsetBeginning, false},
			{options.DeliverPolicyStartFrom(kafkaID(42)), 42, false},
			{options.DeliverPolicyStartAfter(kafkaID(42)), 42, true},
		} {
			reader, err := w.Read(context.Background(), walimpls.ReadOption{DeliverPolicy: tc.policy})
			require.NoError(t, err)
			require.Equal(t, tc.offset, assigned.Offset)
			require.Equal(t, "policy-test", *assigned.Topic)
			s := reader.(*scannerImpl)
			require.Equal(t, tc.exclude, s.exclude != nil)
			if tc.exclude {
				require.Equal(t, kafkaID(42), *s.exclude)
			}
		}
	})
}
