package message

import (
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/kafka"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/nmq"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/pulsar"
)

// NewMessageIDFromPBMessageID creates a new message id from pb message id.
func NewMessageIDFromPBMessageID(msgID *logpb.MessageID) MessageID {
	switch id := msgID.Id.(type) {
	case *logpb.MessageID_Kafka:
		return kafka.NewKafkaID(id.Kafka.GetOffset())
	case *logpb.MessageID_Pulsar:
		msgID, err := pulsar.BytesToMsgID(id.Pulsar.GetSerialized())
		if err != nil {
			panic("failed to get pulsar message id")
		}
		return msgID
	case *logpb.MessageID_Rmq:
		return &server.RmqID{MessageID: id.Rmq.GetOffset()}
	case *logpb.MessageID_Nmq:
		return nmq.NewNmqID(msgID.GetNmq().GetOffset())
	default:
		panic("unknown message id type")
	}
}

func NewPBMessageIDFromMessageID(msgID MessageID) *logpb.MessageID {
	switch id := msgID.(type) {
	case interface{ KafkaID() int64 }:
		return &logpb.MessageID{
			Id: &logpb.MessageID_Kafka{
				Kafka: &logpb.MessageIDKafka{
					Offset: id.KafkaID(),
				},
			},
		}
	case interface{ NMQID() uint64 }:
		return &logpb.MessageID{
			Id: &logpb.MessageID_Nmq{
				Nmq: &logpb.MessageIDNmq{
					Offset: id.NMQID(),
				},
			},
		}
	case *server.RmqID:
		return &logpb.MessageID{
			Id: &logpb.MessageID_Rmq{
				Rmq: &logpb.MessageIDRmq{
					Offset: id.MessageID,
				},
			},
		}
	case interface{ PulsarID() []byte }:
		return &logpb.MessageID{
			Id: &logpb.MessageID_Pulsar{
				Pulsar: &logpb.MessageIDPulsar{
					Serialized: id.PulsarID(),
				},
			},
		}
	default:
		panic("unknown message id type")
	}
}

type MessageID = mqwrapper.MessageID
