package types_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

type AckedResultTestSuite struct {
	suite.Suite
}

func TestAckedResult(t *testing.T) {
	suite.Run(t, new(AckedResultTestSuite))
}

func (s *AckedResultTestSuite) TestNewAckedPendings() {
	channels := []string{"ch1", "ch2"}
	result := types.NewAckedPendings(channels)

	s.Equal(channels, result.Channels)
	s.Equal(len(channels), len(result.AckedCheckpoints))
	for _, cp := range result.AckedCheckpoints {
		s.Nil(cp)
	}
}

func (s *AckedResultTestSuite) TestNewAckedPendingsFromProto() {
	// Test nil input
	s.Nil(types.NewAckedPendingsFromProto(nil))

	// Test valid input
	proto := &streamingpb.AckedResult{
		Channels:         []string{"ch1"},
		AckedCheckpoints: []*streamingpb.AckedCheckpoint{nil},
	}
	result := types.NewAckedPendingsFromProto(proto)
	s.Equal(proto, result.AckedResult)
}

func (s *AckedResultTestSuite) TestAck() {
	channels := []string{"ch1", "ch2"}
	result := types.NewAckedPendings(channels)

	// Test successful ack
	cp := types.AckedCheckpoint{
		Channel:                "ch1",
		MessageID:              walimplstest.NewTestMessageID(1),
		LastConfirmedMessageID: walimplstest.NewTestMessageID(0),
		TimeTick:               100,
	}
	s.True(result.Ack(cp))
	s.NotNil(result.AckedCheckpoints[0])
	s.Equal(cp.MessageID.IntoProto(), result.AckedCheckpoints[0].MessageId)
	s.Equal(cp.LastConfirmedMessageID.IntoProto(), result.AckedCheckpoints[0].LastConfirmedMessageId)
	s.Equal(cp.TimeTick, result.AckedCheckpoints[0].TimeTick)

	// Test duplicate ack
	s.False(result.Ack(cp))

	// Test panic on non-existent channel
	s.Panics(func() {
		result.Ack(types.AckedCheckpoint{Channel: "non-existent"})
	})
}

func (s *AckedResultTestSuite) TestIsAllAcked() {
	channels := []string{"ch1", "ch2"}
	result := types.NewAckedPendings(channels)

	// Test not all acked
	s.False(result.IsAllAcked())

	// Ack first channel
	result.Ack(types.AckedCheckpoint{
		Channel:                "ch1",
		MessageID:              walimplstest.NewTestMessageID(1),
		LastConfirmedMessageID: walimplstest.NewTestMessageID(0),
		TimeTick:               100,
	})
	s.False(result.IsAllAcked())

	// Ack second channel
	result.Ack(types.AckedCheckpoint{
		Channel:                "ch2",
		MessageID:              walimplstest.NewTestMessageID(2),
		LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
		TimeTick:               200,
	})
	s.True(result.IsAllAcked())
}
