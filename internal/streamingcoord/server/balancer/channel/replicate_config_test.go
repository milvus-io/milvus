package channel

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

type ReplicateConfigHelperSuite struct {
	suite.Suite
	helper *replicateConfigHelper
}

func TestReplicateConfigHelperSuite(t *testing.T) {
	suite.Run(t, new(ReplicateConfigHelperSuite))
}

func (s *ReplicateConfigHelperSuite) SetupTest() {
	s.helper = nil
}

func (s *ReplicateConfigHelperSuite) TestNewReplicateConfigHelper() {
	// Test nil input
	helper := newReplicateConfigHelper(nil)
	s.Nil(helper)

	// Test valid input
	meta := &streamingpb.ReplicateConfigurationMeta{
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev"},
			},
		},
		AckedResult: types.NewAckedPendings([]string{"p1", "p2"}).AckedResult,
	}
	helper = newReplicateConfigHelper(meta)
	s.NotNil(helper)
	s.NotNil(helper.ConfigHelper)
	s.NotNil(helper.ackedPendings)
	s.False(helper.dirty)
}

func (s *ReplicateConfigHelperSuite) TestStartUpdating() {
	s.helper = &replicateConfigHelper{
		ConfigHelper:  nil,
		ackedPendings: nil,
		dirty:         false,
	}

	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev"},
		},
	}
	pchannels := []string{"p1", "p2"}

	// First update should return true
	changed := s.helper.StartUpdating(config, pchannels)
	s.True(changed)
	s.NotNil(s.helper.ackedPendings)

	s.helper.Apply(config, []types.AckedCheckpoint{
		{Channel: "p1", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: 1},
		{Channel: "p2", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: 1},
	})
	s.helper.ConsumeIfDirty(config)

	// Same config should return false
	changed = s.helper.StartUpdating(config, pchannels)
	s.False(changed)
}

func (s *ReplicateConfigHelperSuite) TestApply() {
	s.helper = &replicateConfigHelper{
		ConfigHelper:  nil,
		ackedPendings: types.NewAckedPendings([]string{"p1", "p2"}),
		dirty:         false,
	}

	config := &commonpb.ReplicateConfiguration{}
	checkpoints := []types.AckedCheckpoint{
		{Channel: "p1", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: 1},
		{Channel: "p2", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: 1},
	}

	s.helper.Apply(config, checkpoints)
	s.True(s.helper.dirty)
	s.True(s.helper.ackedPendings.IsAllAcked())
}

func (s *ReplicateConfigHelperSuite) TestConsumeIfDirty() {
	s.helper = &replicateConfigHelper{
		ConfigHelper:  nil,
		ackedPendings: types.NewAckedPendings([]string{"p1", "p2"}),
		dirty:         true,
	}

	// Not all acked case
	config, tasks, dirty := s.helper.ConsumeIfDirty(&commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev"},
		},
	})
	s.NotNil(config)
	s.Nil(tasks)
	s.True(dirty)
	s.False(s.helper.dirty)

	// All acked case
	s.helper.dirty = true
	s.helper.ackedPendings.Ack(types.AckedCheckpoint{Channel: "p1", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	s.helper.ackedPendings.Ack(types.AckedCheckpoint{Channel: "p2", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})

	config, tasks, dirty = s.helper.ConsumeIfDirty(&commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev"},
		},
	})
	s.NotNil(config)
	s.NotNil(tasks)
	s.True(dirty)
	s.False(s.helper.dirty)
	s.Nil(s.helper.ackedPendings)
}
