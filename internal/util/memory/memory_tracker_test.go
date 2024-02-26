package memory

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type GoMemoryTrackerTestSuite struct {
	suite.Suite
}

func (s *GoMemoryTrackerTestSuite) TestConsume() {
	tracker := NewGoMemoryTracker("test", nil)
	tracker.Consume(10)
	s.EqualValues(10, tracker.TotalConsumed())
	tracker.Return(10)
	s.EqualValues(0, tracker.TotalConsumed())
}

func (s *GoMemoryTrackerTestSuite) TestChild() {
	tracker := NewGoMemoryTracker("test", nil)
	child := tracker.CreateChild("test-child")
	child.Consume(10)
	s.EqualValues(10, tracker.TotalConsumed())
	child.Return(5)
	s.EqualValues(5, tracker.TotalConsumed())
	child.Drop()
	s.EqualValues(0, tracker.TotalConsumed())
}

func (s *GoMemoryTrackerTestSuite) TestLimit() {
	var exceed bool
	tracker := NewGoMemoryTrackerWithLimit("test", nil, 10, func(MemoryTracker) {
		exceed = true
	})
	tracker.Consume(5)
	tracker.Consume(6)
	s.True(exceed)
}

func TestGoMemoryTracherSuite(t *testing.T) {
	suite.Run(t, new(GoMemoryTrackerTestSuite))
}
