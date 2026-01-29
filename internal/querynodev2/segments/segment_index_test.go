package segments

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

type SegmentIndexStatusSuite struct {
	suite.Suite
}

func (s *SegmentIndexStatusSuite) TestBaseSegmentIndexStatus() {
	// Create a simple baseSegment with index status
	bs := baseSegment{
		indexStatus: atomic.NewInt32(int32(IndexStatusUnindexed)),
	}

	// Test GetIndexStatus
	status := bs.GetIndexStatus()
	s.Equal(IndexStatusUnindexed, status)

	// Test SetIndexStatus
	bs.SetIndexStatus(IndexStatusIndexed)
	status = bs.GetIndexStatus()
	s.Equal(IndexStatusIndexed, status)

	// Test CompareAndSetIndexStatus - success
	success := bs.CompareAndSetIndexStatus(IndexStatusIndexed, IndexStatusUnindexed)
	s.True(success)
	status = bs.GetIndexStatus()
	s.Equal(IndexStatusUnindexed, status)

	// Test CompareAndSetIndexStatus - failure
	success = bs.CompareAndSetIndexStatus(IndexStatusIndexed, IndexStatusIndexed)
	s.False(success)
	status = bs.GetIndexStatus()
	s.Equal(IndexStatusUnindexed, status) // Should remain unchanged
}

func (s *SegmentIndexStatusSuite) TestIndexStatusDefaultValue() {
	// Test that default value is unindexed
	bs := baseSegment{
		indexStatus: atomic.NewInt32(int32(IndexStatusUnindexed)),
	}

	// Verify default index status is unindexed
	status := bs.GetIndexStatus()
	s.Equal(IndexStatusUnindexed, status)

	// Test initialization with indexed
	bs2 := baseSegment{
		indexStatus: atomic.NewInt32(int32(IndexStatusIndexed)),
	}
	status2 := bs2.GetIndexStatus()
	s.Equal(IndexStatusIndexed, status2)
}

func (s *SegmentIndexStatusSuite) TestConcurrentIndexStatusOperations() {
	bs := baseSegment{
		indexStatus: atomic.NewInt32(int32(IndexStatusUnindexed)),
	}

	// Simulate concurrent operations
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 100; i++ {
			bs.SetIndexStatus(IndexStatusIndexed)
			bs.SetIndexStatus(IndexStatusUnindexed)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			bs.CompareAndSetIndexStatus(IndexStatusUnindexed, IndexStatusIndexed)
			bs.CompareAndSetIndexStatus(IndexStatusIndexed, IndexStatusUnindexed)
		}
		done <- true
	}()

	<-done
	<-done

	// Just verify it doesn't crash and returns a valid status
	status := bs.GetIndexStatus()
	s.True(status == IndexStatusIndexed || status == IndexStatusUnindexed)
}

func TestSegmentIndexStatusSuite(t *testing.T) {
	suite.Run(t, new(SegmentIndexStatusSuite))
}
