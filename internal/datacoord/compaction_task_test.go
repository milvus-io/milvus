package datacoord

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(CompactionTaskSuite))
}

type CompactionTaskSuite struct {
	suite.Suite

	mockMeta    *MockCompactionMeta
	mockSessMgr *MockSessionManager
}

func (s *CompactionTaskSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockSessMgr = NewMockSessionManager(s.T())
}
