package datacoord

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/datacoord/session"
)

func TestCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(CompactionTaskSuite))
}

type CompactionTaskSuite struct {
	suite.Suite

	mockMeta    *MockCompactionMeta
	mockSessMgr *session.MockDataNodeManager
}

func (s *CompactionTaskSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockSessMgr = session.NewMockDataNodeManager(s.T())
}
