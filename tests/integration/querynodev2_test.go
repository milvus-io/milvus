package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type QueryNodeV2Suite struct {
	suite.Suite
	c *MiniClusterV2
}

func (s *QueryNodeV2Suite) SetupSuite() {
	ctx := context.Background()
	var err error

	s.c, err = StartMiniClusterV2(ctx)
	s.Require().NoError(err)

	time.Sleep(time.Second)
}

func (s *QueryNodeV2Suite) TearDownSuite() {
	if s.c != nil {
		err := s.c.Stop()
		s.NoError(err)
	}
}

func TestQueryNodeV2(t *testing.T) {
	suite.Run(t, new(QueryNodeV2Suite))
}
