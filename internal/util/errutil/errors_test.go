package errutil

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"
)

type ErrSuite struct {
	suite.Suite
}

func (s *ErrSuite) TestCombine() {
	var (
		errFirst  = errors.New("first")
		errSecond = errors.New("second")
		errThird  = errors.New("third")
	)

	err := Combine(errFirst, errSecond)
	s.True(errors.Is(err, errFirst))
	s.True(errors.Is(err, errSecond))
	s.False(errors.Is(err, errThird))

	s.Equal("first: second", err.Error())
}

func TestErrors(t *testing.T) {
	suite.Run(t, new(ErrSuite))
}
