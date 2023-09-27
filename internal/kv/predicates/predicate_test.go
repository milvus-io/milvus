package predicates

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type PredicateSuite struct {
	suite.Suite
}

func (s *PredicateSuite) TestValueEqual() {
	p := ValueEqual("key", "value")
	s.Equal("key", p.Key())
	s.Equal("value", p.TargetValue())
	s.Equal(PredTargetValue, p.Target())
	s.Equal(PredTypeEqual, p.Type())
	s.True(p.IsTrue("value"))
	s.False(p.IsTrue("not_value"))
	s.True(p.IsTrue([]byte("value")))
	s.False(p.IsTrue(1))
}

func (s *PredicateSuite) TestPredicateValue() {
	s.True(predicateValue(PredTypeEqual, 1, 1))
	s.False(predicateValue(PredTypeEqual, 1, 2))
	s.False(predicateValue(0, 1, 1))
}

func TestPredicates(t *testing.T) {
	suite.Run(t, new(PredicateSuite))
}
