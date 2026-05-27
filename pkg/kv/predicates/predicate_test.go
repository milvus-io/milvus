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

func (s *PredicateSuite) TestKeyNotExists() {
	p := KeyNotExists("k")
	s.Equal("k", p.Key())
	s.Equal(PredTargetCreateRevision, p.Target())
	s.Equal(PredTypeEqual, p.Type())
	s.Equal(int64(0), p.TargetValue())
	s.True(p.IsTrue(int64(0)))
	s.False(p.IsTrue(int64(1)))
	s.False(p.IsTrue("0"))
}

func (s *PredicateSuite) TestModRevisionEqual() {
	p := ModRevisionEqual("k", 42)
	s.Equal("k", p.Key())
	s.Equal(PredTargetModRevision, p.Target())
	s.Equal(PredTypeEqual, p.Type())
	s.Equal(int64(42), p.TargetValue())
	s.True(p.IsTrue(int64(42)))
	s.False(p.IsTrue(int64(41)))
	s.False(p.IsTrue("42"))
}

func TestPredicates(t *testing.T) {
	suite.Run(t, new(PredicateSuite))
}
