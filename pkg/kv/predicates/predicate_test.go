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
	s.Equal(PredTargetValueEqual, p.Target())
	s.True(p.IsTrue("value"))
	s.False(p.IsTrue("not_value"))
	s.True(p.IsTrue([]byte("value")))
	s.False(p.IsTrue(1))
}

func (s *PredicateSuite) TestKeyNotExist() {
	p := KeyNotExist("key")
	s.Equal("key", p.Key())
	s.Nil(p.TargetValue())
	s.Equal(PredTargetKeyNotExist, p.Target())
	// IsTrue receives the value of a key that exists, so it never holds.
	s.False(p.IsTrue("value"))
	s.False(p.IsTrue([]byte("value")))
	s.False(p.IsTrue([]byte{}))
	s.False(p.IsTrue(nil))
}

func (s *PredicateSuite) TestKeyExist() {
	p := KeyExist("key")
	s.Equal("key", p.Key())
	s.Nil(p.TargetValue())
	s.Equal(PredTargetKeyExist, p.Target())
	// IsTrue receives the value of a key that exists, so it always holds.
	s.True(p.IsTrue("value"))
	s.True(p.IsTrue([]byte{}))
}

func (s *PredicateSuite) TestUnchangedSince() {
	p := UnchangedSince("key", 10)
	s.Equal("key", p.Key())
	s.Equal(int64(10), p.TargetValue())
	s.Equal(PredTargetUnchangedSince, p.Target())
	for _, c := range []struct {
		target any
		holds  bool
	}{
		{int64(0), true}, // missing key
		{int64(9), true},
		{int64(10), true},
		{int64(11), false},
		{10, false}, // not an int64
		{"10", false},
		{nil, false},
	} {
		s.Equal(c.holds, p.IsTrue(c.target), "%v", c.target)
	}
}

func TestPredicates(t *testing.T) {
	suite.Run(t, new(PredicateSuite))
}
