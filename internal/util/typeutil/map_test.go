package typeutil

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MapUtilSuite struct {
	suite.Suite
}

func (suite *MapUtilSuite) TestMapEqual() {
	left := map[int64]int64{
		1: 10,
		2: 20,
	}
	right := map[int64]int64{
		1: 10,
		2: 200,
	}
	suite.False(MapEqual(left, right))
	suite.False(MapEqual(right, left))

	right[2] = 20
	suite.True(MapEqual(left, right))
	suite.True(MapEqual(right, left))

	left[3] = 30
	suite.False(MapEqual(left, right))
	suite.False(MapEqual(right, left))
}

func (suite *MapUtilSuite) TestMergeMap() {
	src := make(map[string]string)
	src["Alice"] = "female"
	src["Bob"] = "male"
	dst := make(map[string]string)
	dst = MergeMap(src, dst)
	suite.EqualValues(dst, src)

	src = nil
	dst = nil
	dst = MergeMap(src, dst)
	suite.Nil(dst)
}

func (suite *MapUtilSuite) TestGetMapKeys() {
	src := make(map[string]string)
	src["Alice"] = "female"
	src["Bob"] = "male"
	keys := GetMapKeys(src)
	suite.Equal(2, len(keys))
	suite.Contains(keys, "Alice", "Bob")
}

func TestMapUtil(t *testing.T) {
	suite.Run(t, new(MapUtilSuite))
}
