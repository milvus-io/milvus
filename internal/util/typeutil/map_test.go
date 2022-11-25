package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

func (suite *MapUtilSuite) TestConcurrentMap() {
	currMap := NewConcurrentMap[int64, string]()

	assert.EqualValues(suite.T(), 0, currMap.Len())
	v, loaded := currMap.GetOrInsert(100, "v-100")
	assert.Equal(suite.T(), "v-100", v)
	assert.Equal(suite.T(), false, loaded)
	v, loaded = currMap.GetOrInsert(100, "v-100")
	assert.Equal(suite.T(), "v-100", v)
	assert.Equal(suite.T(), true, loaded)
	v, loaded = currMap.GetOrInsert(100, "v-100")
	assert.Equal(suite.T(), "v-100", v)
	assert.Equal(suite.T(), true, loaded)
	assert.Equal(suite.T(), uint64(1), currMap.Len())
	assert.EqualValues(suite.T(), 1, currMap.Len())

	currMap.InsertIfNotPresent(100, "v-100-new")
	currMap.InsertIfNotPresent(200, "v-200")
	currMap.InsertIfNotPresent(300, "v-300")
	assert.Equal(suite.T(), uint64(3), currMap.Len())
	assert.EqualValues(suite.T(), 3, currMap.Len())

	var exist bool
	v, exist = currMap.Get(100)
	assert.Equal(suite.T(), "v-100", v)
	assert.Equal(suite.T(), true, exist)
	v, exist = currMap.Get(200)
	assert.Equal(suite.T(), "v-200", v)
	assert.Equal(suite.T(), true, exist)
	v, exist = currMap.Get(300)
	assert.Equal(suite.T(), "v-300", v)
	assert.Equal(suite.T(), true, exist)
	assert.EqualValues(suite.T(), 3, currMap.Len())

	v, exist = currMap.GetOrInsert(100, "new-v")
	assert.Equal(suite.T(), "v-100", v)
	assert.Equal(suite.T(), true, exist)
	v, exist = currMap.GetOrInsert(200, "new-v")
	assert.Equal(suite.T(), "v-200", v)
	assert.Equal(suite.T(), true, exist)
	v, exist = currMap.GetOrInsert(300, "new-v")
	assert.Equal(suite.T(), "v-300", v)
	assert.Equal(suite.T(), true, exist)
	v, exist = currMap.GetOrInsert(400, "new-v")
	assert.Equal(suite.T(), "new-v", v)
	assert.Equal(suite.T(), false, exist)
	assert.EqualValues(suite.T(), 4, currMap.Len())

	v, loaded = currMap.GetAndRemove(100)
	assert.Equal(suite.T(), "v-100", v)
	assert.Equal(suite.T(), true, loaded)
	v, loaded = currMap.GetAndRemove(200)
	assert.Equal(suite.T(), "v-200", v)
	assert.Equal(suite.T(), true, loaded)
	v, loaded = currMap.GetAndRemove(300)
	assert.Equal(suite.T(), "v-300", v)
	assert.Equal(suite.T(), true, loaded)
	v, loaded = currMap.GetAndRemove(400)
	assert.Equal(suite.T(), "new-v", v)
	assert.Equal(suite.T(), true, loaded)
	v, loaded = currMap.GetAndRemove(500)
	assert.Equal(suite.T(), "", v)
	assert.Equal(suite.T(), false, loaded)
	assert.EqualValues(suite.T(), 0, currMap.Len())
}

func TestMapUtil(t *testing.T) {
	suite.Run(t, new(MapUtilSuite))
}
