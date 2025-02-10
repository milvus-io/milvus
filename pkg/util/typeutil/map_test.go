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

func (suite *MapUtilSuite) TestConcurrentMap() {
	currMap := NewConcurrentMap[int64, string]()

	suite.Equal(currMap.Len(), 0)

	v, loaded := currMap.GetOrInsert(100, "v-100")
	suite.Equal("v-100", v)
	suite.Equal(false, loaded)
	v, loaded = currMap.GetOrInsert(100, "v-100")
	suite.Equal("v-100", v)
	suite.Equal(true, loaded)
	v, loaded = currMap.GetOrInsert(100, "v-100")
	suite.Equal("v-100", v)
	suite.Equal(true, loaded)

	suite.Equal(currMap.Len(), 1)

	currMap.GetOrInsert(100, "v-100-new")
	currMap.GetOrInsert(200, "v-200")
	currMap.GetOrInsert(300, "v-300")

	suite.ElementsMatch([]int64{100, 200, 300}, currMap.Keys())
	suite.ElementsMatch([]string{"v-100", "v-200", "v-300"}, currMap.Values())

	var exist bool
	v, exist = currMap.Get(100)
	suite.Equal("v-100", v)
	suite.Equal(true, exist)
	v, exist = currMap.Get(200)
	suite.Equal("v-200", v)
	suite.Equal(true, exist)
	v, exist = currMap.Get(300)
	suite.Equal("v-300", v)
	suite.Equal(true, exist)

	v, exist = currMap.GetOrInsert(100, "new-v")
	suite.Equal("v-100", v)
	suite.Equal(true, exist)
	v, exist = currMap.GetOrInsert(200, "new-v")
	suite.Equal("v-200", v)
	suite.Equal(true, exist)
	v, exist = currMap.GetOrInsert(300, "new-v")
	suite.Equal("v-300", v)
	suite.Equal(true, exist)
	v, exist = currMap.GetOrInsert(400, "new-v")
	suite.Equal("new-v", v)
	suite.Equal(false, exist)

	currMap.Range(func(k int64, value string) bool {
		suite.Contains([]int64{100, 200, 300, 400}, k)
		expect, _ := currMap.Get(k)
		suite.Equal(expect, value)

		return true
	})

	v, loaded = currMap.GetAndRemove(100)
	suite.Equal("v-100", v)
	suite.Equal(true, loaded)
	v, loaded = currMap.GetAndRemove(200)
	suite.Equal("v-200", v)
	suite.Equal(true, loaded)
	v, loaded = currMap.GetAndRemove(300)
	suite.Equal("v-300", v)
	suite.Equal(true, loaded)
	v, loaded = currMap.GetAndRemove(400)
	suite.Equal("new-v", v)
	suite.Equal(true, loaded)
	v, loaded = currMap.GetAndRemove(500)
	suite.Equal("", v)
	suite.Equal(false, loaded)

	currMap.Range(func(k int64, value string) bool {
		suite.FailNow("empty map range")
		return false
	})

	suite.Run("TestRemove", func() {
		currMap := NewConcurrentMap[int64, string]()
		suite.Equal(0, currMap.Len())

		currMap.Remove(100)
		suite.Equal(0, currMap.Len())

		suite.Equal(currMap.Len(), 0)
		v, loaded := currMap.GetOrInsert(100, "v-100")
		suite.Equal("v-100", v)
		suite.Equal(false, loaded)
		suite.Equal(1, currMap.Len())

		currMap.Remove(100)
		suite.Equal(0, currMap.Len())
	})
}

func TestMapUtil(t *testing.T) {
	suite.Run(t, new(MapUtilSuite))
}
