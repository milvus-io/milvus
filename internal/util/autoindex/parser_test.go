package autoindex

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestAutoIndexParser_Parse(t *testing.T) {
	jsonStr := `
	{
        "1": {
          "methodID": 1,
          "function": "{\"funcID\": 3, \"cof1\": 3,\"cof2\": 4,\"cof3\": 5}"
        },
        "2": {
          "methodID": 2,
          "bp": [10, 200],
          "functions": [
            "{\"funcID\": 1}",
            "{\"funcID\": 2, \"cof1\": 4,\"cof2\": 5}",
            "{\"funcID\": 3, \"cof1\": 4,\"cof2\": 5,\"cof3\": 6}"
          ]
        },
        "3": {
          "methodID": 2,
          "bp": [10, 300],
          "functions": [
            "{\"funcID\": 2, \"cof1\": 3,\"cof2\": 4}",
            "{\"funcID\": 1}",
            "{\"funcID\": 3, \"cof1\": 4,\"cof2\": 5,\"cof3\": 6}"
          ]
        }
	}`
	parser := NewParser()
	err := parser.InitFromJSONStr(jsonStr)
	assert.NoError(t, err)

	invalid1JSONS := []string{
		`{}`,
		`{"1":xxx}`,
		`{
			"1":  {
				"methodID": 1,
          		"function": "{\"funcID\": 3, \"cof1\": 3,\"cof2\": 4,\"cof3\": 5}"
			},
			"2": x
          }`,
		`{
			"1":  {
				"methodID": x,
          		"function": "{\"funcID\": 3, \"cof1\": 3,\"cof2\": 4,\"cof3\": 5}"
			},
			"2": x
          }`,
		`{"methodID": 1}`,
		`{"methodID": 1, "func": ""}`,
		`{"methodID": 1, "function": ""}`,
		`{"methodID": 1, "function": "xxx"}`,
	}

	for _, jsonStr := range invalid1JSONS {
		err := parser.InitFromJSONStr(jsonStr)
		assert.NotNil(t, err)
	}
}

func TestAutoIndexParser_GetMethodByLevel(t *testing.T) {
	jsonStr := `
	{
        "1": {
          "methodID": 1,
          "function": "{\"funcID\": 3, \"cof1\": 3,\"cof2\": 4,\"cof3\": 5}"
        },
        "2": {
          "methodID": 2,
          "bp": [10, 200],
          "functions": [
            "{\"funcID\": 1}",
            "{\"funcID\": 2, \"cof1\": 4,\"cof2\": 5}",
            "{\"funcID\": 3, \"cof1\": 4,\"cof2\": 5,\"cof3\": 6}"
          ]
        },
        "3": {
          "methodID": 2,
          "bp": [10, 300],
          "functions": [
            "{\"funcID\": 2, \"cof1\": 3,\"cof2\": 4}",
            "{\"funcID\": 1}",
            "{\"funcID\": 3, \"cof1\": 4,\"cof2\": 5,\"cof3\": 6}"
          ]
        }
	}`

	var m Calculator
	var err error
	parser := NewParser()
	err = parser.InitFromJSONStr(jsonStr)
	assert.NoError(t, err)

	var params []*commonpb.KeyValuePair
	params = append(params, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "10",
	})

	smallLevels := []int{-1, 0, 1}
	for _, l := range smallLevels { // for level < 1 , all same to level 1
		m, err = parser.GetMethodByLevel(l)
		assert.NoError(t, err)
		assert.NotNil(t, m)
		targetV1 := int64(3*math.Pow(10, 4) + 5)
		targetMap1 := make(map[string]interface{})
		targetMap1["ef"] = targetV1
		targetJSON1, err := json.Marshal(targetMap1)
		targetJSONStr1 := string(targetJSON1)
		assert.NoError(t, err)
		ret1, err := m.Calculate(params)
		assert.NoError(t, err)
		assert.Equal(t, targetJSONStr1, ret1)
	}

	m, err = parser.GetMethodByLevel(2)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	targetV2 := int64(4*10 + 5)
	targetMap2 := make(map[string]interface{})
	targetMap2["ef"] = targetV2
	targetJSON2, err := json.Marshal(targetMap2)
	targetJSONStr2 := string(targetJSON2)
	assert.NoError(t, err)
	ret2, err := m.Calculate(params)
	assert.NoError(t, err)
	assert.Equal(t, targetJSONStr2, ret2)

	largeLevels := []int{3, 4}
	for _, l := range largeLevels { // for level >=3, all same to level 3
		m, err = parser.GetMethodByLevel(l)
		assert.NoError(t, err)
		assert.NotNil(t, m)

		targetV3 := int64(10)
		targetMap3 := make(map[string]interface{})
		targetMap3["ef"] = targetV3
		targetJSON3, err := json.Marshal(targetMap3)
		assert.NoError(t, err)
		targetJSONStr3 := string(targetJSON3)
		assert.NoError(t, err)
		ret3, err := m.Calculate(params)
		assert.NoError(t, err)
		assert.Equal(t, targetJSONStr3, ret3)
	}
}
