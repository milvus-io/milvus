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
          "function": "__output = 3*__input  + 4"
        },
        "2": {
          "bp": [10, 200],
          "functions": [
		"__output = __input  + 4",
		"__output = 3*__input  + 4",
		"__output = pow(__input, 2) + 4"
          ]
        },
        "3": {
          "bp": [10, 300],
          "functions": [
		"__output = __input  + 4",
		"__output = 2*__input  + 3",
		"__output = pow(__input, 1.2) + 4"
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
				"func": "{"
			},
			"2": x
          	 }`,
		`{
			"1":  {
          		"function": "{\"funcID\": 3, \"cof1\": 3,\"cof2\": 4,\"cof3\": 5}"
			},
			"2": x
          	}`,
		`{"": 1}`,
		`{"": 1, "func": ""}`,
		`{"": 1, "function": ""}`,
		`{1, "function": "xxx"}`,
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
          "function": "__output = __input"
        },
        "2": {
		"bp": [10, 50],
		"functions": [
			"__output = __input",
			"__output = 3.0*pow(__input,2) + 5",
			"__output = 10 * __input + 5"
		]
        },
        "3": {
          "bp": [10, 300],
          "functions": [
		"__output = 3.0*pow(__input,2) + 5",
		"__output = 10 * __input + 5",
		"__output = __input"
          ]
        }
	}`

	var err error
	parser := NewParser()
	err = parser.InitFromJSONStr(jsonStr)
	assert.NoError(t, err)

	inputKey := "input"
	outputKey := "output"

	var params []*commonpb.KeyValuePair
	params = append(params, &commonpb.KeyValuePair{
		Key:   inputKey,
		Value: "10",
	})

	assertValueMapEqual := func(source, target map[string]interface{}) {
		expJSON, err := json.Marshal(source)
		expJSONStr := string(expJSON)
		assert.NoError(t, err)

		targetJSON, err := json.Marshal(target)
		assert.NoError(t, err)
		targetJSONStr := string(targetJSON)
		assert.Equal(t, expJSONStr, targetJSONStr)
	}

	normalTargetValues := []int64{
		10,
		int64(3*math.Pow(10, 2) + 5),
		int64(10*10 + 5),
	}
	normalLevels := []int{1, 2, 3}

	for i, l := range normalLevels {
		targetV := normalTargetValues[i]
		expMap := make(map[string]interface{})
		expMap[outputKey] = targetV

		m, exist := parser.GetMethodByLevel(l)
		assert.NotNil(t, m)
		assert.True(t, exist)
		ret, err := m.Calculate(params)
		assert.NoError(t, err)
		assertValueMapEqual(expMap, ret)
	}

	invalidLevels := []int{-1, 0, 4}

	for _, l := range invalidLevels {
		m, exist := parser.GetMethodByLevel(l)
		assert.Nil(t, m)
		assert.False(t, exist)

	}
}
