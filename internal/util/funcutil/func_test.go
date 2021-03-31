package funcutil

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckPortAvailable(t *testing.T) {
	num := 10

	for i := 0; i < num; i++ {
		port := GetAvailablePort()
		assert.Equal(t, CheckPortAvailable(port), true)
	}
}

func TestParseIndexParamsMap(t *testing.T) {
	num := 10
	keys := make([]string, 0)
	values := make([]string, 0)
	params := make(map[string]string)

	for i := 0; i < num; i++ {
		keys = append(keys, "key"+strconv.Itoa(i))
		values = append(values, "value"+strconv.Itoa(i))
		params[keys[i]] = values[i]
	}

	paramsBytes, err := json.Marshal(params)
	assert.Equal(t, err, nil)
	paramsStr := string(paramsBytes)

	parsedParams, err := ParseIndexParamsMap(paramsStr)
	assert.Equal(t, err, nil)
	assert.Equal(t, parsedParams, params)

	invalidStr := "invalid string"
	_, err = ParseIndexParamsMap(invalidStr)
	assert.NotEqual(t, err, nil)
}
