package autoindex

import (
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/sandertv/go-formula/v2"
	"github.com/stretchr/testify/assert"
)

func TestAutoIndexUtil_getInt64FromParams(t *testing.T) {
	var params []*commonpb.KeyValuePair
	params = append(params, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "10",
	})

	t.Run("getInt64FromParams1", func(t *testing.T) {
		topK, err := getInt64FromParams(params, TopKKey)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), topK)
	})

	t.Run("getInt64FromParams2", func(t *testing.T) {
		topK, err := getInt64FromParams(params, "")
		assert.Error(t, err)
		assert.Equal(t, int64(0), topK)
	})

	t.Run("getInt64FromParams3", func(t *testing.T) {
		var params []*commonpb.KeyValuePair
		params = append(params, &commonpb.KeyValuePair{
			Key:   TopKKey,
			Value: "x",
		})

		topK, err := getInt64FromParams(params, TopKKey)
		assert.Error(t, err)
		assert.Equal(t, int64(0), topK)
	})
}

func TestAutoIndexUtil_parseAssignment(t *testing.T) {

	t.Run("stmt1", func(t *testing.T) {
		stmt := "__output = 1 *pow(__input * 1.1, 4) + __input * 2 + 2"
		input, output, expr, err := parseAssignment(stmt)
		assert.NoError(t, err)
		assert.Equal(t, "__output", output)
		assert.Equal(t, "__input", input)
		f, err := formula.New(expr)
		assert.NotNil(t, f)
		assert.NoError(t, err)
	})

	t.Run("stmt2", func(t *testing.T) {
		stmt := "__output 1 *pox(__input * 1.1, 4) + __input * 2 + 2"
		input, output, expr, err := parseAssignment(stmt)
		assert.Error(t, err)
		assert.Equal(t, "", output)
		assert.Equal(t, "", input)
		assert.Equal(t, "", expr)
		f, err := formula.New(expr)
		assert.Nil(t, f)
		assert.Error(t, err)
	})

	t.Run("stmt3", func(t *testing.T) {
		stmt := "_output = 1 *pox(__input * 1.1, 4) + __input * 2 + 2"
		input, output, expr, err := parseAssignment(stmt)
		assert.Error(t, err)
		assert.Equal(t, "", output)
		assert.Equal(t, "", input)
		assert.Equal(t, "", expr)
		f, err := formula.New(expr)
		assert.Nil(t, f)
		assert.Error(t, err)
	})

	t.Run("stmt4", func(t *testing.T) {
		stmt := "__output = 1 *pox(_topk * 1.1, 4) + __input * 2 + 2"
		input, output, expr, err := parseAssignment(stmt)
		assert.NoError(t, err)
		assert.Equal(t, "__output", output)
		assert.Equal(t, "__input", input)
		f, err := formula.New(expr)
		assert.NotNil(t, f)
		assert.NoError(t, err)
		inputVar := formula.Var(input, 1)
		_, err = f.Eval(inputVar)
		assert.Error(t, err)
	})

	t.Run("stmt5", func(t *testing.T) {
		stmt := "__output = 1 *pox(_topK * 1.1, 4) + _topK1 * 2 + 2"
		input, output, expr, err := parseAssignment(stmt)
		assert.Error(t, err)
		assert.Equal(t, "", output)
		assert.Equal(t, "", input)
		assert.Equal(t, "", expr)
		f, err := formula.New(expr)
		assert.Nil(t, f)
		assert.Error(t, err)
	})

}
