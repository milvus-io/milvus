package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

func TestCompareIndexParams(t *testing.T) {
	cases := []*struct {
		param1 []*commonpb.KeyValuePair
		param2 []*commonpb.KeyValuePair
		result bool
	}{
		{param1: nil, param2: nil, result: true},
		{param1: nil, param2: []*commonpb.KeyValuePair{}, result: false},
		{param1: []*commonpb.KeyValuePair{}, param2: []*commonpb.KeyValuePair{}, result: true},
		{param1: []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, param2: []*commonpb.KeyValuePair{}, result: false},
		{param1: []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, param2: []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, result: true},
	}

	for _, testcase := range cases {
		assert.EqualValues(t, testcase.result, CompareIndexParams(testcase.param1, testcase.param2))
	}
}
