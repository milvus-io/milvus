package etcdkv

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
)

type EtcdKVUtilSuite struct {
	suite.Suite
}

func (s *EtcdKVUtilSuite) TestParsePredicateType() {
	type testCase struct {
		tag           string
		pt            predicates.PredicateType
		expectResult  string
		expectSucceed bool
	}

	cases := []testCase{
		{tag: "equal", pt: predicates.PredTypeEqual, expectResult: "=", expectSucceed: true},
		{tag: "zero_value", pt: 0, expectResult: "", expectSucceed: false},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			result, err := parsePredicateType(tc.pt)
			if tc.expectSucceed {
				s.NoError(err)
				s.Equal(tc.expectResult, result)
			} else {
				s.Error(err)
			}
		})
	}
}

func (s *EtcdKVUtilSuite) TestParsePredicates() {
	type testCase struct {
		tag           string
		input         []predicates.Predicate
		expectSucceed bool
	}

	badPredicate := predicates.NewMockPredicate(s.T())
	badPredicate.EXPECT().Target().Return(0)

	cases := []testCase{
		{tag: "normal_value_equal", input: []predicates.Predicate{predicates.ValueEqual("a", "b")}, expectSucceed: true},
		{tag: "empty_input", input: nil, expectSucceed: true},
		{tag: "bad_predicates", input: []predicates.Predicate{badPredicate}, expectSucceed: false},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			result, err := parsePredicates("", tc.input...)
			if tc.expectSucceed {
				s.NoError(err)
				s.Equal(len(tc.input), len(result))
			} else {
				s.Error(err)
			}
		})
	}
}

func TestEtcdKVUtil(t *testing.T) {
	suite.Run(t, new(EtcdKVUtilSuite))
}
