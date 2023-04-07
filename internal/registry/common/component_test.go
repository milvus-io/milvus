package common

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/stretchr/testify/suite"
)

type ComponentSuite struct {
	suite.Suite
}

func (s *ComponentSuite) TestIsCoordinator() {
	type testCase struct {
		componentName string
		expected      bool
	}

	cases := []testCase{
		{typeutil.RootCoordRole, true},
		{typeutil.QueryCoordRole, true},
		{typeutil.IndexCoordRole, true},
		{typeutil.DataCoordRole, true},
		{typeutil.ProxyRole, false},
		{typeutil.QueryNodeRole, false},
		{typeutil.DataNodeRole, false},
		{typeutil.IndexNodeRole, false},
		{typeutil.EmbeddedRole, false},
		{typeutil.StandaloneRole, false},
		{"others", false},
	}

	for _, tc := range cases {
		s.Run(tc.componentName, func() {
			s.Equal(tc.expected, IsCoordinator(tc.componentName))
		})
	}
}

func (s *ComponentSuite) TestIsNode() {
	type testCase struct {
		componentName string
		expected      bool
	}

	cases := []testCase{
		{typeutil.RootCoordRole, false},
		{typeutil.QueryCoordRole, false},
		{typeutil.IndexCoordRole, false},
		{typeutil.DataCoordRole, false},
		{typeutil.ProxyRole, true},
		{typeutil.QueryNodeRole, true},
		{typeutil.DataNodeRole, true},
		{typeutil.IndexNodeRole, true},
		{typeutil.EmbeddedRole, false},
		{typeutil.StandaloneRole, false},
		{"others", false},
	}

	for _, tc := range cases {
		s.Run(tc.componentName, func() {
			s.Equal(tc.expected, IsNode(tc.componentName))
		})
	}
}

func TestComponent(t *testing.T) {
	suite.Run(t, new(ComponentSuite))
}
