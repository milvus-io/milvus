// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package column

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ColumnDynamicSuite struct {
	suite.Suite
}

func (s *ColumnDynamicSuite) TestSlicePreservesDynamicColumn() {
	column := NewColumnDynamic(NewColumnJSONBytes("", [][]byte{
		[]byte(`{"field": 1}`),
		[]byte(`{"field": 2}`),
	}), "field")

	sliced := column.Slice(1, -1)
	dynamic, ok := sliced.(*ColumnDynamic)
	s.Require().True(ok)
	s.Equal("field", dynamic.Name())
	value, err := dynamic.GetAsInt64(0)
	s.Require().NoError(err)
	s.EqualValues(2, value)
}

func (s *ColumnDynamicSuite) TestSliceColumnsSharesDynamicJSONBase() {
	base := NewColumnJSONBytes("$meta", [][]byte{
		[]byte(`{"color": "red", "price": 10}`),
		[]byte(`{"color": "blue", "price": 20}`),
	}).WithIsDynamic(true)
	color := NewColumnDynamic(base, "color")
	price := NewColumnDynamic(base, "price")

	sliced := SliceColumns([]Column{base, color, price}, 1, -1)
	s.Require().Len(sliced, 3)
	slicedBase, ok := sliced[0].(*ColumnJSONBytes)
	s.Require().True(ok)
	slicedColor, ok := sliced[1].(*ColumnDynamic)
	s.Require().True(ok)
	slicedPrice, ok := sliced[2].(*ColumnDynamic)
	s.Require().True(ok)

	s.Same(slicedBase, slicedColor.ColumnJSONBytes)
	s.Same(slicedBase, slicedPrice.ColumnJSONBytes)
	s.True(slicedBase.FieldData().GetIsDynamic())
	colorValue, err := slicedColor.GetAsString(0)
	s.Require().NoError(err)
	s.Equal("blue", colorValue)
	priceValue, err := slicedPrice.GetAsInt64(0)
	s.Require().NoError(err)
	s.EqualValues(20, priceValue)
}

func (s *ColumnDynamicSuite) TestGetInt() {
	cases := []struct {
		input       string
		expectErr   bool
		expectValue int64
	}{
		{`{"field": 1000000000000000001}`, false, 1000000000000000001},
		{`{"field": 4418489049307132905}`, false, 4418489049307132905},
		{`{"other_field": 4418489049307132905}`, true, 0},
		{`{"field": "string"}`, true, 0},
	}

	for _, c := range cases {
		s.Run(c.input, func() {
			column := NewColumnDynamic(NewColumnJSONBytes("", [][]byte{[]byte(c.input)}), "field")
			v, err := column.GetAsInt64(0)
			if c.expectErr {
				s.Error(err)
				return
			}
			s.NoError(err)
			s.Equal(c.expectValue, v)
		})
	}
}

func (s *ColumnDynamicSuite) TestGetString() {
	cases := []struct {
		input       string
		expectErr   bool
		expectValue string
	}{
		{`{"field": "abc"}`, false, "abc"},
		{`{"field": "test"}`, false, "test"},
		{`{"other_field": "string"}`, true, ""},
		{`{"field": 123}`, true, ""},
	}

	for _, c := range cases {
		s.Run(c.input, func() {
			// column := NewColumnDynamic(&ColumnJSONBytes{
			// 	values: [][]byte{[]byte(c.input)},
			// }, "field")
			column := NewColumnDynamic(NewColumnJSONBytes("", [][]byte{[]byte(c.input)}), "field")
			v, err := column.GetAsString(0)
			if c.expectErr {
				s.Error(err)
				return
			}
			s.NoError(err)
			s.Equal(c.expectValue, v)
		})
	}
}

func (s *ColumnDynamicSuite) TestGetBool() {
	cases := []struct {
		input       string
		expectErr   bool
		expectValue bool
	}{
		{`{"field": true}`, false, true},
		{`{"field": false}`, false, false},
		{`{"other_field": true}`, true, false},
		{`{"field": "test"}`, true, false},
	}

	for _, c := range cases {
		s.Run(c.input, func() {
			// column := NewColumnDynamic(&ColumnJSONBytes{
			// 	values: [][]byte{[]byte(c.input)},
			// }, "field")
			column := NewColumnDynamic(NewColumnJSONBytes("", [][]byte{[]byte(c.input)}), "field")
			v, err := column.GetAsBool(0)
			if c.expectErr {
				s.Error(err)
				return
			}
			s.NoError(err)
			s.Equal(c.expectValue, v)
		})
	}
}

func (s *ColumnDynamicSuite) TestGetDouble() {
	cases := []struct {
		input       string
		expectErr   bool
		expectValue float64
	}{
		{`{"field": 1}`, false, 1.0},
		{`{"field": 6231.123}`, false, 6231.123},
		{`{"other_field": 1.0}`, true, 0},
		{`{"field": "string"}`, true, 0},
	}

	for _, c := range cases {
		s.Run(c.input, func() {
			// column := NewColumnDynamic(&ColumnJSONBytes{
			// 	values: [][]byte{[]byte(c.input)},
			// }, "field")
			column := NewColumnDynamic(NewColumnJSONBytes("", [][]byte{[]byte(c.input)}), "field")
			v, err := column.GetAsDouble(0)
			if c.expectErr {
				s.Error(err)
				return
			}
			s.NoError(err)
			s.Less(v-c.expectValue, 1e-10)
		})
	}
}

func (s *ColumnDynamicSuite) TestIndexOutOfRange() {
	var err error
	column := NewColumnDynamic(NewColumnJSONBytes("", nil), "field")

	s.Equal("field", column.Name())

	_, err = column.GetAsInt64(0)
	s.Error(err)

	_, err = column.GetAsString(0)
	s.Error(err)

	_, err = column.GetAsBool(0)
	s.Error(err)

	_, err = column.GetAsDouble(0)
	s.Error(err)
}

func TestColumnDynamic(t *testing.T) {
	suite.Run(t, new(ColumnDynamicSuite))
}
