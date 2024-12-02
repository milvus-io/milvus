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

package storage

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type DeleteLogSuite struct {
	suite.Suite
}

func (s *DeleteLogSuite) TestParse() {
	type testCase struct {
		tag       string
		input     string
		expectErr bool
		expectID  any // int64 or string
		expectTs  uint64
	}

	cases := []testCase{
		{
			tag:      "normal_int64",
			input:    `{"pkType":5,"ts":1000,"pk":100}`,
			expectID: int64(100),
			expectTs: 1000,
		},
		{
			tag:      "normal_varchar",
			input:    `{"pkType":21,"ts":1000,"pk":"100"}`,
			expectID: "100",
			expectTs: 1000,
		},
		{
			tag:      "legacy_format",
			input:    `100,1000`,
			expectID: int64(100),
			expectTs: 1000,
		},
		{
			tag:       "bad_format",
			input:     "abc",
			expectErr: true,
		},
		{
			tag:       "bad_legacy_id",
			input:     "abc,100",
			expectErr: true,
		},
		{
			tag:       "bad_legacy_ts",
			input:     "100,timestamp",
			expectErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			dl := &DeleteLog{}
			err := dl.Parse((tc.input))
			if tc.expectErr {
				s.Error(err)
				return
			}

			s.NoError(err)
			s.EqualValues(tc.expectID, dl.Pk.GetValue())
			s.Equal(tc.expectTs, dl.Ts)
		})
	}
}

func (s *DeleteLogSuite) TestUnmarshalJSON() {
	type testCase struct {
		tag       string
		input     string
		expectErr bool
		expectID  any // int64 or string
		expectTs  uint64
	}

	cases := []testCase{
		{
			tag:      "normal_int64",
			input:    `{"pkType":5,"ts":1000,"pk":100}`,
			expectID: int64(100),
			expectTs: 1000,
		},
		{
			tag:      "normal_varchar",
			input:    `{"pkType":21,"ts":1000,"pk":"100"}`,
			expectID: "100",
			expectTs: 1000,
		},
		{
			tag:       "bad_format",
			input:     "abc",
			expectErr: true,
		},
		{
			tag:       "bad_pk_type",
			input:     `{"pkType":"unknown","ts":1000,"pk":100}`,
			expectErr: true,
		},
		{
			tag:       "bad_id_type",
			input:     `{"pkType":5,"ts":1000,"pk":"abc"}`,
			expectErr: true,
		},
		{
			tag:       "bad_ts_type",
			input:     `{"pkType":5,"ts":{},"pk":100}`,
			expectErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			dl := &DeleteLog{}
			err := dl.UnmarshalJSON([]byte(tc.input))
			if tc.expectErr {
				s.Error(err)
				return
			}

			s.NoError(err)
			s.EqualValues(tc.expectID, dl.Pk.GetValue())
			s.Equal(tc.expectTs, dl.Ts)
		})
	}
}

func TestDeleteLog(t *testing.T) {
	suite.Run(t, new(DeleteLogSuite))
}

type DeltaDataSuite struct {
	suite.Suite
}

func (s *DeltaDataSuite) TestCreateWithPkType() {
	s.Run("int_pks", func() {
		dd, err := NewDeltaDataWithPkType(10, schemapb.DataType_Int64)
		s.Require().NoError(err)

		s.Equal(schemapb.DataType_Int64, dd.PkType())
		intPks, ok := dd.DeletePks().(*Int64PrimaryKeys)
		s.Require().True(ok)
		s.EqualValues(10, cap(intPks.values))
	})

	s.Run("varchar_pks", func() {
		dd, err := NewDeltaDataWithPkType(10, schemapb.DataType_VarChar)
		s.Require().NoError(err)

		s.Equal(schemapb.DataType_VarChar, dd.PkType())
		intPks, ok := dd.DeletePks().(*VarcharPrimaryKeys)
		s.Require().True(ok)
		s.EqualValues(10, cap(intPks.values))
	})

	s.Run("unsupport_pk_type", func() {
		_, err := NewDeltaDataWithPkType(10, schemapb.DataType_Bool)
		s.Error(err)
	})
}

func (s *DeltaDataSuite) TestAppend() {
	s.Run("normal_same_type", func() {
		s.Run("int64_pk", func() {
			dd, err := NewDeltaDataWithPkType(10, schemapb.DataType_Int64)
			s.Require().NoError(err)

			err = dd.Append(NewInt64PrimaryKey(100), 100)
			s.NoError(err)

			err = dd.Append(NewInt64PrimaryKey(200), 200)
			s.NoError(err)

			s.EqualValues(2, dd.DeleteRowCount())
			s.Equal([]Timestamp{100, 200}, dd.DeleteTimestamps())
			intPks, ok := dd.DeletePks().(*Int64PrimaryKeys)
			s.Require().True(ok)
			s.Equal([]int64{100, 200}, intPks.values)
			s.EqualValues(32, dd.MemSize())
		})

		s.Run("varchar_pk", func() {
			dd, err := NewDeltaDataWithPkType(10, schemapb.DataType_VarChar)
			s.Require().NoError(err)

			err = dd.Append(NewVarCharPrimaryKey("100"), 100)
			s.NoError(err)

			err = dd.Append(NewVarCharPrimaryKey("200"), 200)
			s.NoError(err)

			s.EqualValues(2, dd.DeleteRowCount())
			s.Equal([]Timestamp{100, 200}, dd.DeleteTimestamps())
			varcharPks, ok := dd.DeletePks().(*VarcharPrimaryKeys)
			s.Require().True(ok)
			s.Equal([]string{"100", "200"}, varcharPks.values)
			s.EqualValues(54, dd.MemSize())
		})
	})

	s.Run("deduct_pk_type", func() {
		s.Run("int64_pk", func() {
			dd := NewDeltaData(10)

			err := dd.Append(NewInt64PrimaryKey(100), 100)
			s.NoError(err)
			s.Equal(schemapb.DataType_Int64, dd.PkType())

			err = dd.Append(NewInt64PrimaryKey(200), 200)
			s.NoError(err)
			s.Equal(schemapb.DataType_Int64, dd.PkType())

			s.EqualValues(2, dd.DeleteRowCount())
			s.Equal([]Timestamp{100, 200}, dd.DeleteTimestamps())
			intPks, ok := dd.DeletePks().(*Int64PrimaryKeys)
			s.Require().True(ok)
			s.Equal([]int64{100, 200}, intPks.values)
		})

		s.Run("varchar_pk", func() {
			dd := NewDeltaData(10)

			err := dd.Append(NewVarCharPrimaryKey("100"), 100)
			s.NoError(err)
			s.Equal(schemapb.DataType_VarChar, dd.PkType())

			err = dd.Append(NewVarCharPrimaryKey("200"), 200)
			s.NoError(err)
			s.Equal(schemapb.DataType_VarChar, dd.PkType())

			s.EqualValues(2, dd.DeleteRowCount())
			s.Equal([]Timestamp{100, 200}, dd.DeleteTimestamps())
			varcharPks, ok := dd.DeletePks().(*VarcharPrimaryKeys)
			s.Require().True(ok)
			s.Equal([]string{"100", "200"}, varcharPks.values)
		})
	})

	// expect errors
	s.Run("mixed_pk_type", func() {
		s.Run("intpks_append_varchar", func() {
			dd, err := NewDeltaDataWithPkType(10, schemapb.DataType_Int64)
			s.Require().NoError(err)

			err = dd.Append(NewVarCharPrimaryKey("100"), 100)
			s.Error(err)
		})

		s.Run("varcharpks_append_int", func() {
			dd, err := NewDeltaDataWithPkType(10, schemapb.DataType_VarChar)
			s.Require().NoError(err)

			err = dd.Append(NewInt64PrimaryKey(100), 100)
			s.Error(err)
		})
	})
}

func TestDeltaData(t *testing.T) {
	suite.Run(t, new(DeltaDataSuite))
}
