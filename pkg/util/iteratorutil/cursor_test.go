// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iteratorutil

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPrimaryKeyCursorPrecisionAndLiteralStrings(t *testing.T) {
	for _, value := range []string{"-9223372036854775808", "9007199254740993", "9223372036854775807"} {
		pk, err := ParsePrimaryKey(Int64PKType, value)
		require.NoError(t, err)
		require.Equal(t, value, strconv.FormatInt(pk.GetInt64Val(), 10))
	}
	for _, value := range []string{"", "a\"b\\c\n雪", "\x00"} {
		pk, err := ParsePrimaryKey(VarCharPKType, value)
		require.NoError(t, err)
		require.Equal(t, value, pk.GetStringVal())
	}
	for _, value := range []string{"9223372036854775808", "1.0", "", "1e3"} {
		_, err := ParsePrimaryKey(Int64PKType, value)
		require.Error(t, err)
	}
}

func TestCursorValidationPreservesLegacyAndRejectsPartialPKState(t *testing.T) {
	score := float32(0.25)
	intPK, err := ParsePrimaryKey(Int64PKType, "9007199254740993")
	require.NoError(t, err)
	stringPK, err := ParsePrimaryKey(VarCharPKType, "")
	require.NoError(t, err)
	require.NoError(t, ValidateCursor(&planpb.SearchIteratorV2Info{LastBound: &score}, schemapb.DataType_Int64))
	require.NoError(t, ValidateCursor(&planpb.SearchIteratorV2Info{CursorVersion: PKCursorVersion}, schemapb.DataType_Int64))
	require.NoError(t, ValidateCursor(&planpb.SearchIteratorV2Info{CursorVersion: PKCursorVersion, LastBound: &score, LastPk: intPK}, schemapb.DataType_Int64))
	require.NoError(t, ValidateCursor(&planpb.SearchIteratorV2Info{CursorVersion: PKCursorVersion, LastBound: &score, LastPk: stringPK}, schemapb.DataType_VarChar))
	for _, info := range []*planpb.SearchIteratorV2Info{
		{CursorVersion: 3},
		{CursorVersion: PKCursorVersion, LastBound: &score},
		{CursorVersion: PKCursorVersion, LastPk: intPK},
		{CursorVersion: PKCursorVersion, LastBound: &score, LastPk: stringPK},
	} {
		require.Error(t, ValidateCursor(info, schemapb.DataType_Int64))
	}
	for _, invalid := range []float32{float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN())} {
		require.Error(t, ValidateCursor(&planpb.SearchIteratorV2Info{CursorVersion: PKCursorVersion, LastBound: &invalid, LastPk: intPK}, schemapb.DataType_Int64))
	}
}

func TestMixedWorkersCannotAdvertisePKCursor(t *testing.T) {
	newWorker := &internalpb.SearchResults{Status: merr.Success()}
	MarkPKCursor(newWorker.Status)
	oldWorker := &internalpb.SearchResults{Status: merr.Success()}
	failedWorker := &internalpb.SearchResults{Status: &commonpb.Status{Code: 5, ExtraInfo: map[string]string{CursorVersionKey: PKCursorVersionString}}}
	for _, workers := range [][]*internalpb.SearchResults{nil, {newWorker, oldWorker}, {newWorker, failedWorker}, {newWorker, nil}} {
		output := &internalpb.SearchResults{Status: &commonpb.Status{ExtraInfo: map[string]string{CursorVersionKey: PKCursorVersionString, LastPKKey: "123", "cost": "7"}}}
		PropagatePKCursor(workers, output)
		require.NotContains(t, output.Status.ExtraInfo, CursorVersionKey)
		require.NotContains(t, output.Status.ExtraInfo, LastPKKey)
		require.Equal(t, "7", output.Status.ExtraInfo["cost"])
	}
	output := &internalpb.SearchResults{Status: &commonpb.Status{ExtraInfo: map[string]string{"cost": "7"}}}
	PropagatePKCursor([]*internalpb.SearchResults{newWorker, newWorker}, output)
	require.Equal(t, PKCursorVersionString, output.Status.ExtraInfo[CursorVersionKey])
	require.Equal(t, "7", output.Status.ExtraInfo["cost"])
}
