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

package segcore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMapSearchCStatusMetricTypeNotMatch(t *testing.T) {
	err := mapSearchCStatus(
		merr.SegcoreMetricTypeNotMatchCode,
		"Operator::GetOutput failed: metric type not match[expected=COSINE][actual=L2] at SegmentIndexMeta.h",
	)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.EqualValues(t, 1100, merr.Code(err))
	require.Equal(t, "metric type not match: invalid parameter[expected=COSINE][actual=L2]", err.Error())
}

func TestMapSearchCStatusMetricTypeNotMatchMalformedDetail(t *testing.T) {
	err := mapSearchCStatus(merr.SegcoreMetricTypeNotMatchCode, "metric mismatch without structured detail")
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Equal(t, "metric type not match: invalid parameter", err.Error())
}

func TestMapSearchCStatusPreservesOtherSegcoreErrors(t *testing.T) {
	err := mapSearchCStatus(2027, "field not loaded")
	require.ErrorIs(t, err, merr.ErrSegcore)
	require.True(t, merr.IsRetryableErr(err))
}
