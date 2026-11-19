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

package indexcgowrapper

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMarshalBuildIndexInfoNormalizesHistoricalSpec(t *testing.T) {
	info := &indexcgopb.BuildIndexInfo{CollectionID: 42, ExternalSpec: `{"Format":"paimon-table","Snapshot_Id":9007199254740993,"Scan_Mode":"data-split","custom":{"n":9007199254740993}}`}
	original := proto.Clone(info)
	encoded, err := marshalBuildIndexInfo(info)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, info))
	var decoded indexcgopb.BuildIndexInfo
	require.NoError(t, proto.Unmarshal(encoded, &decoded))
	require.Equal(t, `{"custom":{"n":9007199254740993},"format":"paimon-table","scan_mode":"data-split","snapshot_id":"9007199254740993"}`, decoded.GetExternalSpec())
	decoded.ExternalSpec = info.ExternalSpec
	require.True(t, proto.Equal(info, &decoded))
}

func TestMarshalBuildIndexInfoRejectsInvalidSpec(t *testing.T) {
	_, err := marshalBuildIndexInfo(&indexcgopb.BuildIndexInfo{ExternalSpec: `{"Scan_Mode":true}`})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}
