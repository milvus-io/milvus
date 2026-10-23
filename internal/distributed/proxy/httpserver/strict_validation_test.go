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

package httpserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func setStrictValidationForTest(t *testing.T, enabled bool) {
	t.Helper()
	paramtable.Init()
	cfg := paramtable.Get()
	key := cfg.HTTPCfg.StrictValidation.Key
	previous := cfg.HTTPCfg.StrictValidation.GetValue()
	value := "false"
	if enabled {
		value = "true"
	}
	require.NoError(t, cfg.Save(key, value))
	t.Cleanup(func() { require.NoError(t, cfg.Save(key, previous)) })
}

func TestStrictValidationDefaultsOff(t *testing.T) {
	paramtable.Init()
	require.Equal(t, "false", paramtable.Get().HTTPCfg.StrictValidation.DefaultValue)
}

func TestStrictValidationWriteAndReadPolicy(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "legacy"
		if enabled {
			name = "strict"
		}
		t.Run(name, func(t *testing.T) {
			setStrictValidationForTest(t, enabled)
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{Name: "small", DataType: schemapb.DataType_Int8},
				{Name: "text", DataType: schemapb.DataType_VarChar},
				{Name: "doc", DataType: schemapb.DataType_JSON},
			}}
			rows, _, err := checkAndSetData([]byte(`{"data":[{"id":1,"small":128,"text":1.50,"doc":"hello"}]}`), schema, false)
			if enabled {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, int8(-128), rows[0]["small"])
				require.Equal(t, "1.5", rows[0]["text"])
				require.Equal(t, []byte("hello"), rows[0]["doc"])
			}
			rows, _, err = checkAndSetData([]byte(`{"data":[{"id":1,"small":1,"text":1.50,"doc":"hello"}]}`), schema, false)
			require.NoError(t, err)
			if enabled {
				require.Equal(t, "1.50", rows[0]["text"])
				require.Equal(t, []byte(`"hello"`), rows[0]["doc"])
			}
			// Native JSON cannot bypass the master switch.
			cfg := paramtable.Get()
			key := cfg.HTTPCfg.NativeJSONResponse.Key
			previous := cfg.HTTPCfg.NativeJSONResponse.GetValue()
			require.NoError(t, cfg.Save(key, "true"))
			t.Cleanup(func() { cfg.Save(key, previous) })
			fields := []*schemapb.FieldData{{FieldName: "doc", Type: schemapb.DataType_JSON, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"a":1}`)}}}}}}}
			result, err := buildQueryResp(1, []string{"doc"}, fields, nil, nil, true, nil)
			require.NoError(t, err)
			if enabled {
				require.IsType(t, json.RawMessage{}, result[0]["doc"])
			} else {
				require.Equal(t, `{"a":1}`, result[0]["doc"])
			}
		})
	}
}

func TestStrictValidationDecodeAndTemplatePolicy(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		setStrictValidationForTest(t, enabled)
		var query SearchReq
		err := json.Unmarshal([]byte(`{"vector":[1,null]}`), &query)
		if enabled {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, FloatVectorQuery{1, 0}, query.Vector)
		}
		var ids Int64ListQuery
		err = json.Unmarshal([]byte(`null`), &ids)
		if enabled {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Nil(t, ids)
		}
		require.Equal(t, enabled, rejectNullInFieldPayload(schemapb.DataType_FloatVector, "v", json.RawMessage(`[1,null]`)) != nil)
		values, err := generateExpressionTemplate(map[string]json.RawMessage{"n": json.RawMessage(`9007199254740993`)})
		require.NoError(t, err)
		if enabled {
			require.Equal(t, int64(9007199254740993), values["n"].GetInt64Val())
		} else {
			require.Equal(t, int64(9007199254740992), values["n"].GetInt64Val())
		}
		var req QueryReqV2
		err = json.Unmarshal([]byte(`{"exprParams":{"n":1e400}}`), &req)
		// The outer JSON decoder rejects infinity in either mode.
		require.Error(t, err)
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}
		filter, values, err := checkGetPrimaryKey(schema, gjson.Parse(`1`))
		require.NoError(t, err)
		if enabled {
			require.NotEmpty(t, values)
		} else {
			require.Nil(t, values)
			require.Equal(t, "id in [1]", filter)
		}
	}
}

func TestDisabledValidationUsesOriginalRequestDecoder(t *testing.T) {
	setStrictValidationForTest(t, false)
	for _, raw := range []string{`null`, `[null]`, `[1,2]`, `[1.5]`, `[9223372036854775808]`} {
		var old struct {
			IDs []int64 `json:"ids"`
		}
		var current struct {
			IDs Int64ListQuery `json:"ids"`
		}
		body := []byte(`{"ids":` + raw + `}`)
		oldErr := legacyJSONUnmarshal(body, &old)
		newErr := legacyJSONUnmarshal(body, &current)
		require.Equal(t, oldErr == nil, newErr == nil, raw)
		if oldErr == nil {
			require.Equal(t, old.IDs, []int64(current.IDs), raw)
		}
	}
}
