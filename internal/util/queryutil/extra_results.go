// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queryutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// selectExtraResults preserves the declared chain/column layout and applies
// exactly the same source row references as the ordinary fields. Row values
// are immutable and may be shared; containers are independently owned.
func selectExtraResults(results []*internalpb.RetrieveResults, refs []rowRef) (*commonpb.ResultData, error) {
	var template *commonpb.ResultData
	for _, r := range results {
		if len(r.GetExtraResults().GetFields()) > 0 {
			template = r.GetExtraResults()
			break
		}
	}
	if template == nil {
		return nil, nil
	}
	output := &commonpb.ResultData{Fields: make(map[string]*commonpb.ResultValue, len(template.Fields))}
	for name, chain := range template.Fields {
		object := chain.GetObjectValue()
		if object == nil {
			return nil, merr.WrapErrServiceInternalMsg("extra result chain %q is not an object", name)
		}
		columns := &commonpb.ResultData{Fields: make(map[string]*commonpb.ResultValue, len(object.Fields))}
		for column := range object.Fields {
			columns.Fields[column] = &commonpb.ResultValue{Value: &commonpb.ResultValue_ListValue{ListValue: &commonpb.ResultList{}}}
		}
		output.Fields[name] = &commonpb.ResultValue{Value: &commonpb.ResultValue_ObjectValue{ObjectValue: columns}}
	}
	for _, r := range results {
		extra := r.GetExtraResults()
		count := typeutil.GetSizeOfIDs(r.GetIds())
		if extra == nil && count == 0 {
			continue
		}
		if len(extra.GetFields()) != len(template.Fields) {
			return nil, merr.WrapErrServiceInternalMsg("inconsistent extra result chain names")
		}
		for name, chain := range template.Fields {
			expected := chain.GetObjectValue().Fields
			actual := extra.GetFields()[name].GetObjectValue()
			if actual == nil || len(actual.Fields) != len(expected) {
				return nil, merr.WrapErrServiceInternalMsg("inconsistent extra result columns")
			}
			for column := range expected {
				list := actual.Fields[column].GetListValue()
				if list == nil || len(list.Values) != count {
					return nil, merr.WrapErrServiceInternalMsg("extra result column %q does not align with IDs", column)
				}
				for _, value := range list.Values {
					if value == nil || value.Value == nil {
						return nil, merr.WrapErrServiceInternalMsg("unset extra result row")
					}
				}
			}
		}
	}
	for _, ref := range refs {
		if ref.resultIdx < 0 || ref.resultIdx >= len(results) || ref.rowIdx < 0 || ref.rowIdx >= int64(typeutil.GetSizeOfIDs(results[ref.resultIdx].GetIds())) {
			return nil, merr.WrapErrServiceInternalMsg("extra result source row out of range")
		}
		source := results[ref.resultIdx].GetExtraResults()
		for name, chain := range output.Fields {
			for column, value := range chain.GetObjectValue().Fields {
				value.GetListValue().Values = append(value.GetListValue().Values, source.Fields[name].GetObjectValue().Fields[column].GetListValue().Values[ref.rowIdx])
			}
		}
	}
	return output, nil
}
