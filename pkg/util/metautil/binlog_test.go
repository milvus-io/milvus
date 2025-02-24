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

package metautil

import (
	"reflect"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestParseInsertLogPath(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name             string
		args             args
		wantCollectionID typeutil.UniqueID
		wantPartitionID  typeutil.UniqueID
		wantSegmentID    typeutil.UniqueID
		wantFieldID      typeutil.UniqueID
		wantLogID        typeutil.UniqueID
		wantOk           bool
	}{
		{
			"test parse insert log path",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/446266956600703326/447985737531772787/102/447985737523710526"},
			446266956600703270,
			446266956600703326,
			447985737531772787,
			102,
			447985737523710526,
			true,
		},

		{
			"test parse insert log path negative1",
			args{path: "foobar"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative2",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/446266956600703326/447985737531772787/102/foo"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative3",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/446266956600703326/447985737531772787/foo/447985737523710526"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative4",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/446266956600703326/foo/102/447985737523710526"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative5",
			args{path: "8a8c3ac2298b12f/insert_log/446266956600703270/foo/447985737531772787/102/447985737523710526"},
			0,
			0,
			0,
			0,
			0,
			false,
		},

		{
			"test parse insert log path negative6",
			args{path: "8a8c3ac2298b12f/insert_log/foo/446266956600703326/447985737531772787/102/447985737523710526"},
			0,
			0,
			0,
			0,
			0,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCollectionID, gotPartitionID, gotSegmentID, gotFieldID, gotLogID, gotOk := ParseInsertLogPath(tt.args.path)
			if !reflect.DeepEqual(gotCollectionID, tt.wantCollectionID) {
				t.Errorf("ParseInsertLogPath() gotCollectionID = %v, want %v", gotCollectionID, tt.wantCollectionID)
			}
			if !reflect.DeepEqual(gotPartitionID, tt.wantPartitionID) {
				t.Errorf("ParseInsertLogPath() gotPartitionID = %v, want %v", gotPartitionID, tt.wantPartitionID)
			}
			if !reflect.DeepEqual(gotSegmentID, tt.wantSegmentID) {
				t.Errorf("ParseInsertLogPath() gotSegmentID = %v, want %v", gotSegmentID, tt.wantSegmentID)
			}
			if !reflect.DeepEqual(gotFieldID, tt.wantFieldID) {
				t.Errorf("ParseInsertLogPath() gotFieldID = %v, want %v", gotFieldID, tt.wantFieldID)
			}
			if !reflect.DeepEqual(gotLogID, tt.wantLogID) {
				t.Errorf("ParseInsertLogPath() gotLogID = %v, want %v", gotLogID, tt.wantLogID)
			}
			if gotOk != tt.wantOk {
				t.Errorf("ParseInsertLogPath() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}
