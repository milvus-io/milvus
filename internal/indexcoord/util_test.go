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

package indexcoord

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
)

func Test_getDimension(t *testing.T) {
	req := &indexpb.BuildIndexRequest{
		IndexBuildID: UniqueID(0),
		IndexID:      UniqueID(1),
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
		},
	}
	dim, err := getDimension(req)
	assert.Equal(t, int64(128), dim)
	assert.Nil(t, err)

	req2 := &indexpb.BuildIndexRequest{
		IndexBuildID: UniqueID(0),
		IndexID:      UniqueID(1),
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "one",
			},
		},
	}
	dim, err = getDimension(req2)
	assert.Error(t, err)
	assert.Equal(t, int64(0), dim)

	req3 := &indexpb.BuildIndexRequest{
		IndexBuildID: UniqueID(0),
		IndexID:      UniqueID(1),
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "TypeParam-Key-1",
				Value: "TypeParam-Value-1",
			},
		},
	}
	dim, err = getDimension(req3)
	assert.Error(t, err)
	assert.Equal(t, int64(0), dim)
}

func Test_estimateIndexSize(t *testing.T) {
	memorySize, err := estimateIndexSize(10, 100, schemapb.DataType_FloatVector)
	assert.Nil(t, err)
	assert.Equal(t, uint64(4000), memorySize)

	memorySize, err = estimateIndexSize(16, 100, schemapb.DataType_BinaryVector)
	assert.Nil(t, err)
	assert.Equal(t, uint64(200), memorySize)

	memorySize, err = estimateIndexSize(10, 100, schemapb.DataType_Float)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), memorySize)
	// assert.Error(t, err)
	// assert.Equal(t, uint64(0), memorySize)
}

func Test_estimateScalarIndexSize(t *testing.T) {
	type args struct {
		req *indexpb.BuildIndexRequest
	}
	tests := []struct {
		name    string
		args    args
		want    uint64
		wantErr bool
	}{
		{
			args: args{
				req: &indexpb.BuildIndexRequest{},
			},
			want:    0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := estimateScalarIndexSize(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("estimateScalarIndexSize(%v) error = %v, wantErr %v", tt.args.req, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("estimateScalarIndexSize(%v) got = %v, want %v", tt.args.req, got, tt.want)
			}
		})
	}
}

func Test_estimateIndexSizeByReq(t *testing.T) {
	type args struct {
		req *indexpb.BuildIndexRequest
	}
	tests := []struct {
		name    string
		args    args
		want    uint64
		wantErr bool
	}{
		{
			// scalar.
			args: args{
				req: &indexpb.BuildIndexRequest{
					FieldSchema: &schemapb.FieldSchema{
						DataType: schemapb.DataType_VarChar,
					},
				},
			},
			want:    0,
			wantErr: false,
		},
		{
			// vector without dim.
			args: args{
				req: &indexpb.BuildIndexRequest{
					FieldSchema: &schemapb.FieldSchema{
						DataType: schemapb.DataType_FloatVector,
					},
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			// float vector.
			args: args{
				req: &indexpb.BuildIndexRequest{
					FieldSchema: &schemapb.FieldSchema{
						DataType: schemapb.DataType_FloatVector,
					},
					NumRows: 1,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "8",
						},
					},
				},
			},
			want:    32,
			wantErr: false,
		},
		{
			// binary vector.
			args: args{
				req: &indexpb.BuildIndexRequest{
					FieldSchema: &schemapb.FieldSchema{
						DataType: schemapb.DataType_BinaryVector,
					},
					NumRows: 1,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "8",
						},
					},
				},
			},
			want:    1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := estimateIndexSizeByReq(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("estimateIndexSizeByReq(%v) error = %v, wantErr %v", tt.args.req, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("estimateIndexSizeByReq(%v) got = %v, want %v", tt.args.req, got, tt.want)
			}
		})
	}
}
