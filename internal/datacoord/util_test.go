// licensed to the lf ai & data foundation under one
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

package datacoord

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

func TestVerifyResponse(t *testing.T) {
	type testCase struct {
		resp       interface{}
		err        error
		expected   error
		equalValue bool
	}
	cases := []testCase{
		{
			resp:       nil,
			err:        errors.New("boom"),
			expected:   errors.New("boom"),
			equalValue: true,
		},
		{
			resp:       nil,
			err:        nil,
			expected:   errNilResponse,
			equalValue: false,
		},
		{
			resp:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			err:        nil,
			expected:   nil,
			equalValue: false,
		},
		{
			resp:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "r1"},
			err:        nil,
			expected:   errors.New("r1"),
			equalValue: true,
		},
		{
			resp:       (*commonpb.Status)(nil),
			err:        nil,
			expected:   errNilResponse,
			equalValue: false,
		},
		{
			resp: &rootcoordpb.AllocIDResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			},
			err:        nil,
			expected:   nil,
			equalValue: false,
		},
		{
			resp: &rootcoordpb.AllocIDResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "r2"},
			},
			err:        nil,
			expected:   errors.New("r2"),
			equalValue: true,
		},
		{
			resp:       &rootcoordpb.AllocIDResponse{},
			err:        nil,
			expected:   errNilStatusResponse,
			equalValue: true,
		},
		{
			resp:       (*rootcoordpb.AllocIDResponse)(nil),
			err:        nil,
			expected:   errNilStatusResponse,
			equalValue: true,
		},
		{
			resp:       struct{}{},
			err:        nil,
			expected:   errUnknownResponseType,
			equalValue: false,
		},
	}
	for _, c := range cases {
		r := VerifyResponse(c.resp, c.err)
		if c.equalValue {
			assert.EqualValues(t, c.expected, r)
		} else {
			assert.Equal(t, c.expected, r)
		}
	}
}

func Test_getTimetravelReverseTime(t *testing.T) {
	Params.Init()
	Params.CommonCfg.RetentionDuration = 43200 // 5 days

	tFixed := time.Date(2021, 11, 15, 0, 0, 0, 0, time.Local)
	tBefore := tFixed.Add(-time.Duration(Params.CommonCfg.RetentionDuration) * time.Second)

	type args struct {
		allocator allocator
	}
	tests := []struct {
		name    string
		args    args
		want    *timetravel
		wantErr bool
	}{
		{
			"test get timetravel",
			args{&fixedTSOAllocator{fixedTime: tFixed}},
			&timetravel{tsoutil.ComposeTS(tBefore.UnixNano()/int64(time.Millisecond), 0)},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getTimetravelReverseTime(context.TODO(), tt.args.allocator)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

type fixedTSOAllocator struct {
	fixedTime time.Time
}

func (f *fixedTSOAllocator) allocTimestamp(_ context.Context) (Timestamp, error) {
	return tsoutil.ComposeTS(f.fixedTime.UnixNano()/int64(time.Millisecond), 0), nil
}

func (f *fixedTSOAllocator) allocID(_ context.Context) (UniqueID, error) {
	panic("not implemented") // TODO: Implement
}
