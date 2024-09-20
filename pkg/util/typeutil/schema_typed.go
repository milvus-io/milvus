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

package typeutil

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

type GenericSchemaSlice[T any] interface {
	Append(T)
	Get(int) T
}

type int64PkSlice struct {
	data *schemapb.IDs
}

func (s *int64PkSlice) Append(v int64) {
	s.data.GetIntId().Data = append(s.data.GetIntId().Data, v)
}

func (s *int64PkSlice) Get(i int) int64 {
	return s.data.GetIntId().Data[i]
}

type stringPkSlice struct {
	data *schemapb.IDs
}

func (s *stringPkSlice) Append(v string) {
	s.data.GetStrId().Data = append(s.data.GetStrId().Data, v)
}

func (s *stringPkSlice) Get(i int) string {
	return s.data.GetStrId().Data[i]
}

func NewInt64PkSchemaSlice(data *schemapb.IDs) GenericSchemaSlice[int64] {
	return &int64PkSlice{
		data: data,
	}
}

func NewStringPkSchemaSlice(data *schemapb.IDs) GenericSchemaSlice[string] {
	return &stringPkSlice{
		data: data,
	}
}

func CopyPk(dst *schemapb.IDs, src *schemapb.IDs, offset int) {
	switch dst.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		v := src.GetIntId().Data[offset]
		dst.GetIntId().Data = append(dst.GetIntId().Data, v)
	case *schemapb.IDs_StrId:
		v := src.GetStrId().Data[offset]
		dst.GetStrId().Data = append(dst.GetStrId().Data, v)
	}
}
