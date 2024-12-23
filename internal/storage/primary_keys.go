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
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// PrimaryKeys is the interface holding a slice of PrimaryKey
type PrimaryKeys interface {
	Append(pks ...PrimaryKey) error
	MustAppend(pks ...PrimaryKey)
	Get(idx int) PrimaryKey
	Type() schemapb.DataType
	Size() int64
	Len() int
	MustMerge(pks PrimaryKeys)
}

type Int64PrimaryKeys struct {
	values []int64
}

func NewInt64PrimaryKeys(cap int64) *Int64PrimaryKeys {
	return &Int64PrimaryKeys{values: make([]int64, 0, cap)}
}

func (pks *Int64PrimaryKeys) AppendRaw(values ...int64) {
	pks.values = append(pks.values, values...)
}

func (pks *Int64PrimaryKeys) Append(values ...PrimaryKey) error {
	iValues := make([]int64, 0, len(values))
	for _, pk := range values {
		iPk, ok := pk.(*Int64PrimaryKey)
		if !ok {
			return merr.WrapErrParameterInvalid("Int64PrimaryKey", "non-int64 pk")
		}
		iValues = append(iValues, iPk.Value)
	}

	pks.AppendRaw(iValues...)
	return nil
}

func (pks *Int64PrimaryKeys) MustAppend(values ...PrimaryKey) {
	err := pks.Append(values...)
	if err != nil {
		panic(err)
	}
}

func (pks *Int64PrimaryKeys) Get(idx int) PrimaryKey {
	return NewInt64PrimaryKey(pks.values[idx])
}

func (pks *Int64PrimaryKeys) GetValues() []int64 {
	return pks.values
}

func (pks *Int64PrimaryKeys) Type() schemapb.DataType {
	return schemapb.DataType_Int64
}

func (pks *Int64PrimaryKeys) Len() int {
	return len(pks.values)
}

func (pks *Int64PrimaryKeys) Size() int64 {
	return int64(pks.Len()) * 8
}

func (pks *Int64PrimaryKeys) MustMerge(another PrimaryKeys) {
	aPks, ok := another.(*Int64PrimaryKeys)
	if !ok {
		panic("cannot merge different kind of pks")
	}

	pks.values = append(pks.values, aPks.values...)
}

type VarcharPrimaryKeys struct {
	values []string
	size   int64
}

func NewVarcharPrimaryKeys(cap int64) *VarcharPrimaryKeys {
	return &VarcharPrimaryKeys{
		values: make([]string, 0, cap),
	}
}

func (pks *VarcharPrimaryKeys) AppendRaw(values ...string) {
	pks.values = append(pks.values, values...)
	lo.ForEach(values, func(str string, _ int) {
		pks.size += int64(len(str)) + 16
	})
}

func (pks *VarcharPrimaryKeys) Append(values ...PrimaryKey) error {
	sValues := make([]string, 0, len(values))
	for _, pk := range values {
		iPk, ok := pk.(*VarCharPrimaryKey)
		if !ok {
			return merr.WrapErrParameterInvalid("Int64PrimaryKey", "non-int64 pk")
		}
		sValues = append(sValues, iPk.Value)
	}

	pks.AppendRaw(sValues...)
	return nil
}

func (pks *VarcharPrimaryKeys) MustAppend(values ...PrimaryKey) {
	err := pks.Append(values...)
	if err != nil {
		panic(err)
	}
}

func (pks *VarcharPrimaryKeys) Get(idx int) PrimaryKey {
	return NewVarCharPrimaryKey(pks.values[idx])
}

func (pks *VarcharPrimaryKeys) GetValues() []string {
	return pks.values
}

func (pks *VarcharPrimaryKeys) Type() schemapb.DataType {
	return schemapb.DataType_VarChar
}

func (pks *VarcharPrimaryKeys) Len() int {
	return len(pks.values)
}

func (pks *VarcharPrimaryKeys) Size() int64 {
	return pks.size
}

func (pks *VarcharPrimaryKeys) MustMerge(another PrimaryKeys) {
	aPks, ok := another.(*VarcharPrimaryKeys)
	if !ok {
		panic("cannot merge different kind of pks")
	}

	pks.values = append(pks.values, aPks.values...)
	pks.size += aPks.size
}
