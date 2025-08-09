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

package milvusclient

import (
	"reflect"
	"runtime/debug"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/row"
)

// ResultSet is struct for search result set.
type ResultSet struct {
	// internal schema for unmarshaling
	sch *entity.Schema

	ResultCount  int // the returning entry count
	GroupByValue column.Column
	IDs          column.Column // auto generated id, can be mapped to the columns from `Insert` API
	Fields       DataSet       // output field data
	Scores       []float32     // distance to the target vector
	Recall       float32       // recall of the query vector's search result (estimated by zilliz cloud)
	Err          error         // search error if any
}

// GetColumn returns column with provided field name.
func (rs *ResultSet) GetColumn(fieldName string) column.Column {
	for _, column := range rs.Fields {
		if column.Name() == fieldName {
			return column
		}
	}
	return nil
}

func (rs ResultSet) Len() int {
	return rs.ResultCount
}

func (rs ResultSet) Slice(start, end int) ResultSet {
	result := ResultSet{
		sch: rs.sch,
		IDs: rs.IDs.Slice(start, end),
		Fields: lo.Map(rs.Fields, func(column column.Column, _ int) column.Column {
			return column.Slice(start, end)
		}),
		// Recall will not be sliced
		Err: rs.Err,
	}

	if rs.GroupByValue != nil {
		result.GroupByValue = rs.GroupByValue.Slice(start, end)
	}

	result.ResultCount = result.IDs.Len()
	result.Scores = rs.Scores[start : start+result.ResultCount]

	return result
}

// Unmarshal puts dataset into receiver in row based way.
// `receiver` shall be a slice of pointer of model struct
// eg, []*Records, in which type `Record` defines the row data.
// note that distance/score is not unmarshaled here.
func (sr *ResultSet) Unmarshal(receiver any) (err error) {
	err = sr.Fields.Unmarshal(receiver)
	if err != nil {
		return err
	}
	return sr.fillPKEntry(receiver)
}

func (sr *ResultSet) fillPKEntry(receiver any) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = errors.Newf("failed to unmarshal result set: %v, stack: %v", x, string(debug.Stack()))
		}
	}()
	rr := reflect.ValueOf(receiver)

	if rr.Kind() == reflect.Ptr {
		if rr.IsNil() && rr.CanAddr() {
			rr.Set(reflect.New(rr.Type().Elem()))
		}
		rr = rr.Elem()
	}

	rt := rr.Type()
	rv := rr

	switch rt.Kind() {
	case reflect.Slice:
		pkField := sr.sch.PKField()

		et := rt.Elem()
		for et.Kind() == reflect.Ptr {
			et = et.Elem()
		}

		candidates := row.ParseCandidate(et)
		candi, ok := candidates[pkField.Name]
		if !ok {
			// pk field not found in struct, skip
			return nil
		}
		for i := 0; i < sr.IDs.Len(); i++ {
			row := rv.Index(i)
			for row.Kind() == reflect.Ptr {
				row = row.Elem()
			}

			val, err := sr.IDs.Get(i)
			if err != nil {
				return err
			}
			row.Field(candi).Set(reflect.ValueOf(val))
		}
		rr.Set(rv)
	default:
		return errors.Newf("receiver need to be slice or array but get %v", rt.Kind())
	}
	return nil
}

// DataSet is an alias type for column slice.
// Returned by query API.
type DataSet []column.Column

// Len returns the row count of dataset.
// if there is no column, it shall return 0.
func (ds DataSet) Len() int {
	if len(ds) == 0 {
		return 0
	}
	return ds[0].Len()
}

// Unmarshal puts dataset into receiver in row based way.
// `receiver` shall be a slice of pointer of model struct
// eg, []*Records, in which type `Record` defines the row data.
func (ds DataSet) Unmarshal(receiver any) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = errors.Newf("failed to unmarshal result set: %v, stack: %v", x, string(debug.Stack()))
		}
	}()
	rr := reflect.ValueOf(receiver)

	if rr.Kind() == reflect.Ptr {
		if rr.IsNil() && rr.CanAddr() {
			rr.Set(reflect.New(rr.Type().Elem()))
		}
		rr = rr.Elem()
	}

	rt := rr.Type()
	rv := rr

	switch rt.Kind() {
	// TODO maybe support Array and just fill data
	// case reflect.Array:
	case reflect.Slice:
		et := rt.Elem()
		if et.Kind() != reflect.Ptr {
			return errors.Newf("receiver must be slice of pointers but get: %v", et.Kind())
		}
		for et.Kind() == reflect.Ptr {
			et = et.Elem()
		}
		for i := 0; i < ds.Len(); i++ {
			data := reflect.New(et)
			err := ds.fillData(data.Elem(), et, i)
			if err != nil {
				return err
			}
			rv = reflect.Append(rv, data)
		}
		rr.Set(rv)
	default:
		return errors.Newf("receiver need to be slice or array but get %v", rt.Kind())
	}
	return nil
}

func (ds DataSet) fillData(data reflect.Value, dataType reflect.Type, idx int) error {
	m := row.ParseCandidate(dataType)
	for i := 0; i < len(ds); i++ {
		name := ds[i].Name()
		fidx, ok := m[name]
		if !ok {
			// if target is not found, the behavior here is to ignore the column
			// `strict` mode could be added in the future to return error if any column missing
			continue
		}
		val, err := ds[i].Get(idx)
		if err != nil {
			return err
		}
		// TODO check datatype, return error here instead of reflect panicking & recover
		data.Field(fidx).Set(reflect.ValueOf(val))
	}
	return nil
}
