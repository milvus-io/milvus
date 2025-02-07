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
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

func appendValueAt(builder array.Builder, a arrow.Array, idx int) error {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		ba, ok := a.(*array.Boolean)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(ba.Value(idx))
		return nil
	case *array.Int8Builder:
		ia, ok := a.(*array.Int8)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(ia.Value(idx))
		return nil
	case *array.Int16Builder:
		ia, ok := a.(*array.Int16)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(ia.Value(idx))
		return nil
	case *array.Int32Builder:
		ia, ok := a.(*array.Int32)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(ia.Value(idx))
		return nil
	case *array.Int64Builder:
		ia, ok := a.(*array.Int64)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(ia.Value(idx))
		return nil
	case *array.Float32Builder:
		fa, ok := a.(*array.Float32)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(fa.Value(idx))
		return nil
	case *array.Float64Builder:
		fa, ok := a.(*array.Float64)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(fa.Value(idx))
		return nil
	case *array.StringBuilder:
		sa, ok := a.(*array.String)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(sa.Value(idx))
		return nil
	case *array.FixedSizeBinaryBuilder:
		ba, ok := a.(*array.FixedSizeBinary)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(ba.Value(idx))
		return nil
	case *array.BinaryBuilder:
		ba, ok := a.(*array.Binary)
		if !ok {
			return fmt.Errorf("invalid value type %T, expect %T", a.DataType(), builder.Type())
		}
		b.Append(ba.Value(idx))
		return nil
	default:
		return fmt.Errorf("unknown builder type %T", builder)
	}
}
