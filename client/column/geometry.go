package column

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type ColumnGeometryBytes struct {
	*genericColumnBase[[]byte]
}

// Name returns column name.
func (c *ColumnGeometryBytes) Name() string {
	return c.name
}

// Type returns column entity.FieldType.
func (c *ColumnGeometryBytes) Type() entity.FieldType {
	return entity.FieldTypeGeometry
}

// Len returns column values length.
func (c *ColumnGeometryBytes) Len() int {
	return len(c.values)
}

func (c *ColumnGeometryBytes) Slice(start, end int) Column {
	l := c.Len()
	if start > l {
		start = l
	}
	if end == -1 || end > l {
		end = l
	}
	return &ColumnGeometryBytes{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

// Get returns value at index as interface{}.
func (c *ColumnGeometryBytes) Get(idx int) (interface{}, error) {
	if idx < 0 || idx > c.Len() {
		return nil, errors.New("index out of range")
	}
	return c.values[idx], nil
}

func (c *ColumnGeometryBytes) GetAsString(idx int) (string, error) {
	bs, err := c.ValueByIdx(idx)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

// FieldData return column data mapped to schemapb.FieldData.
func (c *ColumnGeometryBytes) FieldData() *schemapb.FieldData {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Geometry,
		FieldName: c.name,
	}

	fd.Field = &schemapb.FieldData_Scalars{
		Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_GeometryData{
				GeometryData: &schemapb.GeometryArray{
					Data: c.values,
				},
			},
		},
	}

	return fd
}

// ValueByIdx returns value of the provided index.
func (c *ColumnGeometryBytes) ValueByIdx(idx int) ([]byte, error) {
	if idx < 0 || idx >= c.Len() {
		return nil, errors.New("index out of range")
	}
	return c.values[idx], nil
}

// AppendValue append value into column.
func (c *ColumnGeometryBytes) AppendValue(i interface{}) error {
	var v []byte
	switch raw := i.(type) {
	case []byte:
		v = raw
	case string:
		v = []byte(raw)
	default:
		return fmt.Errorf("expect geometry compatible type([]byte, struct, map), got %T", i)
	}
	c.values = append(c.values, v)

	return nil
}

// Data returns column data.
func (c *ColumnGeometryBytes) Data() [][]byte {
	return c.values
}

func NewColumnGeometryBytes(name string, values [][]byte) *ColumnGeometryBytes {
	return &ColumnGeometryBytes{
		genericColumnBase: &genericColumnBase[[]byte]{
			name:      name,
			fieldType: entity.FieldTypeGeometry,
			values:    values,
		},
	}
}
