package column

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type ColumnGeometryWKT struct {
	*genericColumnBase[string]
}

// Name returns column name.
func (c *ColumnGeometryWKT) Name() string {
	return c.name
}

// Type returns column entity.FieldType.
func (c *ColumnGeometryWKT) Type() entity.FieldType {
	return entity.FieldTypeGeometry
}

// Len returns column values length.
func (c *ColumnGeometryWKT) Len() int {
	return len(c.values)
}

func (c *ColumnGeometryWKT) Slice(start, end int) Column {
	l := c.Len()
	if start > l {
		start = l
	}
	if end == -1 || end > l {
		end = l
	}
	return &ColumnGeometryWKT{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

// Get returns value at index as interface{}.
func (c *ColumnGeometryWKT) Get(idx int) (interface{}, error) {
	if idx < 0 || idx >= c.Len() {
		return nil, errors.New("index out of range")
	}
	return c.values[idx], nil
}

func (c *ColumnGeometryWKT) GetAsString(idx int) (string, error) {
	return c.ValueByIdx(idx)
}

// FieldData return column data mapped to schemapb.FieldData.
func (c *ColumnGeometryWKT) FieldData() *schemapb.FieldData {
	fd := c.genericColumnBase.FieldData()
	return fd
}

// ValueByIdx returns value of the provided index.
func (c *ColumnGeometryWKT) ValueByIdx(idx int) (string, error) {
	if idx < 0 || idx >= c.Len() {
		return "", errors.New("index out of range")
	}
	return c.values[idx], nil
}

// AppendValue append value into column.
func (c *ColumnGeometryWKT) AppendValue(i interface{}) error {
	s, ok := i.(string)
	if !ok {
		return errors.New("expect geometry WKT type(string)")
	}
	c.values = append(c.values, s)
	return nil
}

// Data returns column data.
func (c *ColumnGeometryWKT) Data() []string {
	return c.values
}

func NewColumnGeometryWKT(name string, values []string) *ColumnGeometryWKT {
	return &ColumnGeometryWKT{
		genericColumnBase: &genericColumnBase[string]{
			name:      name,
			fieldType: entity.FieldTypeGeometry,
			values:    values,
		},
	}
}
