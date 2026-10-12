package column

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
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

// Len returns the logical row count.
func (c *ColumnGeometryWKT) Len() int {
	return c.genericColumnBase.Len()
}

func (c *ColumnGeometryWKT) Slice(start, end int) Column {
	return &ColumnGeometryWKT{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

// Get returns value at index as interface{}.
func (c *ColumnGeometryWKT) Get(idx int) (interface{}, error) {
	return c.genericColumnBase.Get(idx)
}

func (c *ColumnGeometryWKT) GetAsString(idx int) (string, error) {
	return c.genericColumnBase.GetAsString(idx)
}

// FieldData return column data mapped to schemapb.FieldData.
func (c *ColumnGeometryWKT) FieldData() *schemapb.FieldData {
	fd := c.genericColumnBase.FieldData()
	return fd
}

// ValueByIdx returns value of the provided index.
func (c *ColumnGeometryWKT) ValueByIdx(idx int) (string, error) {
	return c.genericColumnBase.Value(idx)
}

// AppendValue append value into column.
func (c *ColumnGeometryWKT) AppendValue(i interface{}) error {
	return c.genericColumnBase.AppendValue(i)
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
