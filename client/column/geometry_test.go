package column

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v2/entity"
)

type ColumnGeometryBytesSuite struct {
	suite.Suite
}

func (s *ColumnGeometryBytesSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *ColumnGeometryBytesSuite) TestAttrMethods() {
	columnName := fmt.Sprintf("column_Geometrybs_%d", rand.Int())
	columnLen := 8 + rand.Intn(10)

	v := make([][]byte, columnLen)
	column := NewColumnGeometryBytes(columnName, v)

	s.Run("test_meta", func() {
		ft := entity.FieldTypeGeometry
		s.Equal("Geometry", ft.Name())
		s.Equal("Geometry", ft.String())
		pbName, pbType := ft.PbFieldType()
		s.Equal("Geometry", pbName)
		s.Equal("Geometry", pbType)
	})

	s.Run("test_column_attribute", func() {
		s.Equal(columnName, column.Name())
		s.Equal(entity.FieldTypeGeometry, column.Type())
		s.Equal(columnLen, column.Len())
		s.EqualValues(v, column.Data())
	})

	s.Run("test_column_field_data", func() {
		fd := column.FieldData()
		s.NotNil(fd)
		s.Equal(fd.GetFieldName(), columnName)
	})

	s.Run("test_column_valuer_by_idx", func() {
		_, err := column.ValueByIdx(-1)
		s.Error(err)
		_, err = column.ValueByIdx(columnLen)
		s.Error(err)
		for i := 0; i < columnLen; i++ {
			v, err := column.ValueByIdx(i)
			s.NoError(err)
			s.Equal(column.values[i], v)
		}
	})

	s.Run("test_append_value", func() {
		item := make([]byte, 10)
		err := column.AppendValue(item)
		s.NoError(err)
		s.Equal(columnLen+1, column.Len())
		val, err := column.ValueByIdx(columnLen)
		s.NoError(err)
		s.Equal(item, val)

		err = column.AppendValue("POINT (30.123 -10.456)")
		s.NoError(err)

		err = column.AppendValue(1)
		s.Error(err)
	})
}

func TestColumnGeometryBytes(t *testing.T) {
	suite.Run(t, new(ColumnGeometryBytesSuite))
}
