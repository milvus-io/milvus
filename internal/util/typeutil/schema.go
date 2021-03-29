package typeutil

import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func EstimateSizePerRecord(schema *schemapb.CollectionSchema) (int, error) {
	res := 0
	for _, fs := range schema.Fields {
		switch fs.DataType {
		case schemapb.DataType_Bool, schemapb.DataType_Int8:
			res++
		case schemapb.DataType_Int16:
			res += 2
		case schemapb.DataType_Int32, schemapb.DataType_Float:
			res += 4
		case schemapb.DataType_Int64, schemapb.DataType_Double:
			res += 8
		case schemapb.DataType_String:
			res += 125 // todo find a better way to estimate string type
		case schemapb.DataType_BinaryVector:
			for _, kv := range fs.TypeParams {
				if kv.Key == "dim" {
					v, err := strconv.Atoi(kv.Value)
					if err != nil {
						return -1, err
					}
					res += v / 8
					break
				}
			}
		case schemapb.DataType_FloatVector:
			for _, kv := range fs.TypeParams {
				if kv.Key == "dim" {
					v, err := strconv.Atoi(kv.Value)
					if err != nil {
						return -1, err
					}
					res += v * 4
					break
				}
			}
		}
	}
	return res, nil
}
