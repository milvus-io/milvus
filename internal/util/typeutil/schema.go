package typeutil

import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func EstimateSizePerRecord(schema *schemapb.CollectionSchema) (int, error) {
	res := 0
	for _, fs := range schema.Fields {
		switch fs.DataType {
		case schemapb.DataType_BOOL, schemapb.DataType_INT8:
			res++
		case schemapb.DataType_INT16:
			res += 2
		case schemapb.DataType_INT32, schemapb.DataType_FLOAT:
			res += 4
		case schemapb.DataType_INT64, schemapb.DataType_DOUBLE:
			res += 8
		case schemapb.DataType_STRING:
			res += 125 // todo find a better way to estimate string type
		case schemapb.DataType_VECTOR_BINARY, schemapb.DataType_VECTOR_FLOAT:
			for _, kv := range fs.TypeParams {
				if kv.Key == "dim" {
					v, err := strconv.Atoi(kv.Value)
					if err != nil {
						return -1, err
					}
					res += v
				}
			}
		}
	}
	return res, nil
}
