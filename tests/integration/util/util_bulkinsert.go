package util

import (
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	DIM8   = 8
	DIM128 = 128
)

func GenerateNumpyFile(filePath string, rowCount int, fieldSchema *schemapb.FieldSchema) error {
	dType := fieldSchema.DataType
	if dType == schemapb.DataType_VarChar {
		var data []string
		for i := 0; i < rowCount; i++ {
			data = append(data, "str")
		}
		err := importutil.CreateNumpyFile(filePath, data)
		if err != nil {
			log.Warn("failed to create numpy file", zap.Error(err))
			return err
		}
	} else if dType == schemapb.DataType_FloatVector {
		dimStr, ok := funcutil.KeyValuePair2Map(fieldSchema.GetTypeParams())[common.DimKey]
		if !ok {
			return errors.New("FloatVector field needs dim parameter")
		}
		dim, err := strconv.Atoi(dimStr)
		if err != nil {
			return err
		}
		switch dim {
		case DIM128:
			var data [][DIM128]float32
			for i := 0; i < rowCount; i++ {
				vec := [DIM128]float32{}
				for j := 0; j < dim; j++ {
					vec[j] = 1.1
				}
				data = append(data, vec)
			}
			err = importutil.CreateNumpyFile(filePath, data)
			if err != nil {
				log.Warn("failed to create numpy file", zap.Error(err))
				return err
			}
		case DIM8:
			var data [][DIM8]float32
			for i := 0; i < rowCount; i++ {
				vec := [DIM8]float32{}
				for j := 0; j < dim; j++ {
					vec[j] = 1.1
				}
				data = append(data, vec)
			}
			err = importutil.CreateNumpyFile(filePath, data)
			if err != nil {
				log.Warn("failed to create numpy file", zap.Error(err))
				return err
			}
		default:
			// dim must be a constant expression, so we make DIM128 a constant, if you want other dim value,
			// please add new constant like: DIM16, DIM32, and change this part into switch case style
			log.Warn("Unsupported dim value", zap.Int("dim", dim))
			return errors.New("Unsupported dim value, please add new dim constants and case")
		}
	}
	return nil
}

func BulkInsertSync(ctx context.Context, cluster *integration.MiniCluster, collectionName string, bulkInsertFiles []string, bulkInsertOptions []*commonpb.KeyValuePair) error {
	importResp, err := cluster.Proxy.Import(ctx, &milvuspb.ImportRequest{
		CollectionName: collectionName,
		Files:          bulkInsertFiles,
		Options:        bulkInsertOptions,
	})
	if err != nil {
		return err
	}
	log.Info("Import result", zap.Any("importResp", importResp), zap.Int64s("tasks", importResp.GetTasks()))

	tasks := importResp.GetTasks()
	for _, task := range tasks {
	loop:
		for {
			importTaskState, err := cluster.Proxy.GetImportState(ctx, &milvuspb.GetImportStateRequest{
				Task: task,
			})
			if err != nil {
				return err
			}
			switch importTaskState.GetState() {
			case commonpb.ImportState_ImportFailed:
			case commonpb.ImportState_ImportFailedAndCleaned:
			case commonpb.ImportState_ImportCompleted:
				break loop
			default:
				log.Info("import task state", zap.Int64("id", task), zap.String("state", importTaskState.GetState().String()))
				time.Sleep(time.Second * time.Duration(3))
				continue
			}
		}
	}
	return nil
}
