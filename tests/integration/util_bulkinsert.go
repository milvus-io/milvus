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

package integration

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
		case 128:
			var data [][128]float32
			for i := 0; i < rowCount; i++ {
				vec := [128]float32{}
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
		case 8:
			var data [][8]float32
			for i := 0; i < rowCount; i++ {
				vec := [8]float32{}
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

func BulkInsertSync(ctx context.Context, cluster *MiniClusterV2, collectionName string, bulkInsertFiles []string, bulkInsertOptions []*commonpb.KeyValuePair, clusteringInfoBytes []byte) (*milvuspb.GetImportStateResponse, error) {
	importResp, err := cluster.Proxy.Import(ctx, &milvuspb.ImportRequest{
		CollectionName: collectionName,
		Files:          bulkInsertFiles,
		Options:        bulkInsertOptions,
		ClusteringInfo: clusteringInfoBytes,
	})
	if err != nil {
		return nil, err
	}
	log.Info("Import result", zap.Any("importResp", importResp), zap.Int64s("tasks", importResp.GetTasks()))

	tasks := importResp.GetTasks()
	var importTaskState *milvuspb.GetImportStateResponse
	for _, task := range tasks {
	loop:
		for {
			importTaskState, err := cluster.Proxy.GetImportState(ctx, &milvuspb.GetImportStateRequest{
				Task: task,
			})
			if err != nil {
				return nil, err
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
	return importTaskState, nil
}
