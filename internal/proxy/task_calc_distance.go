package proxy

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type calcDistanceTask struct {
	traceID   string
	queryFunc func(ids *milvuspb.VectorIDs) (*milvuspb.QueryResults, error)
}

func (t *calcDistanceTask) arrangeVectorsByIntID(inputIds []int64, sequence map[int64]int, retrievedVectors *schemapb.VectorField) (*schemapb.VectorField, error) {
	if retrievedVectors.GetFloatVector() != nil {
		floatArr := retrievedVectors.GetFloatVector().GetData()
		element := retrievedVectors.GetDim()
		result := make([]float32, 0, int64(len(inputIds))*element)
		for _, id := range inputIds {
			index, ok := sequence[id]
			if !ok {
				log.Error("id not found in CalcDistance", zap.Int64("id", id))
				return nil, errors.New("failed to fetch vectors by id: " + fmt.Sprintln(id))
			}
			result = append(result, floatArr[int64(index)*element:int64(index+1)*element]...)
		}

		return &schemapb.VectorField{
			Dim: element,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: result,
				},
			},
		}, nil
	}

	if retrievedVectors.GetBinaryVector() != nil {
		binaryArr := retrievedVectors.GetBinaryVector()
		singleBitLen := distance.SingleBitLen(retrievedVectors.GetDim())
		numBytes := singleBitLen / 8

		result := make([]byte, 0, int64(len(inputIds))*numBytes)
		for _, id := range inputIds {
			index, ok := sequence[id]
			if !ok {
				log.Error("id not found in CalcDistance", zap.Int64("id", id))
				return nil, errors.New("failed to fetch vectors by id: " + fmt.Sprintln(id))
			}
			result = append(result, binaryArr[int64(index)*numBytes:int64(index+1)*numBytes]...)
		}

		return &schemapb.VectorField{
			Dim: retrievedVectors.GetDim(),
			Data: &schemapb.VectorField_BinaryVector{
				BinaryVector: result,
			},
		}, nil
	}

	return nil, errors.New("unsupported vector type")
}

func (t *calcDistanceTask) arrangeVectorsByStrID(inputIds []string, sequence map[string]int, retrievedVectors *schemapb.VectorField) (*schemapb.VectorField, error) {
	if retrievedVectors.GetFloatVector() != nil {
		floatArr := retrievedVectors.GetFloatVector().GetData()
		element := retrievedVectors.GetDim()
		result := make([]float32, 0, int64(len(inputIds))*element)
		for _, id := range inputIds {
			index, ok := sequence[id]
			if !ok {
				log.Error("id not found in CalcDistance", zap.String("id", id))
				return nil, errors.New("failed to fetch vectors by id: " + fmt.Sprintln(id))
			}
			result = append(result, floatArr[int64(index)*element:int64(index+1)*element]...)
		}

		return &schemapb.VectorField{
			Dim: element,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: result,
				},
			},
		}, nil
	}

	if retrievedVectors.GetBinaryVector() != nil {
		binaryArr := retrievedVectors.GetBinaryVector()
		singleBitLen := distance.SingleBitLen(retrievedVectors.GetDim())
		numBytes := singleBitLen / 8

		result := make([]byte, 0, int64(len(inputIds))*numBytes)
		for _, id := range inputIds {
			index, ok := sequence[id]
			if !ok {
				log.Error("id not found in CalcDistance", zap.String("id", id))
				return nil, errors.New("failed to fetch vectors by id: " + fmt.Sprintln(id))
			}
			result = append(result, binaryArr[int64(index)*numBytes:int64(index+1)*numBytes]...)
		}

		return &schemapb.VectorField{
			Dim: retrievedVectors.GetDim(),
			Data: &schemapb.VectorField_BinaryVector{
				BinaryVector: result,
			},
		}, nil
	}

	return nil, errors.New("unsupported vector type")
}

func (t *calcDistanceTask) Execute(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	param, _ := funcutil.GetAttrByKeyFromRepeatedKV("metric", request.GetParams())
	metric, err := distance.ValidateMetricType(param)
	if err != nil {
		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	// the vectors retrieved are random order, we need re-arrange the vectors by the order of input ids
	arrangeFunc := func(ids *milvuspb.VectorIDs, retrievedFields []*schemapb.FieldData) (*schemapb.VectorField, error) {
		var retrievedIds *schemapb.ScalarField
		var retrievedVectors *schemapb.VectorField
		isStringID := true
		for _, fieldData := range retrievedFields {
			if fieldData.FieldName == ids.FieldName {
				retrievedVectors = fieldData.GetVectors()
			}
			if fieldData.Type == schemapb.DataType_Int64 ||
				fieldData.Type == schemapb.DataType_VarChar ||
				fieldData.Type == schemapb.DataType_String {
				retrievedIds = fieldData.GetScalars()

				if fieldData.Type == schemapb.DataType_Int64 {
					isStringID = false
				}
			}
		}

		if retrievedIds == nil || retrievedVectors == nil {
			return nil, errors.New("failed to fetch vectors")
		}

		if isStringID {
			dict := make(map[string]int)
			for index, id := range retrievedIds.GetStringData().GetData() {
				dict[id] = index
			}

			inputIds := ids.IdArray.GetStrId().GetData()
			return t.arrangeVectorsByStrID(inputIds, dict, retrievedVectors)
		}

		dict := make(map[int64]int)
		for index, id := range retrievedIds.GetLongData().GetData() {
			dict[id] = index
		}

		inputIds := ids.IdArray.GetIntId().GetData()
		return t.arrangeVectorsByIntID(inputIds, dict, retrievedVectors)
	}

	log := log.Ctx(ctx)
	log.Debug("CalcDistance received",
		zap.String("role", typeutil.ProxyRole),
		zap.String("metric", metric))

	vectorsLeft := request.GetOpLeft().GetDataArray()
	opLeft := request.GetOpLeft().GetIdArray()
	if opLeft != nil {
		log.Debug("OpLeft IdArray not empty, Get vectors by id", zap.String("role", typeutil.ProxyRole))

		result, err := t.queryFunc(opLeft)
		if err != nil {
			log.Warn("Failed to get left vectors by id",
				zap.String("role", typeutil.ProxyRole),
				zap.Error(err))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("OpLeft IdArray not empty, Get vectors by id done",
			zap.String("role", typeutil.ProxyRole))

		vectorsLeft, err = arrangeFunc(opLeft, result.FieldsData)
		if err != nil {
			log.Debug("Failed to re-arrange left vectors",
				zap.String("role", typeutil.ProxyRole),
				zap.Error(err))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("Re-arrange left vectors done",
			zap.String("role", typeutil.ProxyRole))
	}

	if vectorsLeft == nil {
		msg := "Left vectors array is empty"
		log.Debug(msg,
			zap.String("role", typeutil.ProxyRole))

		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msg,
			},
		}, nil
	}

	vectorsRight := request.GetOpRight().GetDataArray()
	opRight := request.GetOpRight().GetIdArray()
	if opRight != nil {
		log.Debug("OpRight IdArray not empty, Get vectors by id",
			zap.String("role", typeutil.ProxyRole))

		result, err := t.queryFunc(opRight)
		if err != nil {
			log.Debug("Failed to get right vectors by id",
				zap.String("role", typeutil.ProxyRole),
				zap.Error(err))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("OpRight IdArray not empty, Get vectors by id done",
			zap.String("role", typeutil.ProxyRole))

		vectorsRight, err = arrangeFunc(opRight, result.FieldsData)
		if err != nil {
			log.Debug("Failed to re-arrange right vectors",
				zap.String("role", typeutil.ProxyRole),
				zap.Error(err))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("Re-arrange right vectors done",
			zap.String("role", typeutil.ProxyRole))
	}

	if vectorsRight == nil {
		msg := "Right vectors array is empty"
		log.Warn(msg, zap.String("role", typeutil.ProxyRole))

		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msg,
			},
		}, nil
	}

	if vectorsLeft.GetDim() != vectorsRight.GetDim() {
		msg := "Vectors dimension is not equal"
		log.Debug(msg, zap.String("role", typeutil.ProxyRole))

		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msg,
			},
		}, nil
	}

	if vectorsLeft.GetFloatVector() != nil && vectorsRight.GetFloatVector() != nil {
		distances, err := distance.CalcFloatDistance(vectorsLeft.GetDim(), vectorsLeft.GetFloatVector().GetData(), vectorsRight.GetFloatVector().GetData(), metric)
		if err != nil {
			log.Warn("Failed to CalcFloatDistance",
				zap.Int64("leftDim", vectorsLeft.GetDim()),
				zap.Int("leftLen", len(vectorsLeft.GetFloatVector().GetData())),
				zap.Int64("rightDim", vectorsRight.GetDim()),
				zap.Int("rightLen", len(vectorsRight.GetFloatVector().GetData())),
				zap.String("traceID", t.traceID),
				zap.String("role", typeutil.ProxyRole),
				zap.Error(err))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("CalcFloatDistance done",
			zap.String("traceID", t.traceID),
			zap.String("role", typeutil.ProxyRole))

		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success, Reason: ""},
			Array: &milvuspb.CalcDistanceResults_FloatDist{
				FloatDist: &schemapb.FloatArray{
					Data: distances,
				},
			},
		}, nil
	}

	if vectorsLeft.GetBinaryVector() != nil && vectorsRight.GetBinaryVector() != nil {
		hamming, err := distance.CalcHammingDistance(vectorsLeft.GetDim(), vectorsLeft.GetBinaryVector(), vectorsRight.GetBinaryVector())
		if err != nil {
			log.Debug("Failed to CalcHammingDistance",
				zap.Int64("leftDim", vectorsLeft.GetDim()),
				zap.Int("leftLen", len(vectorsLeft.GetBinaryVector())),
				zap.Int64("rightDim", vectorsRight.GetDim()),
				zap.Int("rightLen", len(vectorsRight.GetBinaryVector())),
				zap.String("role", typeutil.ProxyRole),
				zap.Error(err))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		if metric == distance.HAMMING {
			log.Debug("CalcHammingDistance done", zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success, Reason: ""},
				Array: &milvuspb.CalcDistanceResults_IntDist{
					IntDist: &schemapb.IntArray{
						Data: hamming,
					},
				},
			}, nil
		}

		if metric == distance.TANIMOTO {
			tanimoto, err := distance.CalcTanimotoCoefficient(vectorsLeft.GetDim(), hamming)
			if err != nil {
				log.Warn("Failed to CalcTanimotoCoefficient",
					zap.String("role", typeutil.ProxyRole),
					zap.Error(err))

				return &milvuspb.CalcDistanceResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    err.Error(),
					},
				}, nil
			}

			log.Debug("CalcTanimotoCoefficient done",
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success, Reason: ""},
				Array: &milvuspb.CalcDistanceResults_FloatDist{
					FloatDist: &schemapb.FloatArray{
						Data: tanimoto,
					},
				},
			}, nil
		}
	}

	err = errors.New("unexpected error")
	if (vectorsLeft.GetBinaryVector() != nil && vectorsRight.GetFloatVector() != nil) || (vectorsLeft.GetFloatVector() != nil && vectorsRight.GetBinaryVector() != nil) {
		err = errors.New("cannot calculate distance between binary vectors and float vectors")
	}

	log.Warn("Failed to CalcDistance",
		zap.String("role", typeutil.ProxyRole),
		zap.Error(err))

	return &milvuspb.CalcDistanceResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		},
	}, nil
}
