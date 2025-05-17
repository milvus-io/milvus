package proxy

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

type rankType int

const (
	invalidRankType  rankType = iota // invalidRankType   = 0
	rrfRankType                      // rrfRankType = 1
	weightedRankType                 // weightedRankType = 2
	udfExprRankType                  // udfExprRankType = 3
)

var rankTypeMap = map[string]rankType{
	"invalid":  invalidRankType,
	"rrf":      rrfRankType,
	"weighted": weightedRankType,
	"expr":     udfExprRankType,
}

type reScorer interface {
	name() string
	scorerType() rankType
	reScore(input *milvuspb.SearchResults)
	setMetricType(metricType string)
	getMetricType() string
}

type baseScorer struct {
	scorerName string
	metricType string
}

func (bs *baseScorer) name() string {
	return bs.scorerName
}

func (bs *baseScorer) setMetricType(metricType string) {
	bs.metricType = metricType
}

func (bs *baseScorer) getMetricType() string {
	return bs.metricType
}

type rrfScorer struct {
	baseScorer
	k float32
}

func (rs *rrfScorer) reScore(input *milvuspb.SearchResults) {
	index := 0
	for _, topk := range input.Results.GetTopks() {
		for i := int64(0); i < topk; i++ {
			input.Results.Scores[index] = 1 / (rs.k + float32(i+1))
			index++
		}
	}
}

func (rs *rrfScorer) scorerType() rankType {
	return rrfRankType
}

type weightedScorer struct {
	baseScorer
	weight    float32
	normScore bool
}

type activateFunc func(float32) float32

func (ws *weightedScorer) getActivateFunc() activateFunc {
	if !ws.normScore {
		return func(distance float32) float32 {
			return distance
		}
	}
	mUpper := strings.ToUpper(ws.getMetricType())
	isCosine := mUpper == strings.ToUpper(metric.COSINE)
	isIP := mUpper == strings.ToUpper(metric.IP)
	isBM25 := mUpper == strings.ToUpper(metric.BM25)
	if isCosine {
		f := func(distance float32) float32 {
			return (1 + distance) * 0.5
		}
		return f
	}

	if isIP {
		f := func(distance float32) float32 {
			return 0.5 + float32(math.Atan(float64(distance)))/math.Pi
		}
		return f
	}

	if isBM25 {
		f := func(distance float32) float32 {
			return 2 * float32(math.Atan(float64(distance))) / math.Pi
		}
		return f
	}

	f := func(distance float32) float32 {
		return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
	}
	return f
}

func (ws *weightedScorer) reScore(input *milvuspb.SearchResults) {
	activateF := ws.getActivateFunc()
	for i, distance := range input.Results.GetScores() {
		input.Results.Scores[i] = ws.weight * activateF(distance)
	}
}

func (ws *weightedScorer) scorerType() rankType {
	return weightedRankType
}

func NewReScorers(ctx context.Context, reqCnt int, rankParams []*commonpb.KeyValuePair) ([]reScorer, error) {
	if reqCnt == 0 {
		return []reScorer{}, nil
	}

	log := log.Ctx(ctx)
	res := make([]reScorer, reqCnt)
	rankTypeStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RankTypeKey, rankParams)
	if err != nil {
		log.Info("rank strategy not specified, use rrf instead")
		// if not set rank strategy, use rrf rank as default
		for i := 0; i < reqCnt; i++ {
			res[i] = &rrfScorer{
				baseScorer: baseScorer{
					scorerName: "rrf",
				},
				k: float32(defaultRRFParamsValue),
			}
		}
		return res, nil
	}

	if _, ok := rankTypeMap[rankTypeStr]; !ok {
		return nil, errors.Errorf("unsupported rank type %s", rankTypeStr)
	}

	paramStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RankParamsKey, rankParams)
	if err != nil {
		return nil, errors.New(RankParamsKey + " not found in rank_params")
	}

	var params map[string]interface{}
	err = json.Unmarshal([]byte(paramStr), &params)
	if err != nil {
		return nil, err
	}

	switch rankTypeMap[rankTypeStr] {
	case rrfRankType:
		_, ok := params[RRFParamsKey]
		if !ok {
			return nil, errors.New(RRFParamsKey + " not found in rank_params")
		}
		var k float64
		if reflect.ValueOf(params[RRFParamsKey]).CanFloat() {
			k = reflect.ValueOf(params[RRFParamsKey]).Float()
		} else {
			return nil, errors.New("The type of rank param k should be float")
		}
		if k <= 0 || k >= maxRRFParamsValue {
			return nil, errors.New(fmt.Sprintf("The rank params k should be in range (0, %d)", maxRRFParamsValue))
		}
		log.Debug("rrf params", zap.Float64("k", k))
		for i := 0; i < reqCnt; i++ {
			res[i] = &rrfScorer{
				baseScorer: baseScorer{
					scorerName: "rrf",
				},
				k: float32(k),
			}
		}
	case weightedRankType:
		if _, ok := params[WeightsParamsKey]; !ok {
			return nil, errors.New(WeightsParamsKey + " not found in rank_params")
		}
		// normalize scores by default
		normScore := true
		if _, ok := params[NormScoreKey]; ok {
			normScore = params[NormScoreKey].(bool)
		}
		weights := make([]float32, 0)
		switch reflect.TypeOf(params[WeightsParamsKey]).Kind() {
		case reflect.Slice:
			rs := reflect.ValueOf(params[WeightsParamsKey])
			for i := 0; i < rs.Len(); i++ {
				v := rs.Index(i).Elem()
				if v.CanFloat() {
					weight := v.Float()
					if weight < 0 || weight > 1 {
						return nil, errors.New("rank param weight should be in range [0, 1]")
					}
					weights = append(weights, float32(weight))
				} else {
					return nil, errors.New("The type of rank param weight should be float")
				}
			}
		default:
			return nil, errors.New("The weights param should be an array")
		}

		log.Debug("weights params", zap.Any("weights", weights), zap.Bool("norm_score", normScore))
		if reqCnt != len(weights) {
			return nil, merr.WrapErrParameterInvalid(fmt.Sprint(reqCnt), fmt.Sprint(len(weights)), "the length of weights param mismatch with ann search requests")
		}
		for i := 0; i < reqCnt; i++ {
			res[i] = &weightedScorer{
				baseScorer: baseScorer{
					scorerName: "weighted",
				},
				weight:    weights[i],
				normScore: normScore,
			}
		}
	default:
		return nil, errors.Errorf("unsupported rank type %s", rankTypeStr)
	}

	return res, nil
}
