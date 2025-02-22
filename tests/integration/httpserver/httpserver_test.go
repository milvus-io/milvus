package httpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type HTTPServerSuite struct {
	integration.MiniClusterSuite
}

const (
	PORT             = 40001
	CollectionName   = "collectionName"
	Data             = "data"
	Dimension        = "dimension"
	IDType           = "idType"
	PrimaryFieldName = "primaryFieldName"
	VectorFieldName  = "vectorFieldName"
	Schema           = "schema"
)

func (s *HTTPServerSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().HTTPCfg.Port.Key, fmt.Sprintf("%d", PORT))
	paramtable.Get().Save(paramtable.Get().QuotaConfig.DMLLimitEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.DMLMaxInsertRate.Key, "1")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.DMLMaxInsertRatePerDB.Key, "1")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.DMLMaxInsertRatePerCollection.Key, "1")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.DMLMaxInsertRatePerPartition.Key, "1")
	s.MiniClusterSuite.SetupSuite()
}

func (s *HTTPServerSuite) TearDownSuite() {
	paramtable.Get().Reset(paramtable.Get().HTTPCfg.Port.Key)
	paramtable.Get().Reset(paramtable.Get().QuotaConfig.DMLLimitEnabled.Key)
	paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)
	paramtable.Get().Reset(paramtable.Get().QuotaConfig.DMLMaxInsertRate.Key)
	paramtable.Get().Reset(paramtable.Get().QuotaConfig.DMLMaxInsertRatePerDB.Key)
	paramtable.Get().Reset(paramtable.Get().QuotaConfig.DMLMaxInsertRatePerCollection.Key)
	paramtable.Get().Reset(paramtable.Get().QuotaConfig.DMLMaxInsertRatePerPartition.Key)
	s.MiniClusterSuite.TearDownSuite()
}

func (s *HTTPServerSuite) TestInsertThrottle() {
	collectionName := "test_collection"
	dim := 768
	client := http.Client{}
	pkFieldName := "pk"
	vecFieldName := "vector"
	// create collection
	{
		dataMap := make(map[string]any, 0)
		dataMap[CollectionName] = collectionName
		dataMap[Dimension] = dim
		dataMap[IDType] = "Int64"
		dataMap[PrimaryFieldName] = "pk"
		dataMap[VectorFieldName] = "vector"

		pkField := httpserver.FieldSchema{FieldName: pkFieldName, DataType: "Int64", IsPrimary: true}
		vecParams := map[string]interface{}{"dim": dim}
		vecField := httpserver.FieldSchema{FieldName: vecFieldName, DataType: "FloatVector", ElementTypeParams: vecParams}
		schema := httpserver.CollectionSchema{Fields: []httpserver.FieldSchema{pkField, vecField}}
		dataMap[Schema] = schema
		payload, _ := json.Marshal(dataMap)
		url := "http://localhost:" + strconv.Itoa(PORT) + "/v2/vectordb" + httpserver.CollectionCategory + httpserver.CreateAction
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		s.NoError(err)
		defer resp.Body.Close()
	}

	// insert data
	{
		url := "http://localhost:" + strconv.Itoa(PORT) + "/v2/vectordb" + httpserver.EntityCategory + httpserver.InsertAction
		prepareData := func() []byte {
			dataMap := make(map[string]any, 0)
			dataMap[CollectionName] = collectionName
			vectorData := make([]float32, dim)
			for i := 0; i < dim; i++ {
				vectorData[i] = 1.0
			}
			count := 500
			dataMap[Data] = make([]map[string]interface{}, count)
			for i := 0; i < count; i++ {
				data := map[string]interface{}{pkFieldName: i, vecFieldName: vectorData}
				dataMap[Data].([]map[string]interface{})[i] = data
			}
			payload, _ := json.Marshal(dataMap)
			return payload
		}

		threadCount := 3
		wg := &sync.WaitGroup{}
		limitedThreadCount := atomic.Int32{}
		for i := 0; i < threadCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				payload := prepareData()
				req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payload))
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				s.NoError(err)
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				bodyStr := string(body)
				if strings.Contains(bodyStr, strconv.Itoa(int(merr.Code(merr.ErrHTTPRateLimit)))) {
					s.True(strings.Contains(bodyStr, "request is rejected by limiter"))
					limitedThreadCount.Inc()
				}
			}()
		}
		wg.Wait()
		// it's expected at least one insert request is rejected for throttle
		log.Info("limited thread count", zap.Int32("limitedThreadCount", limitedThreadCount.Load()))
		s.True(limitedThreadCount.Load() > 0)
	}
}

func TestHttpSearch(t *testing.T) {
	suite.Run(t, new(HTTPServerSuite))
}
