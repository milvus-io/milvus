package proxynode

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
)

func GetPulsarConfig(protocol, ip, port, url string) (map[string]interface{}, error) {
	var resp *http.Response
	var err error

	getResp := func() error {
		log.Debug("proxynode util", zap.String("url", protocol+"://"+ip+":"+port+url))
		resp, err = http.Get(protocol + "://" + ip + ":" + port + url)
		return err
	}

	err = retry.Retry(10, time.Second, getResp)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]interface{})
	err = json.Unmarshal(body, &ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func getMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func getMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func CheckIntByRange(params map[string]string, key string, min, max int) bool {
	valueStr, ok := params[key]
	if !ok {
		return false
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false
	}

	return value >= min && value <= max
}

func CheckStrByValues(params map[string]string, key string, container []string) bool {
	value, ok := params[key]
	if !ok {
		return false
	}

	return funcutil.SliceContain(container, value)
}
