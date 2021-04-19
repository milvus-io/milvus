package proxynode

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/retry"
)

func GetPulsarConfig(protocol, ip, port, url string) (map[string]interface{}, error) {
	var resp *http.Response
	var err error

	getResp := func() error {
		log.Println("GET: ", protocol+"://"+ip+":"+port+url)
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
