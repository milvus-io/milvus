package proxynode

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jarcoal/httpmock"
)

func TestGetPulsarConfig(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	runtimeConfig := make(map[string]interface{})
	runtimeConfig[PulsarMaxMessageSizeKey] = strconv.FormatInt(5*1024*1024, 10)

	protocol := "http"
	ip := "pulsar"
	port := "18080"
	url := "/admin/v2/brokers/configuration/runtime"
	httpmock.RegisterResponder("GET", protocol+"://"+ip+":"+port+url,
		func(req *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, runtimeConfig)
		},
	)

	ret, err := GetPulsarConfig(protocol, ip, port, url)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(ret), len(runtimeConfig))
	assert.Equal(t, len(ret), 1)
	for key, value := range ret {
		assert.Equal(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", runtimeConfig[key]))
	}
}
