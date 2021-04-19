package funcutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/go-basic/ipv4"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
)

func CheckGrpcReady(ctx context.Context, targetCh chan error) {
	select {
	case <-time.After(100 * time.Millisecond):
		targetCh <- nil
	case <-ctx.Done():
		return
	}
}

func GetAvailablePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func GetLocalIP() string {
	return ipv4.LocalIP()
}

func WaitForComponentReady(service StateComponent, serviceName string, attempts int, sleep time.Duration) error {
	checkFunc := func() error {
		resp, err := service.GetComponentStates()
		if err != nil {
			return err
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return errors.New(resp.Status.Reason)
		}

		if resp.State.StateCode != internalpb2.StateCode_HEALTHY {
			return errors.New("")
		}

		return nil
	}
	err := retry.Retry(attempts, sleep, checkFunc)
	if err != nil {
		errMsg := fmt.Sprintf("ProxyNode wait for %s ready failed", serviceName)
		return errors.New(errMsg)
	}
	return nil
}

func ParseIndexParamsMap(mStr string) (map[string]string, error) {
	buffer := make(map[string]interface{})
	err := json.Unmarshal([]byte(mStr), &buffer)
	if err != nil {
		return nil, errors.New("Unmarshal params failed")
	}
	ret := make(map[string]string)
	for key, value := range buffer {
		valueStr := fmt.Sprintf("%v", value)
		ret[key] = valueStr
	}
	return ret, nil
}
