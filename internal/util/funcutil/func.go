package funcutil

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"errors"

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

func CheckPortAvailable(port int) bool {
	addr := ":" + strconv.Itoa(port)
	listener, err := net.Listen("tcp", addr)
	if listener != nil {
		listener.Close()
	}
	return err == nil
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

func WaitForComponentStates(ctx context.Context, service StateComponent, serviceName string, states []internalpb2.StateCode, attempts int, sleep time.Duration) error {
	checkFunc := func() error {
		resp, err := service.GetComponentStates(ctx)
		if err != nil {
			return err
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		meet := false
		for _, state := range states {
			if resp.State.StateCode == state {
				meet = true
				break
			}
		}
		if !meet {
			msg := fmt.Sprintf("WaitForComponentStates, not meet, %s current state:%d", serviceName, resp.State.StateCode)
			return errors.New(msg)
		}
		return nil
	}
	return retry.Retry(attempts, sleep, checkFunc)
}

func WaitForComponentInitOrHealthy(ctx context.Context, service StateComponent, serviceName string, attempts int, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb2.StateCode{internalpb2.StateCode_Initializing, internalpb2.StateCode_Healthy}, attempts, sleep)
}

func WaitForComponentInit(ctx context.Context, service StateComponent, serviceName string, attempts int, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb2.StateCode{internalpb2.StateCode_Initializing}, attempts, sleep)
}

func WaitForComponentHealthy(ctx context.Context, service StateComponent, serviceName string, attempts int, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb2.StateCode{internalpb2.StateCode_Healthy}, attempts, sleep)
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
