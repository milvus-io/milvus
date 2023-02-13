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

package server

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/google/uuid"

	"github.com/milvus-io/milvus/cdc/core/util"

	"github.com/mitchellh/mapstructure"
)

//go:generate easytags $GOFILE json,mapstructure

type Request struct {
	RequestType string         `json:"request_type" mapstructure:"request_type"`
	RequestData map[string]any `json:"request_data" mapstructure:"request_data"`
}

type ConnectParam struct {
	Address string `json:"address" mapstructure:"address"`
	Port    int    `json:"port" mapstructure:"port"`
}

type CreateRequest struct {
	IncludeNew bool         `json:"include_new" mapstructure:"include_new"`
	Param      ConnectParam `json:"param" mapstructure:"param"`
}

type DeleteRequest struct {
	Id string `json:"id" mapstructure:"id"`
}

func TestJson(t *testing.T) {
	requestStr := `{"request_type": "create", "request_data": {"include_new": true, "param":{"address": "127.0.0.1", "port": 8888}}}`
	var request Request
	json.Unmarshal([]byte(requestStr), &request)

	fmt.Printf("Request: %+v\n", request)

	var create CreateRequest
	mapstructure.Decode(request.RequestData, &create)
	fmt.Printf("Create: %+v\n", create)
}

func TestError(t *testing.T) {
	getError1 := func() error {
		return errors.New("get error 1")
	}

	getError2 := func() (string, error) {
		return "get2", errors.New("get error 2")
	}

	err := getError1()
	fmt.Println("err1", err)
	str, err := getError2()
	fmt.Println("err2", str, err)
}

func TestPrint(t *testing.T) {
	a := []string{"a", "b"}
	fmt.Printf("list: %v", a)
}

func TestHash(t *testing.T) {
	var str = "hello world"
	hash := md5.Sum(util.ToBytes(str))
	fmt.Println(hex.EncodeToString(hash[:]))

	uid := uuid.Must(uuid.NewRandom())
	fmt.Println(uid.String())
}

func TestIsError(t *testing.T) {
	err := NewClientError("client error")
	fmt.Println(errors.Is(err, ClientErr))
	fmt.Println(errors.Is(err, ServerErr))

	err = NewServerError(errors.New("server error"))
	fmt.Println(errors.Is(err, ClientErr))
	fmt.Println(errors.Is(err, ServerErr))
}

func TestChan(t *testing.T) {
	c1 := make(chan string, 2)
	c2 := make(chan struct{})
	func() {
		close(c1)
		close(c2)
	}()
	select {
	case <-c2:
		fmt.Println("c2")
	default:
		c1 <- "foo"
		fmt.Println("c1")
	}
}

func TestEtcd(t *testing.T) {
	fmt.Println("new")
	etcdCtl, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}, DialTimeout: 5 * time.Second})
	fmt.Println("txn")
	ctx := context.Background()
	//ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	//defer cancel()
	//fmt.Println(etcdCtl.Status(ctx, "loalhost:2379"))
	//etcdCtl.Txn(ctx).Then(clientv3.OpPut("foo1", "0123"),
	//	clientv3.OpPut("foo2", "4560")).Commit()
	c := etcdCtl.Watch(ctx, "foo")
	fmt.Println("receive")
	data, ok := <-c
	fmt.Println(data, ok)
}

func TestEtcdConn(t *testing.T) {
	etcdCtl, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}, DialTimeout: 5 * time.Second})
	fmt.Println(err)
	fmt.Println(util.EtcdStatus(etcdCtl))
}

func outer() (func() int, int) {
	outer_var := 2
	inner := func() int {
		outer_var += 99 // outer_var from outer scope is mutated.
		return outer_var
	}
	inner()
	return inner, outer_var // return inner func and mutated outer_var 101
}

func TestOut(t *testing.T) {
	fmt.Println(outer())
}

func TestSplit(t *testing.T) {
	strings.SplitAfter("/a/b/c/d", "/a/b")
}

func TestChanReadAll(t *testing.T) {
	c := make(chan int, 5)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 4; i++ {
			c <- i
		}
	}()
	go func() {
		done <- struct{}{}
	}()
	for {
		select {
		case <-done:
			for {
				select {
				case i := <-c:
					if i == 2 {
						continue
					}
					fmt.Println("handleDone", i)
				default:
					fmt.Println("over")
					return
				}
			}
		default:
			i := <-c
			fmt.Println("defult", i)
		}
	}
}

func TestChanConcurrent(t *testing.T) {
	pauseChan := make(chan struct{})
	close(pauseChan)
	go func() {
		for {
			<-pauseChan
			fmt.Println(rand.Int())
		}
	}()
	go func() {
		time.Sleep(time.Millisecond)
		pauseChan = make(chan struct{})
		fmt.Println("=========================")
		time.Sleep(time.Millisecond)
		close(pauseChan)
	}()
	time.Sleep(time.Second)
}

func TestDef(t *testing.T) {
	f := func() (s string, e error) {
		defer func() {
			if e != nil {
				fmt.Println("err", e)
			}
		}()
		return "hello", errors.New("foo2")
	}
	f()
}
