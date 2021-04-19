package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"

	pd "github.com/czs007/suvlim/master/grpc/proto"
)

const (
	port = ":50051"
)

var address = ""
type masterServer struct {
	pd.UnimplementedGreeterServer
}

func GetAddressFromETCD(ctx context.Context, key string) (string, error) {
	ETCDClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"192.168.2.28:2379"},
		DialTimeout: 5 * time.Second,
	})

	fmt.Println(err)
	kv := clientv3.NewKV(ETCDClient)

	getResp, err := kv.Get(ctx, key)
	if err != nil {
		return "", err
	}

	address := getResp.Kvs[0].Value

	return string(address), nil
}

func PutAddressToETCD(ctx context.Context, key string, address string) (error) {
	ETCDClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"192.168.2.28:2379"},
		DialTimeout: 5 * time.Second,
	})

	fmt.Println(err)
	kv := clientv3.NewKV(ETCDClient)

	_, err = kv.Put(ctx, key, address)
	return err
}

func (s *masterServer) ReportAddress(ctx context.Context, in *pd.Request) (*pd.Reply, error) {
	log.Printf("Receive a grpc server address: %v!", in.GetAddress())
	err := PutAddressToETCD(ctx, "proxyAddress", in.GetAddress())
	if err != nil{
		return &pd.Reply{Status: false}, err
	}
	//address = in.GetAddress()
	return &pd.Reply{Status: true}, nil
}

func (s *masterServer) GetAddress(ctx context.Context, in *pd.EmptyRequest) (*pd.Request, error) {
	fmt.Println("This is test SendAddress!")
	address, err := GetAddressFromETCD(ctx, "proxyAddress")
	if err != nil {
		return nil, err
	}
	return &pd.Request{Address: address}, nil
}

func main() {
	listen, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
	}

	s := grpc.NewServer()

	pd.RegisterGreeterServer(s, &masterServer{})
	if err := s.Serve(listen); err != nil{
		log.Printf("failed to server: %v", err)
	}
}