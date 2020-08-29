package main

import (
	"context"
	"fmt"
	"log"
	"time"

	master "github.com/czs007/suvlim/master/grpc/proto"
	"google.golang.org/grpc"
)

const (
	masterAddress     = "192.168.2.28:50051"
)


func main() {
	con, err := grpc.Dial(masterAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect %v", err)
	}

	masterClient := master.NewGreeterClient(con)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()
	proxyAddress, err := masterClient.GetAddress(ctx, &master.EmptyRequest{})
	if err != nil {
		log.Printf("could not get address: %v", err)
	}
	log.Printf("you can connect the proxy server: %v", proxyAddress.GetAddress())
	con.Close()

	test, err := masterClient.GetAddress(ctx, &master.EmptyRequest{})
	fmt.Println(test.GetAddress())

	con, err = grpc.Dial(proxyAddress.GetAddress(), grpc.WithInsecure(), grpc.WithBlock())
	proxyClient := master.NewHelloServiceClient(con)
	helloReply, err := proxyClient.SayHello(ctx, &master.HelloRequest{Name: "hahaha"})
	fmt.Println(helloReply.Msg)
}