package main

import (
	"context"
	"fmt"
	pb "github.com/czs007/suvlim/proxy/generate_entity_ids/proto"
	"google.golang.org/grpc"
	"log"
	"time"
)

const (
	address     = "localhost:10087"
)


func getIds(length int64) {
	con, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer con.Close()
	c := pb.NewGreeterClient(con)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := c.GetEntityID(ctx, &pb.Request{Length: length})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	fmt.Println("+++++++++++++++++++++++++++++++++++++")
	fmt.Println(r.GetIds())

}


func main() {

	go getIds(100)

	go getIds(100)

	time.Sleep(3 * time.Second)
}
