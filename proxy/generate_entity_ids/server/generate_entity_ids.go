package main

import (
	"context"
	pb "github.com/czs007/suvlim/proxy/generate_entity_ids/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

var (
	currentID int64 = 0
)


const (
	port = ":10087"
)

type server struct {
	pb.UnimplementedGreeterServer
}


func (s *server) GetEntityID(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	var mutex sync.Mutex
	var ids []int64

	length := in.Length
	for i := int64(0); i < length; i++ {
		go func() {
			mutex.Lock()
			ids = append(ids, currentID)
			currentID++
			mutex.Unlock()
		}()

	}

	for{
		if int64(len(ids)) < length {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	return &pb.Reply{Ids: ids}, nil
}

func main() {
	listen, err := net.Listen("tcp", port)
	if err != nil{
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	if err := s.Serve(listen); err != nil{
		log.Fatalf("failed to serve: %v", err)
	}
}