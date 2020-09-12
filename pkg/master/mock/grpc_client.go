package mock

import (
	"context"
	"log"
	"time"

	pb "github.com/czs007/suvlim/pkg/master/grpc/master"
	msgpb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"google.golang.org/grpc"
)

// func main() {
//	// Set up a connection to the server.
//	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
//	if err != nil {
//		log.Fatalf("did not connect: %v", err)
//	}
//	defer conn.Close()
//	c := pb.NewGreeterClient(conn)

//	// Contact the server and print out its response.
//	name := defaultName
//	if len(os.Args) > 1 {
//		name = os.Args[1]
//	}
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	r, err := c.SayHello(ctx, &pb.HelloRequest{Name: name})
//	if err != nil {
//		log.Fatalf("could not greet: %v", err)
//	}
//	log.Printf("Greeting: %s", r.GetMessage())
// }

const (
	addr = "192.168.1.10:53100"
)

func FakeCreateCollectionByGRPC() (string, uint64) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewMasterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()

	r, err := c.CreateCollection(ctx, &msgpb.Mapping{
		CollectionName: "test-collection",
	})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	//	log.Printf("CreateCollection: %s, id: %d", r.GetCollectionName(), r.GetCollectionId())
	//	return r.GetCollectionName(), r.GetCollectionId()
	return r.GetReason(), 0
}
