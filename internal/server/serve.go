package server

import (
	"fmt"
	"log"
	"net"

	rpc "github.com/awitten1/multipaxos/internal/rpc"
	"google.golang.org/grpc"
)

var (
	Peers      []string
	Port       int32
	Replica    int8
	DBPath     string
	NextBallot uint64 = 0
)

type ServerAddress struct {
	Address string
	Port    int32
}

func StartServer() {
	log.Printf("About to start server listening on port %d", Port)
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	rpc.RegisterPaxosServer(grpcServer, &PaxosServerImpl{})
	go func() { EstablishConnections() }()
	grpcServer.Serve(lis)
}
