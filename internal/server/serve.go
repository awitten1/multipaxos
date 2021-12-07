package server

import (
	"fmt"
	"log"
	"net"
	"time"

	rpc "github.com/awitten1/multipaxos/internal/rpc"
	"google.golang.org/grpc"
)

type State int

var (
	Peers   []string
	Port    int32
	Replica int8
	DBPath  string
	// Only increment while sending Prepare messages
	Leader       bool
	NextLogIndex uint64 = 0
	N            uint32
	ServerState  State
)

const (
	LEADER State = iota
	FOLLOWER
)

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
	//go func() { PrintLog() }()
	grpcServer.Serve(lis)
}

func PrintLog() {
	for {
		time.Sleep(20 * time.Second)
		//db.DB.PrintLog()
	}
}
