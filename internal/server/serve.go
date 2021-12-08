package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/awitten1/multipaxos/internal/db"
	"github.com/awitten1/multipaxos/internal/leader"
	rpc "github.com/awitten1/multipaxos/internal/rpc"
	"google.golang.org/grpc"
)

type State int32

var (
	Peers   []string
	Replica int8
	// Only increment while sending Prepare messages
	NextLogIndex uint64 = 0
	N            uint32
	ServerState  State
)

const (
	LEADER State = iota
	FOLLOWER
)

func StartServer(port int) {
	log.Printf("About to start server listening on port %d", port)
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	rpc.RegisterPaxosServer(grpcServer, &PaxosServerImpl{})
	EstablishConnections()
	go PrintPaxosInfo()
	go MonitorHeartbeats(context.Background(), uint32(Replica))

	grpcServer.Serve(lis)
}

// Log paxos instance state.  Just for the purpose of visibility/debugging
func PrintPaxosInfo() {
	for {
		time.Sleep(10 * time.Second)
		var state = "FOLLOWER"
		if leader.AmILeader() {
			state = "LEADER"
		}
		log.Printf("In state: %s", state)
		db.DB.PrintPaxosInfo()
	}
}
