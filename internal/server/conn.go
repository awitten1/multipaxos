package server

import (
	"log"

	rpc "github.com/awitten1/multipaxos/internal/rpc"
	"google.golang.org/grpc"
)

var (
	Clients map[string]rpc.PaxosClient = make(map[string]rpc.PaxosClient)
)

func EstablishConnections() error {
	for _, addr := range Peers {
		cc, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("Error connecting to addr %s: %s", addr, err)
			return err
		}
		Clients[addr] = rpc.NewPaxosClient(cc)
	}
	return nil
}
