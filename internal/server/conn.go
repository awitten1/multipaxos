package server

import (
	"log"

	"google.golang.org/grpc"
)

var (
	Conns []*grpc.ClientConn = make([]*grpc.ClientConn, 0)
)

func EstablishConnections() error {
	for _, addr := range Peers {
		cc, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("Error connecting to addr %s: %w", addr, err)
			return err
		}
		Conns = append(Conns, cc)
	}
	return nil
}
