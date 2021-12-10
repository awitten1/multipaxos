package server

import (
	"fmt"
	"log"

	"google.golang.org/grpc"
)

var retryPolicy = `{
	"methodConfig": [{
		// config per method or all methods under service
		"waitForReady": true,

		"retryPolicy": {
			"MaxAttempts": 4,
			"InitialBackoff": ".01s",
			"MaxBackoff": ".01s",
			"BackoffMultiplier": 1.0,
			// this value is grpc code
			"RetryableStatusCodes": [ "UNAVAILABLE" ]
		}
	}]
}`

var (
	Conns map[string]*grpc.ClientConn = make(map[string]*grpc.ClientConn)
	Self  *grpc.ClientConn
)

func EstablishConnections(port int) error {
	for _, addr := range Peers {
		cc, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("Error connecting to addr %s: %s", addr, err)
			return err
		}
		Conns[addr] = cc
	}
	cc, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithInsecure())
	if err != nil {
		log.Printf("Could not create self connection: %s", err.Error())
	} else {
		Self = cc
	}
	return nil
}
