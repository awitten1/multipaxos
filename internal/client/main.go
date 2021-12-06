package main

import (
	"context"
	"flag"
	"log"
	"time"

	rpc "github.com/awitten1/multipaxos/internal/rpc"
	"google.golang.org/grpc"
)

var (
	address string
	command string
)

func main() {
	parseCliArgs()
	log.Printf("parsed cli args successfully")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := rpc.NewPaxosClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := c.ClientCommand(ctx, &rpc.CommandBody{Decree: command})
	if err != nil {
		log.Fatalf("command failed: %v", err)
	}
	log.Printf("Command success status: %t, error message (empty on success): %s", resp.Committed, resp.ErrorMessage)
}

func parseCliArgs() {
	flag.StringVar(&address, "server", "", "server to send command to")
	flag.StringVar(&command, "cmd", "", "command to send to server")
	flag.Parse()
	if address == "" {
		panic("must provide server address")
	}
	if command == "" {
		panic("must provide command")
	}
}
