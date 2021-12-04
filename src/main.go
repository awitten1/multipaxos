package main

import (
	"context"
	"log"
	"net"
	"flag"
	"strings"

	"google.golang.org/grpc"
	pb "github.com/awitten1/multipaxos/rpc"
)

var (
	Peers []string,
	Port int32,
)

func main() {

}

func parseCliArgs() {
	var peers String

	flag.StringVar(&peers, "peers")
	flag.Int(&Port, "port")

	flag.Parse()
	Peers = strings.Split(peers, ",")
}
