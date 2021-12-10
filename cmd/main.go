package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	db "github.com/awitten1/multipaxos/internal/db"
	"github.com/awitten1/multipaxos/internal/leader"
	"github.com/awitten1/multipaxos/internal/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	port, err := parseCliArgs()
	leader.BecomeFollower()
	if err != nil {
		panic(err)
	}
	db.DB, err = db.SetupDB(uint8(server.Replica))
	if err != nil {
		panic(err)
	}
	log.Printf("Starting server")
	server.StartServer(port)
}

func parseCliArgs() (int, error) {
	var peers string
	var port int
	var replicaId int

	flag.StringVar(&peers, "peers", "", "Comma separated list of <ip address>:<port number>")
	flag.IntVar(&port, "port", -1, "Port number to listen on")
	flag.IntVar(&replicaId, "replica", -1, "Replica ID number (must be unique)")
	flag.StringVar(&db.DBPath, "dbpath", "", "Directory to persist to")
	//flag.BoolVar(&server.Leader, "leader", false, "is the leader (this option will be removed)")

	flag.Parse()
	if peers == "" {
		return 0, fmt.Errorf("must provide list of peers")
	}
	if port < 0 {
		return 0, fmt.Errorf("must provide valid port number")
	}
	if replicaId < 0 || replicaId >= 256 {
		return 0, fmt.Errorf("replica Id must be a positive, 8-bit integer")
	}
	server.Replica = int8(replicaId)
	if db.DBPath == "" {
		return 0, fmt.Errorf("must provide a db directory")
	}
	server.Peers = strings.Split(peers, ",")
	server.N = uint32(len(server.Peers)) + 1
	return port, nil
}
