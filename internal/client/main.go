package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	rpc "github.com/awitten1/multipaxos/internal/rpc"
	"google.golang.org/grpc"
)

var (
	servers []string
	command string
	getLog  bool
)

func main() {
	parseCliArgs()
	for _, address := range servers {
		if err := sendCommand(address); err != nil {
			log.Printf("failed sending command to %s because of: %s", address, err.Error())
		} else {
			break
		}
	}
}

func sendCommand(address string) error {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()
	c := rpc.NewPaxosClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if getLog {
		resp, err := c.GetLog(ctx, &rpc.GetLogBody{})
		if err != nil {
			return err
		}
		log.Printf("Received log: %s", strings.Join(resp.Entries, ","))
		return nil
	}

	_, err = c.ClientCommand(ctx, &rpc.CommandBody{Decree: command})
	return err
}

func parseCliArgs() {
	var s string
	flag.StringVar(&s, "servers", "", "comma separated list of servers")
	flag.StringVar(&command, "cmd", "", "command to send to server")
	flag.BoolVar(&getLog, "getlog", false, "query log from server (mutually exclusive with cmd)")
	flag.Parse()
	if s == "" {
		panic("must provide server addresses")
	}
	servers = strings.Split(s, ",")
	if command == "" && !getLog {
		panic("must provide command")
	}
	if getLog && command != "" {
		panic("getlog is mutually exclusive with cmd")
	}
}
