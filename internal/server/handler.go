package server

import (
	"context"
	"log"

	"github.com/awitten1/multipaxos/internal/rpc"
)

type PaxosServerImpl struct {
	rpc.UnimplementedPaxosServer
}

func (s *PaxosServerImpl) ClientCommand(ctx context.Context, commandBody *rpc.CommandBody) (*rpc.CommandResponse, error) {
	log.Printf("Received new decree from client: %s", commandBody.Decree)
	return &rpc.CommandResponse{Committed: true}, nil
}

func (s *PaxosServerImpl) Prepare(ctx context.Context, prepareBody *rpc.PrepareBody) (*rpc.PromiseBody, error) {
	log.Printf("Received prepare message: %s", prepareBody.String())
	lastVotes := make(map[uint64]string)
	return &rpc.PromiseBody{Decrees: lastVotes}, nil
}

func (s *PaxosServerImpl) Accept(ctx context.Context, acceptBody *rpc.AcceptBody) (*rpc.AcceptedBody, error) {
	log.Printf("Received accept message: %s", acceptBody.String())
	return &rpc.AcceptedBody{Accepted: false}, nil
}

func ComputeNextProposalNumber() uint64 {
	defer func() { NextBallot++ }()
	return NextBallot*10 + uint64(Replica)
}
