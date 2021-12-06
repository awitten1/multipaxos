package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awitten1/multipaxos/internal/db"
	"github.com/awitten1/multipaxos/internal/rpc"
)

type PaxosServerImpl struct {
	rpc.UnimplementedPaxosServer
}

func broadcastAccepts(ctx context.Context, decree string, logIndex uint64) (uint32, error) {
	var acks uint32 = 1
	var wg sync.WaitGroup
	for _, client := range Clients {
		c := client
		wg.Add(1)
		go func() {
			defer wg.Done()

			acceptedBody, err := c.Accept(ctx, &rpc.AcceptBody{BallotNumber: NextBallot, Decree: decree, LogIndex: logIndex})
			if err != nil || acceptedBody == nil {
				log.Printf("Error sending Accept message: %s", err.Error())
				return
			}
			if acceptedBody.Accepted {
				atomic.AddUint32(&acks, 1)
			}
		}()
	}
	wg.Wait()
	return acks, nil
}

func broadcastLearns(decree string, logIndex uint64) {
	for _, client := range Clients {
		c := client
		go func() {
			for {
				acceptedBody, err := c.Learn(context.Background(), &rpc.LearnBody{LogIndex: logIndex, Decree: decree})
				if err != nil || acceptedBody == nil {
					log.Printf("error sending Learn message: %s", err.Error())
					time.Sleep(10 * time.Second)
					continue
				}
				break
			}
		}()
	}
}

func (s *PaxosServerImpl) ClientCommand(ctx context.Context, commandBody *rpc.CommandBody) (*rpc.CommandResponse, error) {
	log.Printf("Received new decree from client: %s", commandBody.Decree)
	if !Leader {
		return &rpc.CommandResponse{Committed: false, ErrorMessage: "cannot commit message, not the leader"}, nil
	}
	nextIndex := atomic.AddUint64(&NextLogIndex, 1)
	db.State.UpsertPaxosLogState(nextIndex, commandBody.Decree, NextBallot)
	acks, err := broadcastAccepts(ctx, commandBody.Decree, nextIndex)
	if err != nil {
		return &rpc.CommandResponse{Committed: false, ErrorMessage: err.Error()}, err
	}
	log.Printf("Received %d positive acks to the accept command, needed %d to replicate on a majority of nodes", acks, N/2+1)
	if acks < N/2+1 {
		log.Printf("Returning failure to client")
		return &rpc.CommandResponse{Committed: false, ErrorMessage: "Failed to replicate message: could not receive enough acks"}, nil
	}
	go func() { db.State.InsertAcceptedLog(nextIndex, commandBody.Decree) }()
	broadcastLearns(commandBody.Decree, nextIndex)
	return &rpc.CommandResponse{Committed: true, ErrorMessage: ""}, nil
}

func (s *PaxosServerImpl) Prepare(ctx context.Context, prepareBody *rpc.PrepareBody) (*rpc.PromiseBody, error) {
	defer updateBallotNumber()
	log.Printf("Received prepare message: %s", prepareBody.String())
	lastVotes := make(map[uint64]string)
	return &rpc.PromiseBody{Decrees: lastVotes}, nil
}

func (s *PaxosServerImpl) Learn(ctx context.Context, learnBody *rpc.LearnBody) (*rpc.LearnAck, error) {
	db.State.InsertAcceptedLog(learnBody.LogIndex, learnBody.Decree)
	log.Printf("learning decree: %s in index: %d", learnBody.Decree, learnBody.LogIndex)
	return &rpc.LearnAck{Acked: true}, nil
}

func (s *PaxosServerImpl) Accept(ctx context.Context, acceptBody *rpc.AcceptBody) (*rpc.AcceptedBody, error) {
	log.Printf("Received accept message: %s", acceptBody.String())
	paxosInstance, err := db.State.GetRowByLogIndex(acceptBody.LogIndex)
	if err != nil {
		s := fmt.Sprintf("error getting paxos instance information: %s", err.Error())
		log.Print(s)
		return &rpc.AcceptedBody{Accepted: false, Message: s}, nil
	}

	logIndex, decree, ballotNum := acceptBody.LogIndex, acceptBody.Decree, acceptBody.BallotNumber
	if ballotNum < paxosInstance.BallotNumOfHighestVotedForDecree {
		s := fmt.Sprintf("promised not to vote for any ballot number less than %d",
			paxosInstance.BallotNumOfHighestVotedForDecree)
		log.Print(s)
		return &rpc.AcceptedBody{Accepted: false, Message: s}, nil
	}

	db.State.UpsertPaxosLogState(logIndex, decree, ballotNum)

	return &rpc.AcceptedBody{Accepted: true}, nil
}

func updateBallotNumber() uint64 {
	NextBallot += 10
	return NextBallot
}
