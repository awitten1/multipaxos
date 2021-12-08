package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awitten1/multipaxos/internal/db"
	"github.com/awitten1/multipaxos/internal/leader"
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
			newCtx, _ := context.WithTimeout(ctx, time.Second)

			acceptedBody, err := c.Accept(newCtx, &rpc.AcceptBody{BallotNumber: db.DB.GetBallotNumber(), Decree: decree, LogIndex: logIndex})
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

func broadcastLearns(ctx context.Context, decree string, logIndex uint64) {
	for _, client := range Clients {
		go func(c rpc.PaxosClient) {
			newCtx, _ := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := c.Learn(newCtx, &rpc.LearnBody{LogIndex: logIndex, Decree: decree})
			if err != nil {
				log.Printf("error sending Learn message: %s", err.Error())
			}
		}(client)
	}
}

func broadcastPrepares(ctx context.Context) (map[uint64]string, error) {
	nextBallot, err := db.DB.GetAndUpdateProposalNumber()
	if err != nil {
		log.Printf("Could not get and update proposal number: %s", err.Error())
		return nil, err
	}
	nextBallot = nextBallot*10 + uint64(Replica) // Must be unique to this replica
	log.Print("broadcasting prepare messages")
	firstGap, err := db.DB.CreatePrepareRequest()
	if err != nil {
		log.Printf("Error building prepare message")
		return nil, err
	}
	var acks uint32 = 1
	var wg sync.WaitGroup
	promiseMsgChan := make(chan *rpc.PromiseBody)
	finalCmdsChan := make(chan map[uint64]string)
	newCtx, cancel := context.WithCancel(ctx)
	go func() {
		buildAllowedCommands(newCtx, promiseMsgChan, finalCmdsChan)
	}()

	for _, client := range Clients {
		c := client
		go func() {
			defer wg.Done()
			newCtx, _ := context.WithTimeout(ctx, time.Second)

			promiseBody, err := c.Prepare(newCtx, &rpc.PrepareBody{SmallestUndecidedIndex: firstGap, ProposalNumber: nextBallot})
			if err != nil {
				log.Printf("Error sending prepare: %s", err.Error())
				return
			}
			if promiseBody == nil {
				return
			}
			if promiseBody.AckedAsLeader {
				atomic.AddUint32(&acks, 1)
			}
			promiseMsgChan <- promiseBody
		}()
	}
	wg.Done()
	localMap, err := db.DB.CreateCurrentReplicaPromiseMaps(firstGap)
	if err != nil {
		log.Printf("Could not build local map of previous paxos instance info: %s", err.Error())
	} else {
		promiseMsgChan <- localMap
	}
	cancel()
	if acks < N/2+1 {
		return nil, fmt.Errorf("received %d acks in response to prepare messages", acks)
	}
	return <-finalCmdsChan, nil
}

func buildAllowedCommands(ctx context.Context, promiseBodies chan *rpc.PromiseBody, outputChannel chan map[uint64]string) {
	ret := make(map[uint64]string) // The commands we are allowed to propose
	correspondingProposals := make(map[uint64]uint64)
	for {
		select {
		case promise := <-promiseBodies:
			for idx, ballotNumOfHighestVotedForDec := range promise.CorrespondingBallotNum {
				curr, ok := correspondingProposals[idx]
				if !ok && ballotNumOfHighestVotedForDec > curr {
					correspondingProposals[idx] = ballotNumOfHighestVotedForDec
					ret[idx] = promise.Decrees[idx]
				}
			}
		case <-ctx.Done():
			outputChannel <- ret
			return
		}
	}
}

func broadcastHeartbeat(ctx context.Context) {
	for _, client := range Clients {
		go func(c rpc.PaxosClient) {
			newCtx, _ := context.WithTimeout(ctx, 700*time.Millisecond)
			c.Heartbeat(newCtx, &rpc.HeartbeatBody{ReplicaNum: uint32(Replica)})
		}(client)
	}
}

func (s *PaxosServerImpl) GetLog(ctx context.Context, _ *rpc.GetLogBody) (*rpc.Log, error) {
	if ret, err := db.DB.GetLog(); err != nil {
		return nil, err
	} else {
		return &rpc.Log{Entries: ret}, nil
	}
}

func (s *PaxosServerImpl) ClientCommand(ctx context.Context, commandBody *rpc.CommandBody) (*rpc.CommandResponse, error) {
	log.Printf("Received new decree from client: %s", commandBody.Decree)
	if !leader.AmILeader() {
		return &rpc.CommandResponse{Committed: false, ErrorMessage: "cannot commit message, not the leader"}, nil
	}
	// Get next log index to use
	nextIndex := atomic.AddUint64(&NextLogIndex, 1)
	db.DB.UpsertPaxosLogState(nextIndex, commandBody.Decree, db.DB.GetBallotNumber())
	acks, err := broadcastAccepts(ctx, commandBody.Decree, nextIndex)
	if err != nil {
		return &rpc.CommandResponse{Committed: false, ErrorMessage: err.Error()}, err
	}
	log.Printf("Received %d positive acks to the accept command, needed %d to replicate on a majority of nodes", acks, N/2+1)
	if acks < N/2+1 {
		log.Printf("Returning failure to client")
		return &rpc.CommandResponse{Committed: false, ErrorMessage: "Failed to replicate message: could not receive enough acks"}, nil
	}
	go func() { db.DB.InsertAcceptedLog(nextIndex, commandBody.Decree) }()
	log.Print("Broadcasting learns to all nodes")
	broadcastLearns(ctx, commandBody.Decree, nextIndex)
	return &rpc.CommandResponse{Committed: true, ErrorMessage: ""}, nil
}

func (s *PaxosServerImpl) Prepare(ctx context.Context, prepareBody *rpc.PrepareBody) (*rpc.PromiseBody, error) {
	log.Printf("Received prepare message: %s", prepareBody.String())
	lastVotes := make(map[uint64]string)
	return &rpc.PromiseBody{Decrees: lastVotes}, nil
}

func (s *PaxosServerImpl) Learn(ctx context.Context, learnBody *rpc.LearnBody) (*rpc.LearnAck, error) {
	db.DB.InsertAcceptedLog(learnBody.LogIndex, learnBody.Decree)
	log.Printf("learning decree: %s in index: %d", learnBody.Decree, learnBody.LogIndex)
	return &rpc.LearnAck{Acked: true}, nil
}

func (s *PaxosServerImpl) Heartbeat(ctx context.Context, heartbeat *rpc.HeartbeatBody) (*rpc.HeartbeatAck, error) {
	leader.SignalHeartbeatRecv(heartbeat.ReplicaNum)
	return &rpc.HeartbeatAck{}, nil
}

func (s *PaxosServerImpl) Accept(ctx context.Context, acceptBody *rpc.AcceptBody) (*rpc.AcceptedBody, error) {
	log.Printf("Received accept message: %s", acceptBody.String())
	paxosInstance, err := db.DB.GetRowByLogIndex(acceptBody.LogIndex)
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

	db.DB.UpsertPaxosLogState(logIndex, decree, ballotNum)

	return &rpc.AcceptedBody{Accepted: true}, nil
}

func MonitorHeartbeats(ctx context.Context, replica uint32) {
	timer := time.NewTimer(leader.GetNextTimeout() * time.Millisecond)
	var cancel context.CancelFunc = func() {}
	for {
		select {
		case replicaNum := <-leader.GetHeartbeatChan():
			if replicaNum < replica {
				cancel()           // If we are sending heartbeats, stop doing that.
				cancel = func() {} // set cancel to do nothing (we aren't sending heartbeats)
				leader.BecomeFollower()
				timer.Reset(leader.GetNextTimeout() * time.Millisecond)
			}

		case <-timer.C:
			newCtx, c := context.WithCancel(ctx)
			cancel = c
			leader.BecomeLeader()
			go func() { SendHeartbeats(newCtx) }()
			//timer.Reset(leader.GetNextTimeout() * time.Millisecond)
		}
	}
}

func SendHeartbeats(ctx context.Context) {
	timer := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-timer.C:
			broadcastHeartbeat(ctx)
		case <-ctx.Done():
			log.Printf("Stop sending heartbeats because we saw a heartbeat from a lower replica")
			return
		}
	}
}
