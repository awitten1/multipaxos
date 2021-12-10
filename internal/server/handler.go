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
	"google.golang.org/grpc"
)

type PaxosServerImpl struct {
	rpc.UnimplementedPaxosServer
}

func IsConnectionReady(conn *grpc.ClientConn) bool {
	return conn.GetState().String() == "READY"
}

// Broadcast accept requests and count the responses
func broadcastAccepts(ctx context.Context, decree string, logIndex uint64) (uint32, error) {
	var acks uint32 = 1
	var wg sync.WaitGroup
	for _, conn := range Conns {
		if !IsConnectionReady(conn) {
			continue
		}
		wg.Add(1)
		go func(c rpc.PaxosClient) {
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
		}(rpc.NewPaxosClient(conn))
	}
	wg.Wait()
	return acks, nil
}

// Broadcast learn commands to inform peers of a new committed command
func broadcastLearns(ctx context.Context, decree string, logIndex uint64) {
	for _, conn := range Conns {
		if !IsConnectionReady(conn) {
			continue
		}
		go func(c rpc.PaxosClient) {
			newCtx, _ := context.WithTimeout(context.Background(), 1*time.Second)
			_, err := c.Learn(newCtx, &rpc.LearnBody{LogIndex: logIndex, Decree: decree})
			if err != nil {
				log.Printf("error sending Learn message: %s", err.Error())
			}
		}(rpc.NewPaxosClient(conn))
	}
}

// Broadcast prepare messages to all peers
func broadcastPrepares(ctx context.Context) (map[uint64]string, error) {
	nextBallot, err := db.DB.GetAndUpdateProposalNumber()
	if err != nil {
		log.Printf("Could not get and update proposal number: %s", err.Error())
		return nil, err
	}

	// Must be unique to this replica.  nextBallot = Replica mod N
	nextBallot = nextBallot*uint64(N) + uint64(Replica)
	log.Print("broadcasting prepare messages")

	// Compute first gap in accepted commands log
	firstGap, err := db.DB.ComputeFirstGapInLog()
	if err != nil {
		log.Printf("Error building prepare message")
		return nil, err
	}

	var acks uint32 = 1
	var resps uint32 = 1
	var wg sync.WaitGroup
	promiseMsgChan := make(chan *rpc.PromiseBody)
	finalCmdsChan := make(chan map[uint64]string)
	newCtx, cancel := context.WithCancel(ctx)

	// Start thread that aggregates all responses
	go func() {
		buildAllowedCommands(newCtx, promiseMsgChan, finalCmdsChan)
	}()

	// For each peer, send prepare command
	for _, conn := range Conns {
		if !IsConnectionReady(conn) {
			continue
		}
		cConn := conn
		wg.Add(1)
		go func(c rpc.PaxosClient) {
			defer wg.Done()
			newCtx, _ := context.WithTimeout(ctx, time.Second)

			promiseBody, err := c.Prepare(newCtx, &rpc.PrepareBody{SmallestUndecidedIndex: firstGap, ProposalNumber: nextBallot})
			if err != nil {
				log.Printf("Error sending prepare: %s", err.Error())
				return
			}
			if promiseBody != nil && promiseBody.AckedAsLeader {
				atomic.AddUint32(&acks, 1)
			}
			atomic.AddUint32(&resps, 1)
			log.Printf("Addr: %s, Received prepare decrees: %s", cConn.Target(), fmt.Sprint(promiseBody.Decrees))
			log.Printf("Addr: %s, Received prepare proposal numbers: %s", cConn.Target(), fmt.Sprint(promiseBody.BallotNums))
			// Drop response in channel
			promiseMsgChan <- promiseBody
		}(rpc.NewPaxosClient(conn))
	}

	// Wait for all threads to finish
	wg.Wait()

	// Do the same computations locally
	localMap, err := db.DB.CreatePrepareResp(&rpc.PrepareBody{SmallestUndecidedIndex: firstGap, ProposalNumber: nextBallot})

	if err != nil {
		log.Printf("Could not build local map of previous paxos instance info: %s", err.Error())
	} else {
		promiseMsgChan <- localMap
	}

	// Cancel aggregation thread
	cancel()

	// Not enough acks
	if acks < N/2+1 && resps >= N/2+1 {
		log.Printf("received %d acks in response to prepare messages", acks)
		// Didn't work, try again with higher proposal number
		return broadcastPrepares(ctx)
	}

	// Read answer from channel
	return <-finalCmdsChan, nil
}

// Thread that combines all promises
func buildAllowedCommands(ctx context.Context, promiseBodies chan *rpc.PromiseBody, outputChannel chan map[uint64]string) {
	ret := make(map[uint64]string) // The commands we are allowed to propose
	ballotNums := make(map[uint64]uint64)
	for {
		select {
		case promise := <-promiseBodies:
			for idx, ballotNum := range promise.BallotNums {
				curr, ok := ballotNums[idx]
				if !ok || ballotNum > curr {
					ballotNums[idx] = ballotNum
					ret[idx] = promise.Decrees[idx]
				}
			}
		case <-ctx.Done():
			outputChannel <- ret
			return
		}
	}
}

// broadcast heartbeats
func broadcastHeartbeat(ctx context.Context) {
	for _, conn := range Conns {
		if !IsConnectionReady(conn) {
			continue
		}
		go func(c rpc.PaxosClient) {
			newCtx, _ := context.WithTimeout(ctx, 600*time.Millisecond)
			c.Heartbeat(newCtx, &rpc.HeartbeatBody{ReplicaNum: uint32(Replica)})
		}(rpc.NewPaxosClient(conn))
	}
}

// Get log from database
func (s *PaxosServerImpl) GetLog(ctx context.Context, _ *rpc.GetLogBody) (*rpc.Log, error) {
	if ret, err := db.DB.GetLog(); err != nil {
		return nil, err
	} else {
		return &rpc.Log{Entries: ret}, nil
	}
}

// Respond to client command
func (s *PaxosServerImpl) ClientCommand(ctx context.Context, commandBody *rpc.CommandBody) (*rpc.CommandResponse, error) {
	log.Printf("Received new decree from client: %s", commandBody.Decree)
	if !leader.AmILeader() {
		return &rpc.CommandResponse{Committed: false}, fmt.Errorf("cannot commit message, not the leader")
	}
	var logIndex uint64
	var err error
	if commandBody.LogIndexToUse != 0 {
		// Only server sets this for "filling the gaps"
		logIndex = commandBody.LogIndexToUse
	} else {
		// Get next log index to use
		logIndex, err = db.DB.GetMaxAcceptedLogIndex()
		logIndex++
		if err != nil {
			return nil, err
		}
	}

	// Upsert this paxos log information
	db.DB.UpsertPaxosLogState(logIndex, commandBody.Decree, db.DB.GetBallotNumber())

	// Attempt to commit command
	acks, err := broadcastAccepts(ctx, commandBody.Decree, logIndex)
	if err != nil {
		return &rpc.CommandResponse{Committed: false}, err
	}
	log.Printf("Received %d positive acks to the accept command, needed %d to replicate on a majority of nodes", acks, N/2+1)
	if acks < N/2+1 {
		log.Printf("Returning failure to client")
		return &rpc.CommandResponse{Committed: false},
			fmt.Errorf("failed to replicate message: could not receive enough acks")
	}
	// asynchronously learn command locally
	go func() { db.DB.InsertAcceptedLog(&db.AcceptedLog{Decree: commandBody.Decree, Index: logIndex}) }()
	log.Print("Broadcasting learns to all nodes")
	// asynchronously learn all commands remotely
	broadcastLearns(ctx, commandBody.Decree, logIndex)
	return &rpc.CommandResponse{Committed: true}, nil
}

// Respond to this prepare
func (s *PaxosServerImpl) Prepare(ctx context.Context, prepareBody *rpc.PrepareBody) (*rpc.PromiseBody, error) {
	log.Printf("Received prepare message: %s", prepareBody.String())
	lastVotes, err := db.DB.CreatePrepareResp(prepareBody)
	if err != nil {
		return nil, err
	}
	return lastVotes, nil
}

// Learn a new committed command
func (s *PaxosServerImpl) Learn(ctx context.Context, learnBody *rpc.LearnBody) (*rpc.LearnAck, error) {
	// Asynchronously learn command
	go db.DB.InsertAcceptedLog(&db.AcceptedLog{Index: learnBody.LogIndex, Decree: learnBody.Decree})
	log.Printf("learning decree: %s in index: %d", learnBody.Decree, learnBody.LogIndex)
	return &rpc.LearnAck{Acked: true}, nil
}

// Acknowledge this heartbeat
func (s *PaxosServerImpl) Heartbeat(ctx context.Context, heartbeat *rpc.HeartbeatBody) (*rpc.HeartbeatAck, error) {
	leader.SignalHeartbeatRecv(heartbeat.ReplicaNum)
	return &rpc.HeartbeatAck{}, nil
}

// Respond to this request to accept this command
func (s *PaxosServerImpl) Accept(ctx context.Context, acceptBody *rpc.AcceptBody) (*rpc.AcceptedBody, error) {
	log.Printf("Received accept message: %s", acceptBody.String())

	// Decide if we should reject this accept request
	paxosInstance, err := db.DB.PaxosInstanceState(acceptBody.LogIndex)
	if err != nil {
		s := fmt.Sprintf("error getting paxos instance information: %s", err.Error())
		log.Print(s)
		return &rpc.AcceptedBody{Accepted: false}, fmt.Errorf(s)
	}

	logIndex, decree, ballotNum := acceptBody.LogIndex, acceptBody.Decree, acceptBody.BallotNumber

	// This node has promised that it will not vote for any lower command
	if ballotNum < paxosInstance.BallotNumOfHighestVotedForDecree {
		s := fmt.Sprintf("promised not to vote for any ballot number less than %d",
			paxosInstance.BallotNumOfHighestVotedForDecree)
		log.Print(s)
		return &rpc.AcceptedBody{Accepted: false}, fmt.Errorf(s)
	}

	// Before sending accept, document this request
	err = db.DB.UpsertPaxosLogState(logIndex, decree, ballotNum)
	if err != nil {
		log.Printf("Could not document this accept: %s", err.Error())
		return &rpc.AcceptedBody{Accepted: false}, err
	}

	return &rpc.AcceptedBody{Accepted: true}, nil
}

// Monitor all heartbeats recieved and decide whether or not to acknowledge it
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
			go SendHeartbeats(newCtx)
			toAccept, err := broadcastPrepares(ctx)
			if err != nil {
				log.Printf("Error broadcasting prepares: %s", err.Error())
			} else {
				sendAccepts(ctx, toAccept)
			}
		}
	}
}

func sendAccepts(ctx context.Context, toAccept map[uint64]string) error {
	log.Print("About to send accepts for log gap")
	var prev = -1
	for k, v := range toAccept {
		logIndex, decree := k, v
		entry, err := db.DB.GetAcceptedLog(logIndex)
		if entry != nil {
			continue
		}
		if err != nil {
			log.Print("Error querying accepted logs")
			continue
		}
		selfClient := rpc.NewPaxosClient(Self)
		go selfClient.ClientCommand(ctx, &rpc.CommandBody{Decree: decree, LogIndexToUse: logIndex})
		if prev != -1 {
			for i := prev; i < int(logIndex); i++ {
				entry, err = db.DB.GetAcceptedLog(logIndex)
				if err != nil {
					continue
				}
				var dec string = "NOOP"
				if entry != nil {
					dec = entry.Decree
				}
				go selfClient.ClientCommand(ctx, &rpc.CommandBody{Decree: dec, LogIndexToUse: uint64(i)})
			}
		}
		prev = int(logIndex)
	}
	return nil
}

// Periodically send heartbeats (at a higher frequency than the timeout period to receive a heartbeat)
func SendHeartbeats(ctx context.Context) {
	timer := time.NewTicker(200 * time.Millisecond)
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
