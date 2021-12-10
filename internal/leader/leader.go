package leader

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type leader struct {
	state  bool
	mu     sync.RWMutex
	recvHB chan uint32
}

var (
	leadershipState leader = leader{state: false, recvHB: make(chan uint32)}
	randGen                = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func AmILeader() bool {
	leadershipState.mu.RLock()
	defer leadershipState.mu.RUnlock()
	return leadershipState.state
}

func BecomeFollower() {
	leadershipState.mu.Lock()
	if leadershipState.state {
		log.Print("Becoming a FOLLOWER")
	}
	leadershipState.state = false
	leadershipState.mu.Unlock()
}

func BecomeLeader() {
	log.Print("Becoming LEADER")
	leadershipState.mu.Lock()
	leadershipState.state = true
	leadershipState.mu.Unlock()
}

func SignalHeartbeatRecv(rn uint32) {
	leadershipState.recvHB <- rn
}

func GetHeartbeatChan() chan uint32 {
	return leadershipState.recvHB
}

func GetNextTimeout() time.Duration {
	return time.Duration(randGen.Intn(1000) + 1000)
}
