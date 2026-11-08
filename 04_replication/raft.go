package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

// RaftState represents the state of a Raft node
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int
	Command string
	Key     string
	Value   string
}

// RaftNode represents a single node in the Raft cluster
type RaftNode struct {
	// Persistent state
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int
	state       RaftState

	// Volatile state on leaders
	nextIndex  map[string]int
	matchIndex map[string]int

	// Node information
	id     string
	peers  map[string]string // peer ID -> address
	server *Server           // Reference to our KV server

	// Channels for communication
	voteCh          chan bool
	appendEntriesCh chan bool
	grantVoteCh     chan bool

	// Configuration
	electionTimeout time.Duration
	mu              sync.RWMutex
	stopCh          chan struct{}
}

// NewRaftNode creates a new Raft node
func NewRaftNode(id string, server *Server) *RaftNode {
	rn := &RaftNode{
		id:              id,
		server:          server,
		currentTerm:     0,
		votedFor:        "",
		log:             []LogEntry{},
		state:           Follower,
		peers:           make(map[string]string),
		nextIndex:       make(map[string]int),
		matchIndex:      make(map[string]int),
		voteCh:          make(chan bool, 1),
		appendEntriesCh: make(chan bool, 1),
		grantVoteCh:     make(chan bool, 1),
		electionTimeout: time.Duration(150+rand.Intn(150)) * time.Millisecond,
		stopCh:          make(chan struct{}),
	}

	// Start with an empty log entry at index 0
	rn.log = append(rn.log, LogEntry{Term: 0, Command: "NOOP"})

	return rn
}

// AddPeer adds a peer to the Raft cluster
func (rn *RaftNode) AddPeer(peerID, address string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.peers[peerID] = address
	rn.nextIndex[peerID] = len(rn.log)
	rn.matchIndex[peerID] = 0
}

// Start begins the Raft node's main loop
func (rn *RaftNode) Start() {
	fmt.Printf("Raft node %s starting as FOLLOWER in term %d\n", rn.id, rn.currentTerm)
	go rn.runElectionTimer()
	go rn.processCommittedEntries()
}

// Stop halts the Raft node
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
}

// runElectionTimer runs the election timer for followers/candidates
func (rn *RaftNode) runElectionTimer() {
	for {
		select {
		case <-rn.stopCh:
			return
		default:
			rn.mu.Lock()
			state := rn.state
			rn.mu.Unlock()

			if state != Leader {
				timeout := rn.resetElectionTimeout()

				select {
				case <-time.After(timeout):
					// Election timeout - start election
					rn.startElection()
				case <-rn.appendEntriesCh:
					// Got heartbeat, reset timer
				case <-rn.grantVoteCh:
					// Granted vote to another candidate, reset timer
				case <-rn.stopCh:
					return
				}
			} else {
				// If we're leader, we don't need election timer
				time.Sleep(rn.electionTimeout)
			}
		}
	}
}

// resetElectionTimeout resets the election timeout with some randomness
func (rn *RaftNode) resetElectionTimeout() time.Duration {
	rn.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	return rn.electionTimeout
}

// startElection begins a new election
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	votesReceived := 1 // Vote for ourselves

	fmt.Printf("Node %s starting election for term %d\n", rn.id, rn.currentTerm)

	// Create RequestVote RPC arguments
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}

	rn.mu.Unlock()

	// Send RequestVote RPC to all peers
	var wg sync.WaitGroup
	var mu sync.Mutex

	for peerID := range rn.peers {
		wg.Add(1)
		go func(pid string) {
			defer wg.Done()

			args := RequestVoteArgs{
				Term:         rn.currentTerm,
				CandidateID:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			var reply RequestVoteReply

			if ok := rn.sendRequestVote(pid, &args, &reply); ok {
				mu.Lock()
				defer mu.Unlock()

				if reply.VoteGranted {
					votesReceived++
					fmt.Printf("Node %s received vote from %s\n", rn.id, pid)
				} else if reply.Term > rn.currentTerm {
					rn.mu.Lock()
					if reply.Term > rn.currentTerm {
						rn.currentTerm = reply.Term
						rn.state = Follower
						rn.votedFor = ""
						fmt.Printf("Node %s stepped down to FOLLOWER due to higher term from %s\n", rn.id, pid)
					}
					rn.mu.Unlock()
				}
			}
		}(peerID)
	}

	wg.Wait()

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Check if we won the election
	if rn.state == Candidate {
		majority := (len(rn.peers) + 1) / 2
		if votesReceived > majority {
			rn.state = Leader
			fmt.Printf("Node %s became LEADER for term %d with %d votes\n", rn.id, rn.currentTerm, votesReceived)

			// Initialize leader state
			for peerID := range rn.peers {
				rn.nextIndex[peerID] = len(rn.log)
				rn.matchIndex[peerID] = 0
			}

			// Start sending heartbeats
			go rn.sendHeartbeats()
		} else {
			// Lost election, back to follower
			rn.state = Follower
			rn.votedFor = ""
			fmt.Printf("Node %s lost election for term %d\n", rn.id, rn.currentTerm)
		}
	}
}

// sendHeartbeats sends periodic heartbeats to all followers
func (rn *RaftNode) sendHeartbeats() {
	for {
		rn.mu.RLock()
		if rn.state != Leader {
			rn.mu.RUnlock()
			return
		}
		rn.mu.RUnlock()

		rn.sendAppendEntriesToAll()
		time.Sleep(50 * time.Millisecond) // Heartbeat interval
	}
}

// sendAppendEntriesToAll sends AppendEntries RPC to all followers
func (rn *RaftNode) sendAppendEntriesToAll() {
	for peerID := range rn.peers {
		go rn.sendAppendEntries(peerID)
	}
}

// RequestVoteArgs represents the arguments for RequestVote RPC
type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply represents the reply for RequestVote RPC
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs represents the arguments for AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply represents the reply for AppendEntries RPC
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RPC handler
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	if args.Term < rn.currentTerm {
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = ""
	}

	// Check if we can grant vote
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}

	logOk := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rn.votedFor == "" || rn.votedFor == args.CandidateID) && logOk {
		rn.votedFor = args.CandidateID
		reply.VoteGranted = true
		select {
		case rn.grantVoteCh <- true:
		default:
		}
		fmt.Printf("Node %s granted vote to %s for term %d\n", rn.id, args.CandidateID, args.Term)
	}

	return nil
}

// --- PARTE MANCANTE NEGLI SCREENSHOT ---

// AppendEntries RPC handler
func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.Success = false

	if args.Term < rn.currentTerm {
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.votedFor = ""
	}

	rn.state = Follower

	// Notify election timer that we heard from the leader
	select {
	case rn.appendEntriesCh <- true:
	default:
	}

	// Check log consistency at PrevLogIndex
	if args.PrevLogIndex >= len(rn.log) || rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	// Append any new entries not already in the log
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		idx := insertIndex + i
		if idx < len(rn.log) {
			if rn.log[idx].Term != entry.Term {
				rn.log = rn.log[:idx]
				rn.log = append(rn.log, args.Entries[i:]...)
				break
			}
		} else {
			rn.log = append(rn.log, args.Entries[i:]...)
			break
		}
	}

	// Update commitIndex
	if args.LeaderCommit > rn.commitIndex {
		lastNewIndex := len(rn.log) - 1
		if args.LeaderCommit < lastNewIndex {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
	return nil
}

// sendRequestVote sends a RequestVote RPC to a peer
func (rn *RaftNode) sendRequestVote(peerID string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rn.mu.RLock()
	address, ok := rn.peers[peerID]
	rn.mu.RUnlock()
	if !ok {
		return false
	}

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return false
	}
	defer client.Close()

	err = client.Call("RaftNode.RequestVote", args, reply)
	return err == nil
}

// sendAppendEntries sends an AppendEntries RPC to a single peer
func (rn *RaftNode) sendAppendEntries(peerID string) {
	rn.mu.RLock()
	if rn.state != Leader {
		rn.mu.RUnlock()
		return
	}

	address, ok := rn.peers[peerID]
	if !ok {
		rn.mu.RUnlock()
		return
	}

	nextIdx := rn.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
		prevLogTerm = rn.log[prevLogIndex].Term
	}

	var entries []LogEntry
	if nextIdx < len(rn.log) {
		entries = append(entries, rn.log[nextIdx:]...)
	}

	args := AppendEntriesArgs{
		Term:         rn.currentTerm,
		LeaderID:     rn.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rn.commitIndex,
	}
	rn.mu.RUnlock()

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return
	}
	defer client.Close()

	var reply AppendEntriesReply
	if err := client.Call("RaftNode.AppendEntries", &args, &reply); err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if reply.Term > rn.currentTerm {
		rn.currentTerm = reply.Term
		rn.state = Follower
		rn.votedFor = ""
		return
	}

	if rn.state == Leader && args.Term == rn.currentTerm {
		if reply.Success {
			rn.matchIndex[peerID] = args.PrevLogIndex + len(args.Entries)
			rn.nextIndex[peerID] = rn.matchIndex[peerID] + 1
			rn.updateCommitIndex()
		} else {
			if rn.nextIndex[peerID] > 1 {
				rn.nextIndex[peerID]--
			}
		}
	}
}

// updateCommitIndex advances commitIndex if a majority of nodes have replicated an entry
func (rn *RaftNode) updateCommitIndex() {
	for n := len(rn.log) - 1; n > rn.commitIndex; n-- {
		if rn.log[n].Term != rn.currentTerm {
			continue
		}
		count := 1 // Leader itself
		for peerID := range rn.peers {
			if rn.matchIndex[peerID] >= n {
				count++
			}
		}
		majority := (len(rn.peers) + 1) / 2
		if count > majority {
			rn.commitIndex = n
			break
		}
	}
}

// processCommittedEntries applies committed log entries to the KV server state machine
func (rn *RaftNode) processCommittedEntries() {
	for {
		select {
		case <-rn.stopCh:
			return
		case <-time.After(10 * time.Millisecond):
			rn.mu.Lock()
			for rn.lastApplied < rn.commitIndex {
				rn.lastApplied++
				entry := rn.log[rn.lastApplied]
				if rn.server != nil && entry.Command != "NOOP" {
					rn.server.Apply(entry)
				}
				fmt.Printf("[Node %s] Committed & Applied index %d: %s %s=%s\n",
					rn.id, rn.lastApplied, entry.Command, entry.Key, entry.Value)
			}
			rn.mu.Unlock()
		}
	}
}

// Submit appends a new command to the leader's log
func (rn *RaftNode) Submit(command, key, value string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return false
	}

	entry := LogEntry{
		Term:    rn.currentTerm,
		Command: command,
		Key:     key,
		Value:   value,
	}
	rn.log = append(rn.log, entry)
	fmt.Printf("[Leader %s] Appended log entry at index %d: %s %s=%s\n",
		rn.id, len(rn.log)-1, command, key, value)
	return true
}
