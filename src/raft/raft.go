package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm

	entries := []LogEntry{
		{
			Term:    term,
			Command: command,
			Index:   index,
		},
	}
	rf.appendEntriesToLog(len(rf.log), entries)
	// Persist the state
	rf.persist(nil)
	DPrintf("ACCEPT COMMAND: Leader %v, term: %v, index: %v", rf.me, term, index)
	// Send the new entry to all followers
	return index, term, true
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		state:     Follower,
		applyCh:   applyCh,

		commitIndex:      0,
		lastApplied:      0,
		log:              make([]LogEntry, 1),
		snapshottedIndex: 0,
		votedFor:         -1,
		electionTimer:    time.NewTimer(RandomizedElectionTimeout()),
		heartbeatTimer:   time.NewTimer(StableHeartbeatTimeout()),

		replicatorCond: make([]*sync.Cond, len(peers)),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)

	for i := range rf.peers {

		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
		if i == rf.me {
			continue
		}
		rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
		// start replicator goroutine to replicate entries in batch
		go rf.replicator(i)
	}

	// Initialize random election timeout for 5.4.1 Election Restriction
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) appendEntriesToLog(index int, entries []LogEntry) {
	rf.log = rf.log[:index]
	rf.log = append(rf.log, entries...)
	rf.persist(nil)
	DPrintf("APPEND LOGS: Server %v logs: %v, index: %v", rf.me, entries, index)
	DPrintf("Current log of Server %v: %v", rf.me, rf.log)
}

func (rf *Raft) setVoteFor(candidateId int) {
	rf.votedFor = candidateId
	rf.persist(nil)
}

func (rf *Raft) upgradeCurrentTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist(nil)
}

func (rf *Raft) getFirstLogIndex() int {
	if len(rf.log) == 0 {
		return 0 // Return a default value if log is empty
	}
	return rf.log[0].Index
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return 0 // Return a default value if log is empty
	}
	return rf.log[len(rf.log)-1].Index
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(350+rand.Intn(150)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return 150 * time.Millisecond
}

func (rf *Raft) ChangeState(newState State) {
	rf.state = newState

	switch newState {
	case Follower:
		rf.heartbeatTimer.Stop()                            // Inactivate the heartbeat timer
		rf.electionTimer.Reset(RandomizedElectionTimeout()) // Activate/reset the election timer
		DPrintf("Server %v becomes follower at TERM %v", rf.me, rf.currentTerm)
	case Candidate:
		rf.heartbeatTimer.Stop()
		rf.setVoteFor(rf.me)                                // Inactivate the heartbeat timer
		rf.electionTimer.Reset(RandomizedElectionTimeout()) // Activate/reset the election timer
	case Leader:
		rf.electionTimer.Stop()                           // Inactivate the election timer
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout()) // Activate/reset the heartbeat timer
		DPrintf("Server %v becomes leader at TERM %v", rf.me, rf.currentTerm)
		rf.broadcastHeartBeat(true)
	}
}

func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
