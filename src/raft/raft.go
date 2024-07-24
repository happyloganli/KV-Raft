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

	entries := []LogEntry{
		{
			Term:    rf.currentTerm,
			Command: command,
		},
	}
	rf.appendEntriesToLog(len(rf.log), entries)
	DPrintf("Current log of Server %v: %v", rf.me, rf.log)

	index := len(rf.log) - 1
	term := rf.currentTerm

	// Persist the state
	// rf.persist()
	DPrintf("ACCEPT COMMAND: Leader %v, term: %v, index: %v", rf.me, term, index)
	// Send the new entry to all followers
	rf.broadcastAppendEntries()
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

		commitIndex:   0,
		lastApplied:   0,
		log:           make([]LogEntry, 1),
		snapShotIndex: 0,
	}

	// Initialize random election timeout for 5.4.1 Election Restriction
	rf.electionTimeout = time.Duration(rand.Intn(int(ElectionTimeoutMax-ElectionTimeoutMin))) + ElectionTimeoutMin
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

func (rf *Raft) setCurrentTerm(term int) {
	rf.currentTerm = term
	rf.persist(nil)
}

func (rf *Raft) getFirstIndex() int {
	return rf.snapShotIndex + 1
}

func (rf *Raft) getLastIndex(term int) int {
	return rf.snapShotIndex + len(rf.log)
}
