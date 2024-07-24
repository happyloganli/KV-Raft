package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) + 1
	}
	DPrintf("Server %v becomes leader at TERM %v", rf.me, rf.currentTerm)
	rf.broadcastHeartbeats()
}

func (rf *Raft) convertToFollower() {
	rf.state = Follower
	rf.setVoteFor(-1)
	rf.lastHeartbeat = time.Now()
	DPrintf("Server %v becomes follower at TERM %v", rf.me, rf.currentTerm)
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) broadcastHeartbeats() {
	DPrintf("SERVER %v broadcasting heartbeats... ", rf.me)
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: -2,
					PrevLogTerm:  -2,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					// Check if the reply contains a higher term
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.setCurrentTerm(reply.Term)
						// Step down to follower
						rf.convertToFollower()
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

// Polling to decide if current raft server needs to start an election
// or needs to send a heartbeat when the leader is timeout
// It will poll by a polling interval until the server is killed
func (rf *Raft) ticker() {

	rf.lastHeartbeat = time.Now()

	for rf.killed() == false {

		rf.mu.Lock()
		if rf.state == Leader {
			rf.broadcastHeartbeats()
		} else {
			if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
				rf.startElection()
			}
		}
		rf.mu.Unlock()

		pollingInterval := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(pollingInterval) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {

	DPrintf("RAFT SERVER %v starting election at TERM %v", rf.me, rf.currentTerm)
	rf.setCurrentTerm(rf.currentTerm + 1)
	rf.lastHeartbeat = time.Now()
	rf.state = Candidate

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	request := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.setVoteFor(rf.me)
	grantedVotes := 1
	// rf.persist()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", request, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == request.Term && rf.state == Candidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("SERVER %v receives majority votes in TERM %v", rf.me, rf.currentTerm)
							rf.convertToLeader()
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("SERVER %vfinds a new leader SERVER %v with term %v and steps down in TERM %v", rf.me, peer, reply.Term, rf.currentTerm)
						rf.convertToFollower()
						rf.setCurrentTerm(reply.Term)
						// rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Leader %v broadcasting to server %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoterId = rf.me

	// If the candidate's term is smaller than the current raft server's term
	// reject the vote request
	if request.Term < rf.currentTerm {
		DPrintf("RAFT SERVER %v refuse to vote for %v because it has higer term", rf.me, request.CandidateId)
		reply.VoteGranted = false
		return
	}

	// If current server discovered a higher term, it turns to a Follower
	// set its current term as the new term
	// since the term is stale, the vote is also stale, so set the vote to -1
	if request.Term > rf.currentTerm {
		rf.convertToFollower()
		rf.setCurrentTerm(request.Term)
	}
	// Read the last log index and term of current raft
	// (5.4.1) The candidate can receive a vote from a raft only
	// if the candidate is up-to-date to the voter
	// Up-to-date: Log with later term or same term but longer is more up-to-date,
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	if rf.votedFor != -1 || rf.votedFor == request.CandidateId {
		DPrintf("RAFT SERVER %v refuse to vote for %v because it has voted", rf.me, request.CandidateId)
		reply.VoteGranted = false
		return
	}
	// If the current raft server has not voted for other candidates, and the
	if request.LastLogTerm > lastLogTerm ||
		(request.LastLogTerm == lastLogTerm && request.LastLogIndex >= lastLogIndex) {
		DPrintf("RAFT SERVER %v voted for SERVER %v in TERM %v", rf.me, request.CandidateId, request.Term)
		reply.VoteGranted = true
		rf.setVoteFor(request.CandidateId)

		rf.lastHeartbeat = time.Now()
	} else {
		DPrintf("RAFT SERVER %v refuse to vote for %v because it is more up-to-date", rf.me, request.CandidateId)
		reply.VoteGranted = false
	}
}
