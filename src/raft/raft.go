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

	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.votedFor = -1
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
						// Step down to follower
						rf.currentTerm = reply.Term
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
	rf.currentTerm++
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

	rf.votedFor = rf.me
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
						rf.currentTerm = reply.Term
						// rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
		rf.currentTerm = request.Term
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
		rf.votedFor = request.CandidateId
		rf.lastHeartbeat = time.Now()
	} else {
		DPrintf("RAFT SERVER %v refuse to vote for %v because it is more up-to-date", rf.me, request.CandidateId)
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	DPrintf("Server %v receive a heartbeat from Leader %v in TERM %v", rf.me, request.LeaderId, request.Term)
	// Refuse append entries from a smaller term
	if request.Term < rf.currentTerm {
		DPrintf("SERVER %v refuse to add the log in TERM %v because the request is stale", rf.me, rf.currentTerm)
		reply.Success = false
		return
	}
	if rf.state == Candidate {
		rf.convertToFollower()
	}
	// Only accept heartbeat from a greater term leader
	rf.lastHeartbeat = time.Now()

	// Check if log contains an entry at PrevLogIndex with term matching PrevLogTerm
	if request.PrevLogIndex > 0 && (len(rf.log) <= request.PrevLogIndex || rf.log[request.PrevLogIndex].Term != request.PrevLogTerm) {
		reply.Success = false
		DPrintf("SERVER %v refuse to add the log in TERM %v because its log is too short", rf.me, rf.currentTerm)
		return
	}

	// Discard the logs after prevlogindex and accept leader's log
	if request.PrevLogIndex >= 0 {
		rf.log = rf.log[:request.PrevLogIndex+1]
		rf.log = append(rf.log, request.Entries...)
		DPrintf("APPEND LOGS: Server %v logs: %v, index: %v", rf.me, request.Entries, request.PrevLogIndex+1)
		DPrintf("Current log of Server %v: %v", rf.me, rf.log)
	}

	if request.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(request.LeaderCommit, len(rf.log)-1)
		DPrintf("COMMIT INDEX UPDATE: Follower %v update commit to %v, log len = %v, leader commit = %v", rf.me, rf.commitIndex, len(rf.log), request.LeaderCommit)
		rf.applyLogs()
	}

	reply.Success = true
}

func (rf *Raft) broadcastAppendEntries() {

	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				for {
					rf.mu.Lock()
					request := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[peer] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
						Entries:      rf.log[rf.nextIndex[peer]:],
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					var reply AppendEntriesReply
					DPrintf("Leader %v is broadcasting a new Entry at TERM %v to Follower %v", rf.me, rf.currentTerm, peer)
					if rf.sendAppendEntries(peer, &request, &reply) {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.convertToFollower()
							// rf.persist()
							DPrintf("Leader %v received a reply from server %v in TERM %v, and it steps down because a higher term follower", rf.me, peer, rf.currentTerm)
							rf.mu.Unlock()
							return
						}
						if reply.Success {
							rf.nextIndex[peer] = request.PrevLogIndex + len(request.Entries) + 1
							rf.matchIndex[peer] = rf.nextIndex[peer] - 1
							DPrintf("Leader %v received a reply from server %v in TERM %v, and it successfully added the log Index %v", rf.me, peer, rf.currentTerm, rf.matchIndex[peer])
							DPrintf("SERVER %v match Index: %v, next Index: %v", peer, rf.matchIndex[peer], rf.nextIndex[peer])
							rf.updateCommitIndex()
							rf.mu.Unlock()
							break
						} else {
							DPrintf("Leader %v received a reply from server %v in TERM %v, and it decreased the index by 1", rf.me, peer, rf.currentTerm)
							rf.nextIndex[peer]--
						}
						rf.mu.Unlock()
					}
				}
			}(peer)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// Find the highest log index N such that a majority of matchIndex[i] >= N and log[N].term == currentTerm
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		count := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			DPrintf("COMMIT INDEX UPDATE: The leader %v has been updated the commit index to %v", rf.me, rf.commitIndex)
			rf.applyLogs()
			break
		}
	}
}

func (rf *Raft) applyLogs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
	DPrintf("{APPLY LOGS}: SERVER: %v, TERM %v, commit index: %v, lastapplied: %v, log: %v", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
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

		commitIndex: 0,
		lastApplied: 0,
		log:         make([]LogEntry, 1),
	}

	// Initialize random election timeout for 5.4.1 Election Restriction
	rf.electionTimeout = time.Duration(rand.Intn(int(ElectionTimeoutMax-ElectionTimeoutMin))) + ElectionTimeoutMin
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
