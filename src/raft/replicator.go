package raft

// Polling to decide if current raft server needs to start an election
// or needs to send a heartbeat when the leader is timeout
// It will poll by a polling interval until the server is killed
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.startElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadcastHeartBeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) broadcastHeartBeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// Need to send immediately to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// Signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.snapshottedIndex {
		rf.mu.RUnlock()
		//Catch up by snapshot
		request := rf.generateInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else {
		// Catch up by log appending
		request := rf.generateAppendRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, &request, response) {
			rf.mu.RLock()
			rf.handleAppendResponse(peer, &request, response)
			rf.mu.RUnlock()
		}
	}
	rf.updateCommitIndex()
	rf.applyLogs()
}

func (rf *Raft) needReplicating(peer int) bool {
	// Lock is already held when this is called
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLogIndex()
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// Wait for a signal if there is no need to replicate entries for this peer
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// Replicate entries until this peer catches up, then wait again
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) startElection() {

	DPrintf("RAFT SERVER %v starting election at TERM %v", rf.me, rf.currentTerm)
	rf.upgradeCurrentTerm(rf.currentTerm + 1)
	rf.ChangeState(Candidate)

	grantedVotes := 1
	// rf.persist()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			request := rf.generateVoteRequest()
			response := &RequestVoteResponse{}
			if rf.sendVoteRequest(peer, &request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == request.Term && rf.state == Candidate {
					if response.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("SERVER %v receives majority votes in TERM %v", rf.me, rf.currentTerm)
							rf.ChangeState(Leader)
						}
					} else if response.Term > rf.currentTerm {
						DPrintf("SERVER %vfinds a new leader SERVER %v with term %v and steps down in TERM %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(Follower)
						rf.upgradeCurrentTerm(response.Term)
						// rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	response.Term = rf.currentTerm
	//response.VoterId = rf.me

	// If the candidate's term is smaller than the current raft server's term
	// reject the vote request
	if request.Term < rf.currentTerm {
		DPrintf("RAFT SERVER %v refuse to vote for %v because it has higer term", rf.me, request.CandidateId)
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.upgradeCurrentTerm(request.Term)
		rf.ChangeState(Follower)
		DPrintf("{Vote} %v voted for %v in TERM %v", rf.me, request.CandidateId, request.Term)
		response.Term, response.VoteGranted = rf.currentTerm, true
		return
	}

	// Read the last log index and term of current raft
	// (5.4.1) The candidate can receive a vote from a raft only
	// if the candidate is up-to-date to the voter
	// Up-to-date: Log with later term or same term but longer is more up-to-date,
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.log[lastLogIndex].Term

	if rf.votedFor != -1 || rf.votedFor == request.CandidateId {
		DPrintf("RAFT SERVER %v refuse to vote for %v because it has voted", rf.me, request.CandidateId)
		response.VoteGranted = false
		return
	}
	// If the current raft server has not voted for other candidates, and the
	if request.LastLogTerm > lastLogTerm ||
		(request.LastLogTerm == lastLogTerm && request.LastLogIndex >= lastLogIndex) {
		DPrintf("{Vote} %v voted for %v in TERM %v", rf.me, request.CandidateId, request.Term)
		response.VoteGranted = true
		rf.setVoteFor(request.CandidateId)
	} else {
		DPrintf("RAFT SERVER %v refuse to vote for %v because it is more up-to-date", rf.me, request.CandidateId)
		response.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, reply *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	DPrintf("{Heartbeat}Leader %v to Server %v in TERM %v", request.LeaderId, rf.me, request.Term)
	// Refuse append entries from a smaller term
	if request.Term < rf.currentTerm {
		DPrintf("SERVER %v refuse to add the log in TERM %v because the request is stale", rf.me, rf.currentTerm)
		reply.Success = false
		return
	}
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	rf.upgradeCurrentTerm(request.Term)
	rf.ChangeState(Follower)

	// Check if log contains an entry at PrevLogIndex with term matching PrevLogTerm
	if request.PrevLogIndex > 0 && (rf.getLastLogIndex() < request.PrevLogIndex || rf.log[request.PrevLogIndex-rf.snapshottedIndex].Term != request.PrevLogTerm) {
		reply.Success = false
		DPrintf("SERVER %v refuse to add the log in TERM %v because previous logs not match", rf.me, rf.currentTerm)
		return
	}

	// Discard the logs after prevlogindex and accept leader's log
	if request.PrevLogIndex >= 0 {
		rf.appendEntriesToLog(request.PrevLogIndex+1, request.Entries)
	}

	if request.LeaderCommit > rf.commitIndex {
		lastIndex := rf.getLastLogIndex()
		rf.commitIndex = min(request.LeaderCommit, lastIndex)
		DPrintf("COMMIT INDEX UPDATE: Follower %v update commit to %v, lastindex = %v, leader commit = %v", rf.me, rf.commitIndex, lastIndex, request.LeaderCommit)
		rf.applyLogs()
	}

	reply.Success = true
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
			rf.applyLogs()
			DPrintf("COMMIT INDEX UPDATE: The leader %v has been updated the commit index to %v", rf.me, rf.commitIndex)
			break
		}
	}

}

func (rf *Raft) applyLogs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-rf.snapshottedIndex].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
	DPrintf("{APPLY LOGS}: SERVER: %v, TERM %v, commit index: %v, lastapplied: %v, log: %v", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log)
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", request, response)
	return ok
}

func (rf *Raft) sendVoteRequest(server int, request *RequestVoteRequest, response *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", request, response)
	return ok
}

func (rf *Raft) generateAppendRequest(preLogIndex int) AppendEntriesRequest {

	preLogTerm := rf.log[preLogIndex-rf.snapshottedIndex].Term
	entries := rf.log[preLogIndex-rf.snapshottedIndex+1:]

	// Create the AppendEntriesArgs
	args := AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) generateVoteRequest() RequestVoteRequest {
	request := RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	return request
}

func (rf *Raft) handleAppendResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
	// If encounter a higher term follower, turn to follower
	if response.Term > rf.currentTerm {
		rf.currentTerm = response.Term
		rf.ChangeState(Follower)
		return
	}
	if response.Success {
		// Append success, update nextIndex and matchIndex
		rf.nextIndex[peer] = request.PrevLogIndex + len(request.Entries) + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
	} else {
		// Decrement nextIndex and retry
		rf.nextIndex[peer] = max(1, rf.nextIndex[peer]-1)
	}
}
