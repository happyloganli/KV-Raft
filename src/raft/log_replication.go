package raft

import "time"

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
		rf.appendEntriesToLog(request.PrevLogIndex+1, request.Entries)
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
				for rf.killed() == false && rf.state == Leader {
					DPrintf("Leader %v is broadcasting a new Entry at TERM %v to Follower %v", rf.me, rf.currentTerm, peer)
					rf.mu.Lock()
					entries := []LogEntry{}
					if rf.nextIndex[peer] < len(rf.log) {
						entries = rf.log[rf.nextIndex[peer]:]
					}
					request := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[peer] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					var reply AppendEntriesReply
					if rf.sendAppendEntries(peer, &request, &reply) {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.setCurrentTerm(reply.Term)
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
							break
						} else if rf.nextIndex[peer] > 1 {
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
