package raft

import (
	"6.5840/labgob"
	"bytes"
	"time"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		DPrintf("Fail to Encode raft's data")
		return
	}
	data := w.Bytes()
	rf.persister.Save(data, snapshot)
	DPrintf("Server %v persist data", rf.me)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("Fail to reading persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check that the index is valid
	if index <= rf.snapshottedIndex {
		return // Snapshot is already up-to-date
	}
	DPrintf("Service create snapshot at index %v", index)
	// Update the snapshot and log
	rf.log = rf.log[index-rf.snapshottedIndex:]
	rf.snapshottedIndex = index

	// Persist the state and snapshot
	rf.persist(snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// If the term is outdated, reject the snapshot
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// Update term and convert to follower if necessary
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ChangeState(Follower)
	}

	rf.lastHeartbeat = time.Now()

	// Check if the snapshot is for an index we have already applied
	if args.LastIncludedIndex <= rf.snapshottedIndex {
		reply.Success = false
		return
	}

	rf.log = rf.log[args.LastIncludedIndex-rf.snapshottedIndex:]
	rf.snapshottedIndex = args.LastIncludedIndex

	// Notify the service of the snapshot
	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		Command:      args.Data,
		CommandIndex: args.LastIncludedIndex,
	}

	// Persist the snapshot
	rf.persist(args.Data)

	reply.Success = true
}

func (rf *Raft) generateInstallSnapshotRequest() InstallSnapshotRequest {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	request := InstallSnapshotRequest{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshottedIndex,
		LastIncludedTerm:  rf.log[rf.snapshottedIndex].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return request
}

func (rf *Raft) handleInstallSnapshotResponse(peer int, request InstallSnapshotRequest, response *InstallSnapshotResponse) {
	if response.Term > rf.currentTerm {
		rf.currentTerm = response.Term
		rf.ChangeState(Follower)
		return
	}

	rf.nextIndex[peer] = request.LastIncludedIndex + 1
	rf.matchIndex[peer] = request.LastIncludedIndex
}

func (rf *Raft) sendInstallSnapshot(server int, request InstallSnapshotRequest, response *InstallSnapshotResponse) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", request, response)
	return ok
}
