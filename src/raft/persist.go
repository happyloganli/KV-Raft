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
	if index <= rf.snapShotIndex {
		return // Snapshot is already up-to-date
	}
	DPrintf("Service create snapshot at index %v", index)
	// Update the snapshot and log
	rf.log = rf.log[index-rf.snapShotIndex:]
	rf.snapShotIndex = index

	// Persist the state and snapshot
	rf.persist(snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
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
		rf.convertToFollower()
	}

	rf.lastHeartbeat = time.Now()

	// Check if the snapshot is for an index we have already applied
	if args.LastIncludedIndex <= rf.snapShotIndex {
		reply.Success = false
		return
	}

	rf.log = rf.log[args.LastIncludedIndex-rf.snapShotIndex:]
	rf.snapShotIndex = args.LastIncludedIndex

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
