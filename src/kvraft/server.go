package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type  string // "Put", "Append", or "Get"
	Key   string
	Value string
	Id    int64 // Client ID
	Seq   int   // Request sequence number
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate       int // snapshot if log grows this big
	data               map[string]string
	lastAppliedRequest map[int64]int
	waitChans          map[int]chan ApplyResult

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	index, _, isLeader := kv.rf.Start(Op{
		Type: "Get",
		Key:  args.Key,
		Id:   args.ClientId,
		Seq:  args.RequestId,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
	}

	ch := kv.getWaitChan(index)
	result := <-ch

	if result.Err == OK {
		reply.Err = OK
		reply.Value = result.Value
	} else {
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:  "Put",
		Key:   args.Key,
		Value: args.Value,
		Id:    args.ClientId,
		Seq:   args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitChan(index)
	result := <-ch

	reply.Err = result.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:  "Append",
		Key:   args.Key,
		Value: args.Value,
		Id:    args.ClientId,
		Seq:   args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitChan(index)
	result := <-ch

	reply.Err = result.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.lastAppliedRequest = make(map[int64]int)
	kv.waitChans = make(map[int]chan ApplyResult)

	go kv.applyOperations()

	return kv
}

func (kv *KVServer) applyOperations() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			var result ApplyResult

			kv.mu.Lock()
			// Check if this operation has already been applied
			if seq, ok := kv.lastAppliedRequest[op.Id]; !ok || op.Seq > seq {
				switch op.Type {
				case "Put":
					kv.data[op.Key] = op.Value
					result.Err = OK
				case "Append":
					kv.data[op.Key] += op.Value
					result.Err = OK
				case "Get":
					result.Value = kv.data[op.Key]
					result.Err = OK
				}

				kv.lastAppliedRequest[op.Id] = op.Seq
			} else {
				// Operation has already been applied
				result.Err = OK
				if op.Type == "Get" {
					result.Value = kv.data[op.Key]
				}
			}

			// Signal the result back to the waiting goroutine
			if ch, ok := kv.waitChans[applyMsg.CommandIndex]; ok {
				ch <- result
				close(ch)
				delete(kv.waitChans, applyMsg.CommandIndex)
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) getWaitChan(index int) chan ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.waitChans[index]; !ok {
		kv.waitChans[index] = make(chan ApplyResult, 1)
	}
	return kv.waitChans[index]
}

func (kv *KVServer) notifyWaitChan(index int, result ApplyResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, ok := kv.waitChans[index]; ok {
		ch <- result
		close(ch)
		delete(kv.waitChans, index)
	}
}

func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int) bool {
	lastSeq, exists := kv.lastAppliedRequest[clientId]
	return exists && requestId <= lastSeq
}
